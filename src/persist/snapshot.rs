//! 快照：整库序列化到磁盘（原子替换）与加载校验（docs/design.md §3.1 v0.4）。
//!
//! 写入路径：`snapshot.tmp` → write_all → **fsync**（design.md D12：快照是持久化
//! 检查点，值得 fsync）→ 原子 rename 到 `snapshot.vrdb`——任何时刻磁盘上都存在
//! 一个完整可用的快照或旧快照。
//! 读取路径：magic / 版本 / **尾校验**（最后 4 字节 = 之前全部字节的 FNV-1a）/
//! 结构逐项校验，任何不符 → `Err(Corrupt)`（启动时拒绝服务，不静默丢数据）。

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use super::{
    corrupt, decode_value, encode_value, fnv1a, le_u32, put_str, put_u32, Cursor, PersistError,
};
use crate::storage::Value;

/// 快照 magic："VRDB"（Vredis DataBase）。
const MAGIC: u32 = 0x56_52_44_42;
/// 快照格式版本。
const VERSION: u32 = 1;

/// 把整库写入 `path`。调用方（Db::bgsave）持有引擎锁期间调用，保证一致性。
///
/// 条目按 key 排序写入：序列化确定性（与 encode_value 的向量排序一致，
/// 便于测试比对与人工检查）。
pub fn save(map: &HashMap<String, Value>, path: &Path) -> Result<(), PersistError> {
    let tmp = path.with_extension("tmp"); // snapshot.vrdb → snapshot.tmp
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&VERSION.to_le_bytes());
    put_u32(&mut buf, map.len() as u32);
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();
    for key in keys {
        put_str(&mut buf, key);
        encode_value(&mut buf, &map[key]);
    }
    // 尾校验：覆盖此前全部字节
    buf.extend_from_slice(&fnv1a(&buf).to_le_bytes());
    // tmp → fsync → 原子替换
    let mut file = File::create(&tmp)
        .map_err(|e| PersistError::Io(format!("创建快照临时文件失败: {e}")))?;
    file.write_all(&buf)
        .map_err(|e| PersistError::Io(format!("写快照失败: {e}")))?;
    file.sync_all()
        .map_err(|e| PersistError::Io(format!("快照 fsync 失败: {e}")))?;
    drop(file);
    // Windows 的 rename 在目标已存在时会失败（POSIX 语义才是原子覆盖），
    // 因此 Windows 下先移除旧快照再 rename。remove 与 rename 之间存在极短窗口期
    // （此刻磁盘上没有快照文件，若恰在此刻崩溃则数据仅由 WAL 兜底），
    // 这是零第三方依赖约束下的已知取舍。
    #[cfg(windows)]
    {
        let _ = std::fs::remove_file(path); // 目标不存在（首次保存）不算错误
        std::fs::rename(&tmp, path)
            .map_err(|e| PersistError::Io(format!("快照原子替换失败: {e}")))?;
    }
    #[cfg(not(windows))]
    std::fs::rename(&tmp, path)
        .map_err(|e| PersistError::Io(format!("快照原子替换失败: {e}")))?;
    Ok(())
}

/// 加载快照；magic / 版本 / 尾校验 / 结构任何不符 → `Err(Corrupt)`。
pub fn load(path: &Path) -> Result<HashMap<String, Value>, PersistError> {
    let data =
        std::fs::read(path).map_err(|e| PersistError::Io(format!("读快照失败: {e}")))?;
    if data.len() < 12 {
        return Err(corrupt("快照文件过短"));
    }
    if le_u32(&data[0..4]) != MAGIC {
        return Err(corrupt("快照 magic 不符"));
    }
    if le_u32(&data[4..8]) != VERSION {
        return Err(corrupt(&format!("快照版本不兼容: {}", le_u32(&data[4..8]))));
    }
    // 尾校验：最后 4 字节是此前全部字节的 FNV-1a
    let (body, tail) = data.split_at(data.len() - 4);
    if fnv1a(body) != le_u32(tail) {
        return Err(corrupt("快照尾校验和不符"));
    }
    // 不按声明条目数预分配（与 decode_value 同理：上限校验 + 按实际数据增长）
    let count = le_u32(&body[8..12]) as usize;
    let mut cur = Cursor::new(&body[12..]);
    let mut map = HashMap::new();
    for _ in 0..count {
        let key = cur.read_str().ok_or_else(|| corrupt("快照条目 key 不完整"))?;
        let value = decode_value(&mut cur)?;
        map.insert(key, value);
    }
    if !cur.at_end() {
        return Err(corrupt("快照尾部有多余字节"));
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// 唯一临时文件路径（测试辅助）。
    fn temp_file(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("vredis-persist-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join(tag)
    }

    fn sample_map() -> HashMap<String, Value> {
        let mut map = HashMap::new();
        map.insert("s".to_string(), Value::Str(b"hello".to_vec()));
        map.insert(
            "ix".to_string(),
            Value::VectorIndex {
                dim: 2,
                vectors: [("0".to_string(), vec![1.0, -2.5]), ("1".to_string(), vec![0.0, 3.25])]
                    .into_iter()
                    .collect(),
                next_id: 5,
            },
        );
        map
    }

    #[test]
    fn p07_snapshot_roundtrip_with_next_id() {
        let path = temp_file("snapshot.vrdb");
        let map = sample_map();
        save(&map, &path).expect("save");
        let loaded = load(&path).expect("load");
        assert_eq!(loaded, map);
    }

    #[test]
    fn p08_snapshot_checksum_corruption_is_error() {
        let path = temp_file("snapshot.vrdb");
        save(&sample_map(), &path).expect("save");
        let mut data = std::fs::read(&path).expect("read");
        // 翻转条目区一个字节（避开 magic 与尾部校验和本身）
        let last = data.len() - 6;
        data[last] ^= 0xFF;
        std::fs::write(&path, &data).expect("write");
        assert!(matches!(load(&path), Err(PersistError::Corrupt(_))));
    }

    #[test]
    fn p09_snapshot_bad_magic_is_error() {
        let path = temp_file("snapshot.vrdb");
        let mut data = vec![0u8; 16];
        data[0..4].copy_from_slice(&0x00_00_00_00u32.to_le_bytes()); // 坏 magic
        std::fs::write(&path, &data).expect("write");
        assert!(matches!(load(&path), Err(PersistError::Corrupt(_))));
    }

    #[test]
    fn p10_save_twice_to_same_path() {
        // P0 回归测试：Windows 下 rename 不能覆盖已存在目标，
        // 第二次 BGSAVE（即对同一 path 连续 save）必须成功。
        let path = temp_file("snapshot.vrdb");
        let mut map = sample_map();
        save(&map, &path).expect("first save");
        map.insert("b".to_string(), Value::Str(b"2".to_vec()));
        save(&map, &path).expect("second save must not fail on existing target");
        let loaded = load(&path).expect("load");
        assert_eq!(loaded, map);
        // rename 生效后不应残留临时文件
        assert!(!path.with_extension("tmp").exists());
    }
}
