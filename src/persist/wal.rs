//! WAL（预写日志）：记录格式、追加写入、启动重放（docs/design.md §3.1 v0.4）。
//!
//! 损坏语义（design.md D10，用户确认）：
//! - **撕裂尾**：文件尾部字节不足一条完整记录（进程崩溃在写入中途）→
//!   `replay` 返回有效长度，调用方截断并警告后继续（该操作从未被 ack，丢弃安全）；
//! - **真损坏**：完整记录但 magic/校验和/结构非法 → `Err`（启动时拒绝服务，不静默丢数据）。
//!
//! 刷盘策略（design.md D12）：每条 `append` 后 flush（不 fsync）——
//! 进程崩溃不丢（数据在 OS 页缓存）；断电最多丢 WAL 尾部。

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::{
    corrupt, decode_value, encode_value, fnv1a, le_u32, put_str, put_u32, Cursor,
    MAX_KEYS_PER_DEL, MAX_VECTOR_DIM, PersistError,
};
use crate::storage::Value;

/// WAL 记录 magic："VRLE"（Vredis Log Entry）。
const MAGIC: u32 = 0x56_52_4C_45;

const OP_SET: u8 = 0x01;
const OP_DEL: u8 = 0x02;
const OP_FLUSH_ALL: u8 = 0x03;
const OP_VADD: u8 = 0x04;

/// 一条 WAL 记录：与写命令一一对应。
///
/// `Set` 携带**完整 Value**（复用快照的值编解码）：SET 可以覆盖向量索引，
/// 只有记录整值才能保证重放后状态与崩溃前一致。
#[derive(Debug, Clone, PartialEq)]
pub enum WalEntry {
    Set { key: String, value: Value },
    Del { keys: Vec<String> },
    FlushAll,
    VAdd { key: String, data: Vec<f32> },
}

impl WalEntry {
    /// 编码 payload（不含 magic/长度/校验头）。
    fn payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            WalEntry::Set { key, value } => {
                buf.push(OP_SET);
                put_str(&mut buf, key);
                encode_value(&mut buf, value);
            }
            WalEntry::Del { keys } => {
                buf.push(OP_DEL);
                put_u32(&mut buf, keys.len() as u32);
                for key in keys {
                    put_str(&mut buf, key);
                }
            }
            WalEntry::FlushAll => buf.push(OP_FLUSH_ALL),
            WalEntry::VAdd { key, data } => {
                buf.push(OP_VADD);
                put_str(&mut buf, key);
                put_u32(&mut buf, data.len() as u32);
                for f in data {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
        buf
    }

    /// 从 payload 解码；任何结构非法 → Corrupt。
    fn from_payload(payload: &[u8]) -> Result<WalEntry, PersistError> {
        let mut cur = Cursor::new(payload);
        let entry = match cur.read_u8() {
            Some(OP_SET) => {
                let key = cur.read_str().ok_or_else(|| corrupt("SET 条目 key 不完整"))?;
                let value = decode_value(&mut cur)?;
                WalEntry::Set { key, value }
            }
            Some(OP_DEL) => {
                let count = cur.read_u32().ok_or_else(|| corrupt("DEL 条目数量不完整"))? as usize;
                // 与 VADD/索引解码同类的防御：损坏的超大 count 不得触发超大 Vec 预分配
                if count as u64 > u64::from(MAX_KEYS_PER_DEL) {
                    return Err(corrupt(&format!(
                        "DEL key 数 {count} 超过上限 {MAX_KEYS_PER_DEL}"
                    )));
                }
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(cur.read_str().ok_or_else(|| corrupt("DEL 条目 key 不完整"))?);
                }
                WalEntry::Del { keys }
            }
            Some(OP_FLUSH_ALL) => WalEntry::FlushAll,
            Some(OP_VADD) => {
                let key = cur.read_str().ok_or_else(|| corrupt("VADD 条目 key 不完整"))?;
                let dim = cur.read_u32().ok_or_else(|| corrupt("VADD 条目维度不完整"))? as usize;
                // 与 decode_value 同类防御：损坏的超大 dim 不得触发超大 Vec 预分配
                if dim as u32 > MAX_VECTOR_DIM {
                    return Err(corrupt(&format!("VADD 维度 {dim} 超过上限 {MAX_VECTOR_DIM}")));
                }
                let mut data = Vec::with_capacity(dim);
                for _ in 0..dim {
                    data.push(cur.read_f32().ok_or_else(|| corrupt("VADD 条目分量不完整"))?);
                }
                WalEntry::VAdd { key, data }
            }
            _ => return Err(corrupt("未知的 WAL 操作码")),
        };
        if !cur.at_end() {
            return Err(corrupt("WAL 条目尾部有多余字节"));
        }
        Ok(entry)
    }
}

/// WAL 追加写入器；保留路径供 BGSAVE 截断重开。
#[derive(Debug)]
pub struct WalWriter {
    file: File,
    path: PathBuf,
}

impl WalWriter {
    /// 以追加模式打开（不存在则创建）。
    pub fn open(path: &Path) -> Result<WalWriter, PersistError> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|e| PersistError::Io(format!("打开 WAL 失败: {e}")))?;
        Ok(WalWriter { file, path: path.to_path_buf() })
    }

    /// 截断为空并重开（BGSAVE 成功后调用；重新打开保证跨平台偏移语义正确）。
    pub fn open_truncated(path: &Path) -> Result<WalWriter, PersistError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| PersistError::Io(format!("截断 WAL 失败: {e}")))?;
        Ok(WalWriter { file, path: path.to_path_buf() })
    }

    /// 截断自身持有的 WAL 文件并返回新写入器（BGSAVE 成功后调用）。
    pub fn truncate_and_reopen(&self) -> Result<WalWriter, PersistError> {
        Self::open_truncated(&self.path)
    }

    /// 追加一条记录并 flush（不 fsync，design.md D12）。
    /// 失败由调用方把引擎置为 Broken（design.md D11）。
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), PersistError> {
        let payload = entry.payload();
        let mut record = Vec::with_capacity(12 + payload.len());
        record.extend_from_slice(&MAGIC.to_le_bytes());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(&fnv1a(&payload).to_le_bytes());
        record.extend_from_slice(&payload);
        self.file
            .write_all(&record)
            .and_then(|()| self.file.flush())
            .map_err(|e| PersistError::Io(format!("写 WAL 失败: {e}")))
    }
}

/// 重放结果：合法条目序列 + 有效字节数（撕裂尾之前的长度）。
#[derive(Debug)]
pub struct Replay {
    pub entries: Vec<WalEntry>,
    pub valid_len: u64,
}

/// 重放整个 WAL 文件（只读，不修改文件；截断由调用方完成）。
pub fn replay(path: &Path) -> Result<Replay, PersistError> {
    let data =
        std::fs::read(path).map_err(|e| PersistError::Io(format!("读 WAL 失败: {e}")))?;
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        let rest = &data[pos..];
        // 撕裂尾形态 1：连 12 字节头都不完整
        if rest.len() < 12 {
            break;
        }
        // 头部完整但 magic 不符：不可能是任何合法记录的撕裂前缀
        //（撕裂必然保留记录自身的 magic）→ 真损坏
        if le_u32(&rest[0..4]) != MAGIC {
            return Err(corrupt(&format!("WAL 偏移 {pos} 处 magic 不符")));
        }
        let len = le_u32(&rest[4..8]) as usize;
        let checksum = le_u32(&rest[8..12]);
        // 撕裂尾形态 2：payload 不完整
        if rest.len() < 12 + len {
            break;
        }
        let payload = &rest[12..12 + len];
        if fnv1a(payload) != checksum {
            return Err(corrupt(&format!("WAL 偏移 {pos} 处校验和不符")));
        }
        entries.push(WalEntry::from_payload(payload)?);
        pos += 12 + len;
    }
    Ok(Replay { entries, valid_len: pos as u64 })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 唯一临时文件路径（测试辅助；自动创建并按计数器去重）。
    fn temp_file(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("vredis-persist-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join(tag)
    }

    #[test]
    fn p01_wal_entry_codec_roundtrip() {
        let entries = vec![
            WalEntry::Set { key: "a".into(), value: Value::Str(b"hello".to_vec()) },
            WalEntry::Set {
                key: "ix".into(),
                value: Value::VectorIndex {
                    dim: 2,
                    vectors: [("0".into(), vec![1.0, 2.0])].into_iter().collect(),
                    next_id: 7,
                },
            },
            WalEntry::Del { keys: vec!["a".into(), "b".into()] },
            WalEntry::FlushAll,
            WalEntry::VAdd { key: "ix".into(), data: vec![1.5, -2.0, 0.0] },
        ];
        for entry in entries {
            let payload = entry.payload();
            assert_eq!(WalEntry::from_payload(&payload).expect("decode"), entry);
        }
    }

    #[test]
    fn p02_append_then_replay_identical() {
        let path = temp_file("wal.log");
        let mut writer = WalWriter::open(&path).expect("open");
        let entries = vec![
            WalEntry::Set { key: "a".into(), value: Value::Str(b"1".to_vec()) },
            WalEntry::VAdd { key: "ix".into(), data: vec![1.0, 0.0] },
            WalEntry::Del { keys: vec!["a".into()] },
        ];
        for entry in &entries {
            writer.append(entry).expect("append");
        }
        drop(writer);
        let replay = replay(&path).expect("replay");
        assert_eq!(replay.entries, entries);
        assert_eq!(
            replay.valid_len,
            std::fs::metadata(&path).expect("meta").len()
        );
    }

    #[test]
    fn p03_checksum_corruption_is_error() {
        let path = temp_file("wal.log");
        let mut writer = WalWriter::open(&path).expect("open");
        writer
            .append(&WalEntry::Set { key: "a".into(), value: Value::Str(b"xyz".to_vec()) })
            .expect("append");
        drop(writer);
        // 翻转最后一个 payload 字节：完整记录 + 校验和不符 → 真损坏
        let mut data = std::fs::read(&path).expect("read");
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&path, &data).expect("write");
        assert!(matches!(replay(&path), Err(PersistError::Corrupt(_))));
    }

    #[test]
    fn p04_bad_magic_is_error() {
        let path = temp_file("wal.log");
        let mut writer = WalWriter::open(&path).expect("open");
        writer
            .append(&WalEntry::Set { key: "a".into(), value: Value::Str(b"1".to_vec()) })
            .expect("append");
        drop(writer);
        let mut data = std::fs::read(&path).expect("read");
        data[0] ^= 0xFF;
        std::fs::write(&path, &data).expect("write");
        assert!(matches!(replay(&path), Err(PersistError::Corrupt(_))));
    }

    #[test]
    fn p05_torn_tail_discarded() {
        let path = temp_file("wal.log");
        let mut writer = WalWriter::open(&path).expect("open");
        writer
            .append(&WalEntry::Set { key: "a".into(), value: Value::Str(b"keep".to_vec()) })
            .expect("append first");
        // 记录第一条写入后的文件长度 = 撕裂截断后应有的有效长度
        let valid_len = std::fs::metadata(&path).expect("meta").len();
        writer
            .append(&WalEntry::Set { key: "b".into(), value: Value::Str(b"lost".to_vec()) })
            .expect("append second");
        drop(writer);
        // 模拟崩溃在第二条写入中途：截掉最后 3 字节
        let full = std::fs::read(&path).expect("read");
        std::fs::write(&path, &full[..full.len() - 3]).expect("write");
        // 撕裂尾不是错误：保留完整前缀
        let replay = replay(&path).expect("torn tail must not error");
        assert_eq!(replay.entries.len(), 1);
        assert_eq!(
            replay.entries[0],
            WalEntry::Set { key: "a".into(), value: Value::Str(b"keep".to_vec()) }
        );
        assert_eq!(replay.valid_len, valid_len);
    }

    #[test]
    fn p06_empty_file_is_fresh() {
        let path = temp_file("wal.log");
        std::fs::write(&path, b"").expect("write");
        let replay = replay(&path).expect("replay");
        assert!(replay.entries.is_empty());
        assert_eq!(replay.valid_len, 0);
    }
}
