//! 持久化层：WAL（预写日志）+ 快照 + 启动恢复（docs/design.md §3.1 v0.4）。
//!
//! 与 storage 为 crate 内兄弟模块互引（见 design.md D7）：
//! - 本层需要 `storage::Value`（WAL 的 SET 与快照都要序列化完整值）；
//! - storage 需要 `persist::wal::WalWriter`（写路径先行落日志）。
//!
//! 全部格式自研、零第三方依赖：长度前缀 + FNV-1a 32 位校验和，小端字节序。

pub mod snapshot;
pub mod wal;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::storage::Value;

/// 持久化配置：数据目录。
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// 数据目录（默认 = 可执行文件所在目录下的 `data/`；测试注入临时目录）
    pub dir: PathBuf,
}

impl Default for PersistConfig {
    fn default() -> Self {
        // 相对可执行文件所在目录，而非进程 CWD（进阶 1 需求第 4 条）
        let dir = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."))
            .join("data");
        Self { dir }
    }
}

impl PersistConfig {
    /// 快照文件路径。
    pub fn snapshot_path(&self) -> PathBuf {
        snapshot_file_in(&self.dir)
    }

    /// WAL 文件路径。
    pub fn wal_path(&self) -> PathBuf {
        wal_file_in(&self.dir)
    }
}

/// 数据目录下快照文件的固定路径（storage 的 bgsave 复用，避免文件名两处维护）。
pub(crate) fn snapshot_file_in(dir: &Path) -> PathBuf {
    dir.join("snapshot.vrdb")
}

/// 数据目录下 WAL 文件的固定路径。
pub(crate) fn wal_file_in(dir: &Path) -> PathBuf {
    dir.join("wal.log")
}

/// 持久化错误。
///
/// - `Io`：读写失败（含磁盘满等）；写路径收到它必须让操作失败（design.md D11）。
/// - `Corrupt`：数据损坏（magic/校验和/结构非法）；启动恢复遇到必须拒绝启动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistError {
    Io(String),
    Corrupt(String),
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistError::Io(msg) => write!(f, "{msg}"),
            PersistError::Corrupt(msg) => write!(f, "数据损坏: {msg}"),
        }
    }
}

/// 构造 Corrupt 错误的便捷函数。
pub(crate) fn corrupt(msg: &str) -> PersistError {
    PersistError::Corrupt(msg.to_string())
}

// ── 防御纵深（用户确认的进阶 1 第 2 步补充）──
// 物理损坏可能伪造出超大 count/dim，必须在其触发 with_capacity 预分配之前拒绝。

/// 单个索引内向量数上限。
pub(crate) const MAX_VECTORS_PER_INDEX: u32 = 10_000_000;
/// 单个向量维度上限。
pub(crate) const MAX_VECTOR_DIM: u32 = 65536;
/// 单条 DEL 命令的 key 数上限（与 VADD/索引防御同类的解码防护）。
pub(crate) const MAX_KEYS_PER_DEL: u32 = 1_000_000;

// ── 共享编解码原语（WAL 与快照复用；固定小端字节序）──

/// FNV-1a 32 位校验和（std 手写，~10 行，零第三方依赖）。
pub fn fnv1a(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C_9DC5;
    for &byte in data {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// 从切片头 4 字节读小端 u32（调用方保证切片长度）。
pub(crate) fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub(crate) fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

pub(crate) fn put_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    put_u32(buf, data.len() as u32);
    buf.extend_from_slice(data);
}

/// 只进游标：从字节切片顺序读取；任何读取不足返回 None（由调用方转为 Corrupt）。
pub(crate) struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// 是否已读到末尾（用于拒绝"尾部有多余字节"的结构性损坏）。
    pub(crate) fn at_end(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let rest = self.data.get(self.pos..)?;
        if rest.len() < n {
            return None;
        }
        self.pos += n;
        Some(&rest[..n])
    }

    pub(crate) fn read_u8(&mut self) -> Option<u8> {
        let byte = *self.take(1)?.first()?;
        Some(byte)
    }

    pub(crate) fn read_u32(&mut self) -> Option<u32> {
        let bytes = self.take(4)?;
        Some(le_u32(bytes))
    }

    pub(crate) fn read_u64(&mut self) -> Option<u64> {
        let bytes = self.take(8)?;
        Some(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(crate) fn read_f32(&mut self) -> Option<f32> {
        let bytes = self.take(4)?;
        Some(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// 长度前缀字节串。
    pub(crate) fn read_bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    /// 长度前缀 UTF-8 字符串；非法 UTF-8 → None（调用方转 Corrupt）。
    pub(crate) fn read_str(&mut self) -> Option<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes.to_vec()).ok()
    }
}

/// 编码一个存储值（WAL 的 SET 与快照共用；SET 可覆盖向量索引，必须还原整值）。
pub fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(data) => {
            buf.push(0x01);
            put_bytes(buf, data);
        }
        Value::VectorIndex { dim, vectors, next_id } => {
            buf.push(0x02);
            put_u32(buf, *dim as u32);
            put_u64(buf, *next_id);
            put_u32(buf, vectors.len() as u32);
            // 按 id 排序写入：序列化确定性（便于测试与人工比对）
            let mut ids: Vec<&String> = vectors.keys().collect();
            ids.sort();
            for id in ids {
                put_str(buf, id);
                let data = &vectors[id];
                put_u32(buf, data.len() as u32);
                for f in data {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }
}

/// 解码一个存储值；任何结构非法 → Corrupt。
pub(crate) fn decode_value(cur: &mut Cursor<'_>) -> Result<Value, PersistError> {
    match cur.read_u8() {
        Some(0x01) => {
            let data = cur.read_bytes().ok_or_else(|| corrupt("Str 值不完整"))?;
            Ok(Value::Str(data.to_vec()))
        }
        Some(0x02) => {
            let dim = cur.read_u32().ok_or_else(|| corrupt("索引维度不完整"))? as usize;
            if dim as u32 > MAX_VECTOR_DIM {
                return Err(corrupt(&format!("索引维度 {dim} 超过上限 {MAX_VECTOR_DIM}")));
            }
            let next_id = cur.read_u64().ok_or_else(|| corrupt("索引计数器不完整"))?;
            let count = cur.read_u32().ok_or_else(|| corrupt("索引向量数不完整"))? as usize;
            if count as u64 > u64::from(MAX_VECTORS_PER_INDEX) {
                return Err(corrupt(&format!(
                    "索引向量数 {count} 超过上限 {MAX_VECTORS_PER_INDEX}"
                )));
            }
            // 不按 count 预分配：上限只是合法性校验，内存增长始终与实际解码数据成正比
            let mut vectors = HashMap::new();
            for _ in 0..count {
                let id = cur.read_str().ok_or_else(|| corrupt("向量 id 不完整"))?;
                let n = cur.read_u32().ok_or_else(|| corrupt("向量分量数不完整"))? as usize;
                if n != dim {
                    return Err(corrupt(&format!(
                        "向量分量数 {n} 与索引维度 {dim} 不符"
                    )));
                }
                let mut data = Vec::with_capacity(n);
                for _ in 0..n {
                    data.push(cur.read_f32().ok_or_else(|| corrupt("向量分量不完整"))?);
                }
                vectors.insert(id, data);
            }
            Ok(Value::VectorIndex { dim, vectors, next_id })
        }
        _ => Err(corrupt("未知的值类型码")),
    }
}
