//! 存储层：key → Value 的内存表 + WAL 先行落盘（docs/design.md §3 / §3.1 v0.4）。
//!
//! 引擎状态（内存表 + WAL + 数据目录）统一在**一把 Mutex** 内：
//! 四条写路径（set/del/flush/vector_add）在**同一临界区**内完成
//! 「先写 WAL 再改内存」（design.md D9）——多线程下 WAL 顺序 == 内存顺序，
//! 崩溃重放必然收敛到崩溃前的最后状态。锁毒化采用纵深防御（见 `lock_inner`）。
//!
//! WAL 状态机（design.md D11）：
//! - `Off`：纯内存模式（`Db::new` / 恢复重放期间），写操作跳过日志；
//! - `Active`：正常持久化；
//! - `Broken`：WAL 写失败后的只读态——所有写操作立即失败，BGSAVE 成功后自愈。

mod value;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::persist::wal::{self, WalEntry, WalWriter};
use crate::persist::{fnv1a, snapshot, PersistConfig, PersistError};
use crate::vector::hnsw::HnswIndex;
use crate::vector::search::Hit;
use crate::vector::Metric;

pub use value::Value;

/// VADD 的错误（命令层映射：NotAnIndex → WRONGTYPE；DimensionMismatch → invalid
/// vector dimension；Persist → `-ERR persist failed: <reason>`；
/// Internal → `-ERR internal error`（不变量违例，如实上抛）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddVectorError {
    /// key 存的是字符串（非向量索引）
    NotAnIndex,
    /// 索引维度已锁定，与本次分量数不符（携带已锁定维度）
    DimensionMismatch(usize),
    /// WAL 写失败（design.md D11：内存未动，引擎转入 Broken）
    Persist(PersistError),
    /// HNSW 索引内部不变量违例（不应发生；章程禁 panic，如实上抛）
    Internal(String),
}

/// VGET 的错误（key 是字符串，非索引）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetVectorError {
    NotAnIndex,
}

/// 索引探测结果（VDIM / VSEARCH 前置校验用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexProbe {
    /// 索引存在：维度（创建时锁定）与 metric（HNSW 建图锁定，VSEARCH 分发用）
    Index { dim: usize, metric: Metric },
    /// key 不存在
    Missing,
    /// key 存在但不是向量索引
    NotAnIndex,
}

/// [`Db::for_each_vector`] 的遍历结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisitOutcome {
    /// 遍历完成
    Traversed,
    /// key 不存在
    Missing,
    /// key 存在但不是向量索引
    NotAnIndex,
}

// ── HNSW 建图常量（进阶 3；召回率已由 h11/h12 验证为 1.0）──
/// 每层最大邻居数 m（论文默认值）
const HNSW_M: usize = 16;
/// 建图搜索宽度 ef_construction
const HNSW_EF_CONSTRUCTION: usize = 200;

/// HNSW 建图 seed：`seed = u64::from(fnv1a(key.as_bytes()))`——
/// 同 key 重建时 seed 确定性一致（重启重建后图与崩溃前逐项一致）。
fn hnsw_seed(key: &str) -> u64 {
    u64::from(fnv1a(key.as_bytes()))
}

/// WAL 状态机（design.md D11）。
#[derive(Debug, Default)]
enum WalState {
    /// 纯内存模式：不持久化（默认态）
    #[default]
    Off,
    /// 正常持久化
    Active(WalWriter),
    /// WAL 写失败后的只读态；BGSAVE 成功后回到 Active
    Broken,
}

/// 引擎状态：内存表 + WAL + 数据目录 + HNSW 索引，整体在一把锁内。
#[derive(Debug, Default)]
struct DbInner {
    map: HashMap<String, Value>,
    wal: WalState,
    /// 数据目录；None = 纯内存模式（bgsave 为 no-op）
    dir: Option<PathBuf>,
    /// HNSW 索引（纯内存，重启重建）：key → 索引，与 map 中的 VectorIndex 一一对应
    ///（仅在 hnsw_enabled 时维护）
    hnsw: HashMap<String, HnswIndex>,
    /// HNSW 启用开关（进阶 3 第 4 步）：false = 纯暴力搜索（默认），
    /// 由 main 的环境变量 `VREDIS_HNSW` 或测试注入决定
    hnsw_enabled: bool,
}

impl DbInner {
    /// 「先写 WAL 再改内存」的公共入口（design.md D9/D11）。
    ///
    /// - `Off` → 跳过（纯内存）；
    /// - `Active` → 追加记录；写失败 → 置 `Broken` 并返回 `Err`（此时**内存尚未改动**）；
    /// - `Broken` → 立即 `Err`（服务器只读）。
    ///
    /// 调用方在本函数返回 `Ok` 之后才允许修改 map——两者在同一临界区内。
    fn write_through(&mut self, make_entry: impl FnOnce() -> WalEntry) -> Result<(), PersistError> {
        let result = match &mut self.wal {
            WalState::Off => Ok(()),
            WalState::Broken => Err(PersistError::Io(
                "WAL 此前写入失败，服务器处于只读状态（BGSAVE 成功后自愈）".to_string(),
            )),
            WalState::Active(writer) => writer.append(&make_entry()),
        };
        if result.is_err() {
            // 内存未动（调用方尚未修改 map）；置 Broken 后所有写操作立即失败
            self.wal = WalState::Broken;
        }
        result
    }
}

/// 数据库本体：key → Value，进程内唯一，Arc 跨线程共享。
#[derive(Debug, Default)]
pub struct Db {
    inner: Mutex<DbInner>,
}

impl Db {
    /// 纯内存模式（无持久化）：供测试与内部使用；生产入口是 [`Db::open`]。
    pub fn new() -> Self {
        Self { inner: Mutex::new(DbInner::default()) }
    }

    // ── 启动恢复（design.md §3.1 v0.4）──

    /// 打开（含恢复）：加载快照 → 重放 WAL → 截断撕裂尾 → attach 追加 writer。
    /// 默认**不启用** HNSW（纯暴力搜索，进阶 3 第 4 步）；启用见 [`Db::open_with`]。
    pub fn open(cfg: &PersistConfig) -> Result<(Db, Vec<String>), PersistError> {
        Self::open_with(cfg, false)
    }

    /// 打开（含恢复）并可选启用 HNSW 索引（进阶 3 第 4 步）。
    ///
    /// - 快照或 WAL **真损坏** → `Err`（main 打印并以非零码退出，不静默丢数据）；
    /// - **撕裂尾**（进程崩溃在写入中途）→ 截断到最后一条完整记录，
    ///   警告文本放入返回的 `Vec<String>`（由 main 打印到 stderr）；
    /// - 重放期间 WAL 状态为 `Off`（重放绝不写日志），完成后才 attach `Active`；
    /// - **已重放的 WAL 原样保留**（只截撕裂尾）——清空会在“无新快照又崩溃”时丢数据；
    ///   仅 BGSAVE 成功后截断（design.md D4）；
    /// - `hnsw_enabled = true` 时：恢复完成后按 id 数值序重建全部 HNSW，
    ///   且后续写路径同步维护索引；`false` 时完全不建图（纯暴力搜索）。
    pub fn open_with(
        cfg: &PersistConfig,
        hnsw_enabled: bool,
    ) -> Result<(Db, Vec<String>), PersistError> {
        std::fs::create_dir_all(&cfg.dir)
            .map_err(|e| PersistError::Io(format!("创建数据目录失败: {e}")))?;
        let mut map = HashMap::new();
        let mut warnings = Vec::new();
        // 1) 快照（若有；损坏 → Err）
        let snapshot = cfg.snapshot_path();
        if snapshot.exists() {
            map = snapshot::load(&snapshot)?;
        }
        // 2) WAL 重放（若有）：保持追加顺序，自增 id 确定性复现
        let wal_path = cfg.wal_path();
        if wal_path.exists() {
            let replay = wal::replay(&wal_path)?;
            for entry in replay.entries {
                apply_recovered(&mut map, entry)?;
            }
            // 3) 撕裂尾截断：只丢弃崩溃残留字节
            let size = std::fs::metadata(&wal_path)
                .map_err(|e| PersistError::Io(format!("读 WAL 元数据失败: {e}")))?
                .len();
            if size > replay.valid_len {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&wal_path)
                    .map_err(|e| PersistError::Io(format!("打开 WAL 截断失败: {e}")))?;
                file.set_len(replay.valid_len)
                    .map_err(|e| PersistError::Io(format!("截断 WAL 撕裂尾失败: {e}")))?;
                warnings.push(format!(
                    "WAL 尾部不完整记录已截断（崩溃残留），丢弃 {} 字节",
                    size - replay.valid_len
                ));
            }
        }
        // 4) HNSW 重建（纯内存结构不持久化；仅启用时）：遍历恢复出的全部向量索引，
        //    按 id 数值序（== 原插入序）逐条重插；seed = fnv1a(key) 确定性，
        //    重建后图与崩溃前逐项一致。失败 → Corrupt（响亮退出）
        let mut hnsw = HashMap::new();
        if hnsw_enabled {
            for (key, value) in &map {
                if let Value::VectorIndex { vectors, metric, .. } = value {
                    let mut ix =
                        HnswIndex::new(*metric, HNSW_M, HNSW_EF_CONSTRUCTION, hnsw_seed(key))
                            .map_err(|e| {
                                PersistError::Corrupt(format!("HNSW 重建失败（{key}）: {e}"))
                            })?;
                    let mut ids: Vec<&String> = vectors.keys().collect();
                    ids.sort_by_key(|id| id.parse::<u64>().unwrap_or(0));
                    for id in &ids {
                        ix.insert(id, &vectors[*id]).map_err(|e| {
                            PersistError::Corrupt(format!(
                                "HNSW 重建失败（{key}/{id}）: {e}"
                            ))
                        })?;
                    }
                    hnsw.insert(key.clone(), ix);
                }
            }
        }
        // 5) attach 追加 writer（不截断：已重放内容必须保留）
        let writer = WalWriter::open(&wal_path)?;
        Ok((
            Db {
                inner: Mutex::new(DbInner {
                    map,
                    wal: WalState::Active(writer),
                    dir: Some(cfg.dir.clone()),
                    hnsw,
                    hnsw_enabled,
                }),
            },
            warnings,
        ))
    }

    /// BGSAVE：引擎锁内整库序列化 → tmp + fsync + 原子 rename 替换快照 →
    /// 截断并重开 WAL（清空已落地部分，design.md §4.1 v0.4）。
    ///
    /// - `Off`（纯内存，无数据目录）→ no-op `Ok`；
    /// - `Broken` → 借此自愈（内存数据仍完整，直接落快照重建 WAL）；
    /// - 同步实现，无后台线程（MVP 约束）；
    /// - **不动 HNSW**（纯内存结构，不持久化；重启时从恢复的数据重建）。
    pub fn bgsave(&self) -> Result<(), PersistError> {
        let mut inner = self.lock_inner();
        let Some(dir) = inner.dir.clone() else {
            return Ok(());
        };
        // 快照失败 → Err，WAL 状态不变（仍是 Active/Broken，数据不受影响）
        snapshot::save(&inner.map, &crate::persist::snapshot_file_in(&dir))?;
        // 快照已包含全部数据：截断重开 WAL；Broken 在此清除（自愈）
        inner.wal = WalState::Active(WalWriter::open_truncated(&crate::persist::wal_file_in(&dir))?);
        Ok(())
    }

    // ── KV 读写 ──

    /// 写入 / 覆盖 key（覆盖任意已存在类型，与 Redis SET 语义一致）。
    ///
    /// ★ 同一临界区内「先写 WAL 再改内存」；WAL 失败 → `Err` 且内存未动。
    pub fn set(&self, key: impl Into<String>, value: Value) -> Result<(), PersistError> {
        let key = key.into();
        let mut inner = self.lock_inner();
        inner.write_through(|| WalEntry::Set { key: key.clone(), value: value.clone() })?;
        // SET 覆盖任意类型：同 key 的 HNSW 索引（若有）随之作废（同一临界区内）
        inner.hnsw.remove(&key);
        inner.map.insert(key, value);
        Ok(())
    }

    /// 快照式读取：存在则克隆返回。MVP 值都很小，克隆换取锁使用的简单性。
    pub fn get(&self, key: &str) -> Option<Value> {
        self.lock_inner().map.get(key).cloned()
    }

    /// 删除一组 key，返回实际删除的数量。
    ///
    /// ★ 同一临界区内「先写 WAL 再改内存」。
    pub fn del(&self, keys: &[String]) -> Result<usize, PersistError> {
        let mut inner = self.lock_inner();
        inner.write_through(|| WalEntry::Del { keys: keys.to_vec() })?;
        let mut removed = 0;
        for key in keys {
            if inner.map.remove(key.as_str()).is_some() {
                // 命中的若是向量索引，其 HNSW 同步作废（同一临界区内）
                inner.hnsw.remove(key.as_str());
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// 统计存在的 key 数量；重复 key 重复计数（与 Redis EXISTS 一致）。只读操作。
    pub fn exists_any(&self, keys: &[String]) -> usize {
        let inner = self.lock_inner();
        keys.iter().filter(|k| inner.map.contains_key(k.as_str())).count()
    }

    /// 返回当前全部 key（顺序不保证）。只读操作。
    pub fn keys(&self) -> Vec<String> {
        self.lock_inner().map.keys().cloned().collect()
    }

    /// 清空全部数据。
    ///
    /// ★ 同一临界区内「先写 WAL 再改内存」。
    pub fn flush(&self) -> Result<(), PersistError> {
        let mut inner = self.lock_inner();
        inner.write_through(|| WalEntry::FlushAll)?;
        inner.map.clear();
        inner.hnsw.clear();
        Ok(())
    }

    // ── 向量 API（v0.3）：遍历/探测单次持锁，禁止把索引克隆出锁外 ──

    /// 向索引添加向量（缺省 metric = Cosine）。见 [`Db::vector_add_with_metric`]。
    pub fn vector_add(&self, key: &str, data: Vec<f32>) -> Result<String, AddVectorError> {
        self.vector_add_with_metric(key, data, Metric::Cosine)
    }

    /// 指定度量并向索引添加向量，返回自动生成的 id（索引内自增十进制串，从 "0" 起）。
    /// 索引不存在则创建并锁定维度与 metric（dim = data.len()）。
    ///
    /// ★ 同一临界区内：先写 WAL（VADD 记录 key + 分量 + metric）再改内存
    /// （map 与 HNSW 同步更新）；WAL 失败 → [`AddVectorError::Persist`] 且内存未动
    /// （design.md D11）。原子性：维度/metric 检查 + id 生成 + 插入 + 计数器递增
    /// 一次完成，并发 VADD 不会产生重复 id 或漏计数。
    pub fn vector_add_with_metric(
        &self,
        key: &str,
        data: Vec<f32>,
        metric: Metric,
    ) -> Result<String, AddVectorError> {
        let dim = data.len();
        let mut inner = self.lock_inner();
        inner
            .write_through(|| WalEntry::VAdd {
                key: key.to_string(),
                data: data.clone(),
                metric,
            })
            .map_err(AddVectorError::Persist)?;
        match inner.map.get_mut(key) {
            None => {
                let id = "0".to_string();
                let vectors = HashMap::from([(id.clone(), data)]);
                // HNSW 维护仅在启用时进行（未启用 → 纯内存 + 暴力搜索，进阶 3 第 4 步）
                if inner.hnsw_enabled {
                    let mut ix =
                        HnswIndex::new(metric, HNSW_M, HNSW_EF_CONSTRUCTION, hnsw_seed(key))
                            .map_err(|e| {
                                AddVectorError::Internal(format!("HNSW 建图失败: {e}"))
                            })?;
                    ix.insert(&id, &vectors[&id])
                        .map_err(|e| AddVectorError::Internal(format!("HNSW 插入失败: {e}")))?;
                    inner.hnsw.insert(key.to_string(), ix);
                }
                inner.map.insert(
                    key.to_string(),
                    Value::VectorIndex { dim, metric, vectors, next_id: 1 },
                );
                Ok(id)
            }
            Some(Value::VectorIndex { dim: locked, vectors, next_id, metric: _ }) => {
                if dim != *locked {
                    return Err(AddVectorError::DimensionMismatch(*locked));
                }
                let id = next_id.to_string();
                *next_id += 1;
                vectors.insert(id.clone(), data.clone());
                // HNSW 维护仅在启用时进行；不变量（启用时 map ⟺ hnsw）违例如实上抛
                if inner.hnsw_enabled {
                    let Some(ix) = inner.hnsw.get_mut(key) else {
                        return Err(AddVectorError::Internal(
                            "HNSW 索引与存储状态不一致（不变量违例）".to_string(),
                        ));
                    };
                    ix.insert(&id, &data)
                        .map_err(|e| AddVectorError::Internal(format!("HNSW 插入失败: {e}")))?;
                }
                Ok(id)
            }
            Some(Value::Str(_)) => Err(AddVectorError::NotAnIndex),
        }
    }

    /// 读取索引内指定 id 的向量（仅克隆单条，绝非整个索引）。只读操作。
    pub fn get_vector(&self, key: &str, id: &str) -> Result<Option<Vec<f32>>, GetVectorError> {
        let inner = self.lock_inner();
        match inner.map.get(key) {
            Some(Value::VectorIndex { vectors, .. }) => Ok(vectors.get(id).cloned()),
            Some(Value::Str(_)) => Err(GetVectorError::NotAnIndex),
            None => Ok(None),
        }
    }

    /// 探测索引维度与 metric。两者均在创建时永久锁定，
    /// probe 与后续操作之间不存在维度/度量漂移。
    pub fn probe_index(&self, key: &str) -> IndexProbe {
        let inner = self.lock_inner();
        match inner.map.get(key) {
            Some(Value::VectorIndex { dim, metric, .. }) => IndexProbe::Index {
                dim: *dim,
                metric: *metric,
            },
            Some(Value::Str(_)) => IndexProbe::NotAnIndex,
            None => IndexProbe::Missing,
        }
    }

    /// 持锁遍历索引内全部向量，回调逐个收到 (id, 分量)；遍历顺序不保证。
    ///
    /// ★ 锁纪律（阶段 5 核心约束）：遍历与回调执行都在 Mutex 临界区内完成，
    ///   临界区 = 一次遍历；command/vector 层经此访问，绝不接触 VectorIndex
    ///   内部结构，也绝不把整个索引克隆出锁外。
    pub fn for_each_vector(&self, key: &str, mut f: impl FnMut(&str, &[f32])) -> VisitOutcome {
        let inner = self.lock_inner();
        match inner.map.get(key) {
            Some(Value::VectorIndex { vectors, .. }) => {
                for (id, data) in vectors.iter() {
                    f(id, data);
                }
                VisitOutcome::Traversed
            }
            Some(Value::Str(_)) => VisitOutcome::NotAnIndex,
            None => VisitOutcome::Missing,
        }
    }

    /// 统一的加锁入口：锁被毒化（持锁线程曾 panic）时继续取内部数据，
    /// 保证服务器不被单次故障拖垮。本实现持锁期间不会 panic，此为纵深防御。
    fn lock_inner(&self) -> MutexGuard<'_, DbInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// HNSW 搜索（进阶 3 第 4 步 VSEARCH 集成预留；本步未接入命令层）。
    ///
    /// ★ metric 匹配语义（第 4 步调用方遵守）：
    /// hnsw_search 只按**索引自己的 metric** 工作（建图时锁定）。
    /// 调用方必须先比对：VSEARCH 的 metric == VectorIndex 的 metric → 走 HNSW；
    /// 不等 → 走暴力搜索（保证正确性，绝不用错 metric 的图）。
    /// 本方法不接收 metric 参数——索引建图时已锁定。
    ///
    /// 返回：`None` = 该 key 无 HNSW 可用（key 不存在或不是向量索引）→ 调用方
    /// fallback；`Some(Ok(hits))` = HNSW 结果（升序）；`Some(Err)` = 维度不符等
    /// HNSW 内部错误。
    pub fn hnsw_search(
        &self,
        key: &str,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Option<Result<Vec<Hit>, String>> {
        let inner = self.lock_inner();
        // 未启用 HNSW：恒为 None，调用方 fallback 暴力（进阶 3 第 4 步）
        if !inner.hnsw_enabled {
            return None;
        }
        inner.hnsw.get(key).map(|ix| {
            ix.search(query, k, ef)
                .map(|hits| hits.into_iter().map(|(id, dist)| Hit { id, dist }).collect())
        })
    }

    /// 仅测试用：把 WAL 置为 Broken（模拟写入失败后的只读状态机）。
    #[cfg(test)]
    fn break_wal(&self) {
        self.lock_inner().wal = WalState::Broken;
    }
}

/// 恢复重放：直接修改内存表，不经 WAL（重放期间引擎为 Off，重放绝不写日志）。
///
/// VADD 与在线插入同语义：索引不存在则创建锁维，自增 id 确定性复现；
/// 重放数据与快照/既有内存不一致（维度不符、目标 key 是字符串）→ Corrupt。
fn apply_recovered(
    map: &mut HashMap<String, Value>,
    entry: WalEntry,
) -> Result<(), PersistError> {
    match entry {
        WalEntry::Set { key, value } => {
            map.insert(key, value);
        }
        WalEntry::Del { keys } => {
            for key in keys {
                map.remove(&key);
            }
        }
        WalEntry::FlushAll => {
            map.clear();
        }
        WalEntry::VAdd { key, data, metric } => {
            let dim = data.len();
            match map.get_mut(&key) {
                None => {
                    let id = "0".to_string();
                    let vectors = HashMap::from([(id, data)]);
                    map.insert(key, Value::VectorIndex { dim, metric, vectors, next_id: 1 });
                }
                Some(Value::VectorIndex { dim: locked, vectors, next_id, metric: locked_metric }) => {
                    if *locked_metric != metric {
                        return Err(PersistError::Corrupt(
                            "WAL 中 VADD 的 metric 与索引 metric 不符".to_string(),
                        ));
                    }
                    if dim != *locked {
                        return Err(PersistError::Corrupt(format!(
                            "WAL 中 VADD 维度 {dim} 与索引维度 {locked} 不符"
                        )));
                    }
                    let id = next_id.to_string();
                    *next_id += 1;
                    vectors.insert(id, data);
                }
                Some(Value::Str(_)) => {
                    return Err(PersistError::Corrupt(
                        "WAL 中 VADD 的目标 key 是字符串".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;

    use super::*;
    use crate::persist::PersistConfig;
    use crate::vector::distance::distance;

    /// 构造一个 VectorIndex 值（测试预置用；metric 取缺省 Cosine）。
    fn vec_index(dim: usize) -> Value {
        Value::VectorIndex {
            dim,
            metric: Metric::Cosine,
            vectors: HashMap::new(),
            next_id: 0,
        }
    }

    /// 唯一临时数据目录（测试辅助；幂等清理）。
    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("vredis-db-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn s01_set_get_str_roundtrip() {
        let db = Db::new();
        db.set("k", Value::Str(b"hello".to_vec())).expect("in-memory set");
        assert_eq!(db.get("k"), Some(Value::Str(b"hello".to_vec())));
    }

    #[test]
    fn s02_get_missing_is_none() {
        let db = Db::new();
        assert_eq!(db.get("nope"), None);
    }

    #[test]
    fn s03_set_overwrites_str_with_str() {
        let db = Db::new();
        db.set("k", Value::Str(b"v1".to_vec())).expect("in-memory set");
        db.set("k", Value::Str(b"v2".to_vec())).expect("in-memory set");
        assert_eq!(db.get("k"), Some(Value::Str(b"v2".to_vec())));
    }

    #[test]
    fn s04_set_overwrites_across_types() {
        // SET 覆盖任意类型：Str → VectorIndex → Str
        let db = Db::new();
        db.set("k", Value::Str(b"v".to_vec())).expect("in-memory set");
        db.set("k", vec_index(3)).expect("in-memory set");
        assert_eq!(db.get("k"), Some(vec_index(3)));
        db.set("k", Value::Str(b"back".to_vec())).expect("in-memory set");
        assert_eq!(db.get("k"), Some(Value::Str(b"back".to_vec())));
    }

    #[test]
    fn s05_del_counts_existing_only() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec())).expect("in-memory set");
        db.set("b", Value::Str(b"2".to_vec())).expect("in-memory set");
        assert_eq!(db.del(&["a".to_string(), "x".to_string()]).expect("in-memory del"), 1);
        assert_eq!(db.del(&["a".to_string()]).expect("in-memory del"), 0);
        assert_eq!(
            db.del(&["b".to_string(), "x".to_string(), "y".to_string()]).expect("in-memory del"),
            1
        );
    }

    #[test]
    fn s06_exists_counts_duplicates() {
        // 与 Redis EXISTS 一致：重复 key 重复计数
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec())).expect("in-memory set");
        assert_eq!(
            db.exists_any(&["a".to_string(), "a".to_string(), "x".to_string()]),
            2
        );
    }

    #[test]
    fn s07_keys_lists_all() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec())).expect("in-memory set");
        db.set("b", vec_index(2)).expect("in-memory set");
        let mut keys = db.keys();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn s08_flush_clears_everything() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec())).expect("in-memory set");
        db.set("b", vec_index(2)).expect("in-memory set");
        db.flush().expect("in-memory flush");
        assert!(db.keys().is_empty());
        assert_eq!(db.get("a"), None);
    }

    #[test]
    fn s09_concurrent_access_smoke() {
        // 8 线程 × 50 次写入并发冒烟：无 panic、无丢失（章程验收第 6 条的地基）
        let db = Arc::new(Db::new());
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..50usize {
                        db.set(format!("k{t}-{i}"), Value::Str(b"v".to_vec()))
                            .expect("concurrent set");
                        let _ = db.get(&format!("k{t}-{}", i.wrapping_sub(1)));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread must not panic");
        }
        assert_eq!(db.keys().len(), 8 * 50);
    }

    #[test]
    fn s10_vector_index_shape() {
        let v = Value::VectorIndex {
            dim: 3,
            metric: Metric::L2,
            vectors: [("alice".to_string(), vec![1.0, 0.0, 0.0])].into_iter().collect(),
            next_id: 0,
        };
        match &v {
            Value::VectorIndex { dim, vectors, .. } => {
                assert_eq!(*dim, 3);
                assert_eq!(vectors.len(), 1);
            }
            _ => panic!("expected VectorIndex"),
        }
    }

    // —— 向量 API（v0.3，v01–v08）——

    #[test]
    fn v01_vector_add_creates_index_and_assigns_ids() {
        let db = Db::new();
        assert_eq!(db.vector_add("ix", vec![1.0, 0.0, 0.0]), Ok("0".to_string()));
        assert_eq!(db.vector_add("ix", vec![0.0, 1.0, 0.0]), Ok("1".to_string()));
        assert_eq!(
            db.probe_index("ix"),
            IndexProbe::Index { dim: 3, metric: Metric::Cosine }
        );
    }

    #[test]
    fn v02_vector_add_locks_dimension() {
        let db = Db::new();
        db.vector_add("ix", vec![1.0, 0.0, 0.0]).expect("setup: add must succeed");
        // 维度锁定后：分量数不符 → Err(携带已锁定维度)；同维度继续可加
        assert_eq!(
            db.vector_add("ix", vec![1.0, 2.0]),
            Err(AddVectorError::DimensionMismatch(3))
        );
        assert_eq!(db.vector_add("ix", vec![0.0, 0.0, 1.0]), Ok("1".to_string()));
    }

    #[test]
    fn v03_vector_add_on_str_key_is_not_an_index() {
        let db = Db::new();
        db.set("s", Value::Str(b"text".to_vec())).expect("in-memory set");
        assert_eq!(db.vector_add("s", vec![1.0]), Err(AddVectorError::NotAnIndex));
    }

    #[test]
    fn v04_get_vector_states() {
        let db = Db::new();
        db.vector_add("ix", vec![1.0, 2.0, 3.0]).expect("setup: add must succeed");
        assert_eq!(db.get_vector("ix", "0"), Ok(Some(vec![1.0, 2.0, 3.0])));
        assert_eq!(db.get_vector("ix", "99"), Ok(None)); // id 不存在
        assert_eq!(db.get_vector("nope", "0"), Ok(None)); // 索引不存在
        db.set("s", Value::Str(b"x".to_vec())).expect("in-memory set");
        assert_eq!(db.get_vector("s", "0"), Err(GetVectorError::NotAnIndex));
    }

    #[test]
    fn v05_probe_index_states() {
        let db = Db::new();
        assert_eq!(db.probe_index("nope"), IndexProbe::Missing);
        db.vector_add("ix", vec![1.0, 0.0]).expect("setup: add must succeed");
        assert_eq!(
            db.probe_index("ix"),
            IndexProbe::Index { dim: 2, metric: Metric::Cosine }
        );
        db.set("s", Value::Str(b"x".to_vec())).expect("in-memory set");
        assert_eq!(db.probe_index("s"), IndexProbe::NotAnIndex);
    }

    #[test]
    fn v06_for_each_vector_visits_all() {
        let db = Db::new();
        db.vector_add("ix", vec![1.0, 0.0]).expect("setup: add must succeed");
        db.vector_add("ix", vec![0.0, 1.0]).expect("setup: add must succeed");
        db.vector_add("ix", vec![1.0, 1.0]).expect("setup: add must succeed");
        // 遍历顺序不保证：收集后按 id 排序断言
        let mut visited: Vec<(String, usize)> = Vec::new();
        assert_eq!(
            db.for_each_vector("ix", |id, data| visited.push((id.to_string(), data.len()))),
            VisitOutcome::Traversed
        );
        visited.sort();
        assert_eq!(
            visited,
            vec![("0".to_string(), 2), ("1".to_string(), 2), ("2".to_string(), 2)]
        );
        assert_eq!(db.for_each_vector("nope", |_, _| ()), VisitOutcome::Missing);
        db.set("s", Value::Str(b"x".to_vec())).expect("in-memory set");
        assert_eq!(db.for_each_vector("s", |_, _| ()), VisitOutcome::NotAnIndex);
    }

    #[test]
    fn v07_auto_id_monotonic_sequence() {
        let db = Db::new();
        for i in 0..5u64 {
            assert_eq!(db.vector_add("ix", vec![i as f32]), Ok(i.to_string()));
        }
    }

    #[test]
    fn v08_concurrent_vector_add_unique_ids() {
        // 4 线程 × 100 次 VADD 并发：id 全局唯一、总数正确（vector_add 原子性冒烟）
        let db = Arc::new(Db::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..100 {
                        db.vector_add("ix", vec![1.0]).expect("add must succeed");
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker must not panic");
        }
        let mut ids = Vec::new();
        db.for_each_vector("ix", |id, _| ids.push(id.to_string()));
        assert_eq!(ids.len(), 400);
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 400, "ids must be unique under concurrency");
    }

    // —— 持久化恢复（v0.4，p11–p18；p10 已被快照测试占用）——

    #[test]
    fn p11_open_empty_dir_fresh_start() {
        let dir = temp_dir("empty");
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
        assert!(warnings.is_empty());
        // 全新启动即可正常读写，且 WAL 文件已创建（Active）
        db.set("a", Value::Str(b"1".to_vec())).expect("set");
        assert_eq!(db.get("a"), Some(Value::Str(b"1".to_vec())));
        assert!(dir.join("wal.log").exists());
    }

    #[test]
    fn p12_recover_full_state_and_next_id() {
        let dir = temp_dir("recover");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("a", Value::Str(b"1".to_vec())).expect("set a");
            db.set("b", Value::Str(b"2".to_vec())).expect("set b");
            assert_eq!(db.vector_add("ix", vec![1.0, 0.0]).expect("add"), "0");
            assert_eq!(db.vector_add("ix", vec![0.0, 1.0]).expect("add"), "1");
            db.del(&["a".to_string()]).expect("del a");
        } // drop：释放 WAL 句柄，模拟进程退出
        // “重启”：同一目录重新打开（main 的启动路径）
        let (db, warnings) = Db::open(&PersistConfig { dir }).expect("reopen");
        assert!(warnings.is_empty());
        assert_eq!(db.get("a"), None); // 删除已重放
        assert_eq!(db.get("b"), Some(Value::Str(b"2".to_vec())));
        assert_eq!(
            db.probe_index("ix"),
            IndexProbe::Index { dim: 2, metric: Metric::Cosine }
        );
        assert_eq!(db.get_vector("ix", "0"), Ok(Some(vec![1.0, 0.0])));
        assert_eq!(db.get_vector("ix", "1"), Ok(Some(vec![0.0, 1.0])));
        // next_id 续号：重开后新 VADD 拿 "2"
        assert_eq!(db.vector_add("ix", vec![1.0, 1.0]).expect("add"), "2");
    }

    #[test]
    fn p13_double_restart() {
        let dir = temp_dir("double");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("k", Value::Str(b"v".to_vec())).expect("set");
        }
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("reopen 1");
            assert_eq!(db.get("k"), Some(Value::Str(b"v".to_vec())));
        }
        let (db, _) = Db::open(&PersistConfig { dir }).expect("reopen 2");
        assert_eq!(db.get("k"), Some(Value::Str(b"v".to_vec())));
    }

    #[test]
    fn p14_bgsave_truncates_wal_and_recovers_snapshot_plus_wal() {
        let dir = temp_dir("bgsave");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("a", Value::Str(b"1".to_vec())).expect("set a");
            db.bgsave().expect("bgsave");
            // BGSAVE 成功后 WAL 被截断为空（快照已包含全部数据，design.md D4）
            assert_eq!(
                std::fs::metadata(dir.join("wal.log")).expect("meta").len(),
                0
            );
            db.set("b", Value::Str(b"2".to_vec())).expect("set b"); // b 只在 WAL
        }
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("reopen");
        assert!(warnings.is_empty());
        assert_eq!(db.get("a"), Some(Value::Str(b"1".to_vec()))); // 来自快照
        assert_eq!(db.get("b"), Some(Value::Str(b"2".to_vec()))); // 来自 WAL 重放
    }

    #[test]
    fn p15_wal_corrupt_middle_is_error() {
        let dir = temp_dir("walcorrupt");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("a", Value::Str(b"1".to_vec())).expect("set a");
            db.set("b", Value::Str(b"2".to_vec())).expect("set b");
        }
        // 在 WAL 末尾追加一段“头完整但 magic 为 0”的字节 → 真损坏（非撕裂尾）
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("wal.log"))
            .expect("append open");
        file.write_all(&[0u8; 20]).expect("append garbage"); // 20 字节 ≥ 12B 头
        drop(file);
        assert!(matches!(
            Db::open(&PersistConfig { dir }),
            Err(PersistError::Corrupt(_))
        ));
    }

    #[test]
    fn p16_snapshot_corrupt_is_error() {
        let dir = temp_dir("snapcorrupt");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("a", Value::Str(b"1".to_vec())).expect("set");
            db.bgsave().expect("bgsave");
        }
        // 翻转快照条目区一个字节 → 尾校验不符 → 真损坏
        let snap = dir.join("snapshot.vrdb");
        let mut data = std::fs::read(&snap).expect("read snapshot");
        let last = data.len() - 6;
        data[last] ^= 0xFF;
        std::fs::write(&snap, &data).expect("write snapshot");
        assert!(matches!(
            Db::open(&PersistConfig { dir }),
            Err(PersistError::Corrupt(_))
        ));
    }

    #[test]
    fn p17_crash_mid_wal_write_torn_tail() {
        let dir = temp_dir("torn");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            db.set("a", Value::Str(b"keep".to_vec())).expect("set a");
        }
        // 模拟崩溃在写入中途：补“半条记录”——合法 magic + 声明 100 字节 payload，
        // 但 payload 字节不存在（文件在此结束）
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("wal.log"))
            .expect("append open");
        let mut partial = Vec::new();
        partial.extend_from_slice(&0x56_52_4C_45u32.to_le_bytes()); // "VRLE"
        partial.extend_from_slice(&100u32.to_le_bytes()); // 声明 100 字节 payload
        partial.extend_from_slice(&0u32.to_le_bytes()); // 校验和占位（不会被读到）
        file.write_all(&partial).expect("append partial");
        drop(file);
        // 撕裂尾不是错误：截断 + 明确警告（不静默）
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("截断"));
        // 前序操作完好
        assert_eq!(db.get("a"), Some(Value::Str(b"keep".to_vec())));
        // 且之后可正常写入
        db.set("b", Value::Str(b"2".to_vec())).expect("set after recovery");
        assert_eq!(db.get("b"), Some(Value::Str(b"2".to_vec())));
    }

    #[test]
    fn p18_broken_readonly_bgsave_heals_and_off_bgsave_noop() {
        // （a）Broken 状态机：WAL 置 Broken 后所有写立即失败（内存未动）、读不受影响，
        //     BGSAVE 成功后自愈（design.md D11）
        let dir = temp_dir("broken");
        let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
        db.break_wal();
        assert!(db.set("a", Value::Str(b"1".to_vec())).is_err());
        assert_eq!(db.get("a"), None); // 内存未动
        assert!(db.del(&["a".to_string()]).is_err());
        assert!(db.bgsave().is_ok(), "bgsave heals broken wal");
        db.set("a", Value::Str(b"1".to_vec())).expect("set after heal");
        assert_eq!(db.get("a"), Some(Value::Str(b"1".to_vec())));
        // （b）纯内存（Off）模式：BGSAVE 是 no-op，写路径照常
        let mem = Db::new();
        assert!(mem.bgsave().is_ok());
        mem.set("a", Value::Str(b"1".to_vec())).expect("in-memory set");
        assert_eq!(mem.get("a"), Some(Value::Str(b"1".to_vec())));
    }

    #[test]
    fn p19_replay_vadd_is_not_idempotent() {
        // 已知边界（design.md §8「已知限制 v0.4」，非 bug）：
        // BGSAVE 若“快照保存成功但 WAL 截断失败”，重启后快照 + WAL 重放会把
        // 同一条 VADD 插入两次（SET/DEL/FLUSHALL 幂等，无影响）。
        // 本测试固化该行为；修复需引入 WAL epoch（列入未来方向）。
        let dir = temp_dir("vadd-dup");
        {
            let (db, _) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open");
            assert_eq!(db.vector_add("ix", vec![1.0]).expect("add"), "0");
            db.bgsave().expect("bgsave"); // 快照已含 id "0"，WAL 被截断为空
            // 模拟“快照成功但 WAL 截断失败”：把同一条 VADD 手工写回 WAL
            let mut writer = WalWriter::open(&dir.join("wal.log")).expect("open wal");
            writer
                .append(&WalEntry::VAdd {
                    key: "ix".into(),
                    data: vec![1.0],
                    metric: Metric::Cosine,
                })
                .expect("append");
            drop(writer);
        }
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("reopen");
        assert!(warnings.is_empty());
        // 重放把同一条 VADD 再插一次：索引内出现 2 条向量
        //（id "0" 来自快照，重放走正常插入路径拿到 id "1"，内容重复）
        let mut ids = Vec::new();
        db.for_each_vector("ix", |id, _| ids.push(id.to_string()));
        ids.sort();
        assert_eq!(ids, vec!["0".to_string(), "1".to_string()]);
    }

    #[test]
    fn v09_hnsw_disabled_falls_back() {
        // 默认（未启用 HNSW）：写路径完全跳过索引，hnsw_search 恒为 None
        //（第 4 步起调用方据此 fallback 暴力搜索）
        let (db, _) = Db::open(&PersistConfig { dir: temp_dir("v09") }).expect("open");
        db.vector_add("ix", vec![1.0, 0.0]).expect("add");
        assert!(db.hnsw_search("ix", &[1.0, 0.0], 1, 32).is_none());
        db.vector_add_with_metric("ix2", vec![1.0], Metric::L2).expect("add");
        assert!(db.hnsw_search("ix2", &[1.0], 1, 32).is_none());
    }

    // —— HNSW 与持久化协同（进阶 3 第 3 步，h20–h26；open_with 启用 HNSW）——

    #[test]
    fn h20_hnsw_available_after_recovery() {
        let dir = temp_dir("h20");
        {
            let (db, _) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("open");
            db.vector_add("ix", vec![1.0, 0.0]).expect("add");
            db.vector_add("ix", vec![0.0, 1.0]).expect("add");
        } // drop：模拟进程退出
        let (db, warnings) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("reopen");
        assert!(warnings.is_empty());
        // 恢复后 HNSW 可用：查询 = 存储向量 → top-1 对应 id、dist 0
        let hits = db
            .hnsw_search("ix", &[1.0, 0.0], 1, 32)
            .expect("hnsw must exist")
            .expect("search");
        assert_eq!(hits[0].id, "0");
        assert!(hits[0].dist.abs() < 1e-12);
    }

    #[test]
    fn h21_hnsw_immediately_contains_new_vector() {
        let (db, _) = Db::open_with(&PersistConfig { dir: temp_dir("h21") }, true).expect("open");
        assert_eq!(db.vector_add("ix", vec![1.0, 0.0]).expect("add"), "0");
        // 不重启：VADD 后 HNSW 立即可查
        let hits = db
            .hnsw_search("ix", &[1.0, 0.0], 1, 32)
            .expect("hnsw must exist")
            .expect("search");
        assert_eq!(hits[0].id, "0");
        assert!(hits[0].dist.abs() < 1e-12);
    }

    #[test]
    fn h22_set_over_index_removes_hnsw() {
        let (db, _) = Db::open_with(&PersistConfig { dir: temp_dir("h22") }, true).expect("open");
        db.vector_add("ix", vec![1.0, 0.0]).expect("add");
        assert!(db.hnsw_search("ix", &[1.0, 0.0], 1, 32).is_some());
        db.set("ix", Value::Str(b"now-string".to_vec())).expect("set");
        // SET 覆盖后 HNSW 作废 → None（第 4 步 fallback 语义）
        assert!(db.hnsw_search("ix", &[1.0, 0.0], 1, 32).is_none());
        assert_eq!(db.get("ix"), Some(Value::Str(b"now-string".to_vec())));
    }

    #[test]
    fn h23_del_and_flush_clear_hnsw() {
        let (db, _) = Db::open_with(&PersistConfig { dir: temp_dir("h23") }, true).expect("open");
        // DEL 段
        db.vector_add("ix", vec![1.0, 0.0]).expect("add");
        db.del(&["ix".to_string()]).expect("del");
        assert!(db.hnsw_search("ix", &[1.0, 0.0], 1, 32).is_none());
        // FLUSHALL 段
        db.vector_add("ix2", vec![1.0, 0.0]).expect("add");
        db.flush().expect("flush");
        assert!(db.hnsw_search("ix2", &[1.0, 0.0], 1, 32).is_none());
    }

    #[test]
    fn h24_dimension_lock_consistent_with_hnsw() {
        let (db, _) = Db::open_with(&PersistConfig { dir: temp_dir("h24") }, true).expect("open");
        db.vector_add("ix", vec![1.0, 0.0]).expect("add");
        // 维度锁定：3 维插入失败
        assert!(matches!(
            db.vector_add("ix", vec![1.0, 0.0, 0.0]),
            Err(AddVectorError::DimensionMismatch(2))
        ));
        // HNSW 不受失败插入影响：2 维查询照常
        let hits = db
            .hnsw_search("ix", &[1.0, 0.0], 1, 32)
            .expect("hnsw must exist")
            .expect("search");
        assert_eq!(hits[0].id, "0");
        // 3 维查询 → HNSW 维度校验 Some(Err)
        assert!(matches!(
            db.hnsw_search("ix", &[1.0, 0.0, 0.0], 1, 32),
            Some(Err(_))
        ));
    }

    #[test]
    fn h25_metric_param_and_default() {
        let (db, _) = Db::open_with(&PersistConfig { dir: temp_dir("h25") }, true).expect("open");
        // 缺省 vector_add → Cosine
        db.vector_add("dflt", vec![1.0]).expect("add");
        match db.get("dflt") {
            Some(Value::VectorIndex { metric, .. }) => assert_eq!(metric, Metric::Cosine),
            other => panic!("expected vector index, got {other:?}"),
        }
        // vector_add_with_metric → 指定 metric
        db.vector_add_with_metric("l2ix", vec![1.0], Metric::L2).expect("add");
        match db.get("l2ix") {
            Some(Value::VectorIndex { metric, .. }) => assert_eq!(metric, Metric::L2),
            other => panic!("expected vector index, got {other:?}"),
        }
    }

    #[test]
    fn h26_metric_persisted_and_hnsw_semantics() {
        // 数据设计：A=[1,0], B=[10,0], C=[0.5,0.5]，Q=[1,0]
        // cos top-2 = {A, B}（同向距离 0）；l2 top-2 = {A, C}（0 与 0.707）——排序不同，
        // 证明 HNSW 真的按索引锁定的 metric 工作
        let dir = temp_dir("h26");
        let a = vec![1.0, 0.0];
        let b = vec![10.0, 0.0];
        let c = vec![0.5, 0.5];
        {
            let (db, _) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("open");
            assert_eq!(
                db.vector_add_with_metric("ix", a.clone(), Metric::L2).expect("add"),
                "0"
            );
            assert_eq!(
                db.vector_add_with_metric("ix", b.clone(), Metric::L2).expect("add"),
                "1"
            );
            assert_eq!(
                db.vector_add_with_metric("ix", c.clone(), Metric::L2).expect("add"),
                "2"
            );
            db.bgsave().expect("bgsave");
        }
        let (db, _) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("reopen");
        // metric 经快照持久化保持 L2
        match db.get("ix") {
            Some(Value::VectorIndex { metric, .. }) => assert_eq!(metric, Metric::L2),
            other => panic!("expected vector index, got {other:?}"),
        }
        // l2 语义 top-2 = A, C（id "0","2"）；ef=64 ≥ 3 ⇒ 精确
        let hits = db
            .hnsw_search("ix", &[1.0, 0.0], 2, 64)
            .expect("hnsw must exist")
            .expect("search");
        let got: Vec<String> = hits.into_iter().map(|h| h.id).collect();
        assert_eq!(got, vec!["0".to_string(), "2".to_string()]);
        // 对照：同数据 cos 暴力 top-2 = A, B（id "0","1"）——两种 metric 排序确实不同
        let points = [("0".to_string(), a), ("1".to_string(), b), ("2".to_string(), c)];
        let mut cos_scored: Vec<(f64, &str)> = points
            .iter()
            .map(|(id, v)| (distance(Metric::Cosine, &[1.0, 0.0], v), id.as_str()))
            .collect();
        cos_scored.sort_by(|x, y| x.0.total_cmp(&y.0));
        let cos_top2: Vec<String> =
            cos_scored.into_iter().take(2).map(|(_, id)| id.to_string()).collect();
        assert_eq!(cos_top2, vec!["0".to_string(), "1".to_string()]);
        assert_ne!(
            cos_top2,
            vec!["0".to_string(), "2".to_string()],
            "test data must discriminate l2 vs cos ordering"
        );
    }
}
