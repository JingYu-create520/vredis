//! 存储层：key → Value 的内存哈希表；进程内唯一，Arc 跨线程共享（docs/design.md §3）。
//!
//! 并发策略（design.md D2）：单把 Mutex 大锁，MVP 以正确性与简单为先；
//! 所有操作都是内存级临界区，足够快。锁毒化采用纵深防御（见 `lock` 辅助函数）。

mod value;

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

pub use value::Value;

/// VADD 的错误（命令层映射：NotAnIndex → WRONGTYPE；DimensionMismatch → invalid vector dimension）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddVectorError {
    /// key 存的是字符串（非向量索引）
    NotAnIndex,
    /// 索引维度已锁定，与本次分量数不符（携带已锁定维度）
    DimensionMismatch(usize),
}

/// VGET 的错误（key 是字符串，非索引）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetVectorError {
    NotAnIndex,
}

/// 索引探测结果（VDIM / VSEARCH 前置校验用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexProbe {
    /// 索引存在，维度为创建时锁定值
    Dim(usize),
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

/// 数据库本体：key → Value。
#[derive(Default)]
pub struct Db {
    map: Mutex<HashMap<String, Value>>,
}

impl Db {
    pub fn new() -> Self {
        Self::default()
    }

    /// 写入 / 覆盖 key（覆盖任意已存在类型，与 Redis SET 语义一致）。
    pub fn set(&self, key: impl Into<String>, value: Value) {
        lock(&self.map).insert(key.into(), value);
    }

    /// 快照式读取：存在则克隆返回。MVP 值都很小，克隆换取锁使用的简单性。
    pub fn get(&self, key: &str) -> Option<Value> {
        lock(&self.map).get(key).cloned()
    }

    /// 删除一组 key，返回实际删除的数量。
    pub fn del(&self, keys: &[String]) -> usize {
        let mut map = lock(&self.map);
        keys.iter().filter(|k| map.remove(k.as_str()).is_some()).count()
    }

    /// 统计存在的 key 数量；重复 key 重复计数（与 Redis EXISTS 一致）。
    pub fn exists_any(&self, keys: &[String]) -> usize {
        let map = lock(&self.map);
        keys.iter().filter(|k| map.contains_key(k.as_str())).count()
    }

    /// 返回当前全部 key（顺序不保证）。
    pub fn keys(&self) -> Vec<String> {
        lock(&self.map).keys().cloned().collect()
    }

    /// 清空全部数据。
    pub fn flush(&self) {
        lock(&self.map).clear();
    }

    // ── 向量 API（v0.3）：全部单次持锁，禁止把索引克隆出锁外 ──

    /// 向索引添加向量并返回自动生成的 id（索引内自增十进制串，从 "0" 起）。
    /// 索引不存在则创建并锁定维度（dim = data.len()）。
    ///
    /// ★ 原子性：维度检查 + id 生成 + 插入 + 计数器递增在一次持锁内完成，
    ///   并发 VADD 不会产生重复 id 或漏计数。
    pub fn vector_add(&self, key: &str, data: Vec<f32>) -> Result<String, AddVectorError> {
        let dim = data.len();
        let mut map = lock(&self.map);
        match map.get_mut(key) {
            None => {
                let id = "0".to_string();
                let vectors = HashMap::from([(id.clone(), data)]);
                map.insert(
                    key.to_string(),
                    Value::VectorIndex { dim, vectors, next_id: 1 },
                );
                Ok(id)
            }
            Some(Value::VectorIndex { dim: locked, vectors, next_id }) => {
                if dim != *locked {
                    return Err(AddVectorError::DimensionMismatch(*locked));
                }
                let id = next_id.to_string();
                *next_id += 1;
                vectors.insert(id.clone(), data);
                Ok(id)
            }
            Some(Value::Str(_)) => Err(AddVectorError::NotAnIndex),
        }
    }

    /// 读取索引内指定 id 的向量（仅克隆单条，绝非整个索引）。
    pub fn get_vector(&self, key: &str, id: &str) -> Result<Option<Vec<f32>>, GetVectorError> {
        match lock(&self.map).get(key) {
            Some(Value::VectorIndex { vectors, .. }) => Ok(vectors.get(id).cloned()),
            Some(Value::Str(_)) => Err(GetVectorError::NotAnIndex),
            None => Ok(None),
        }
    }

    /// 探测索引维度。维度创建即永久锁定，probe 与后续遍历之间不存在维度漂移。
    pub fn probe_index(&self, key: &str) -> IndexProbe {
        match lock(&self.map).get(key) {
            Some(Value::VectorIndex { dim, .. }) => IndexProbe::Dim(*dim),
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
        match lock(&self.map).get(key) {
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
}

/// 统一的加锁入口：锁被毒化（持锁线程曾 panic）时继续取内部数据，
/// 保证服务器不被单次故障拖垮。本实现持锁期间不会 panic，此为纵深防御。
fn lock(map: &Mutex<HashMap<String, Value>>) -> MutexGuard<'_, HashMap<String, Value>> {
    map.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// 构造一个 VectorIndex 值（测试预置用）。
    fn vec_index(dim: usize) -> Value {
        Value::VectorIndex { dim, vectors: HashMap::new(), next_id: 0 }
    }

    #[test]
    fn s01_set_get_str_roundtrip() {
        let db = Db::new();
        db.set("k", Value::Str(b"hello".to_vec()));
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
        db.set("k", Value::Str(b"v1".to_vec()));
        db.set("k", Value::Str(b"v2".to_vec()));
        assert_eq!(db.get("k"), Some(Value::Str(b"v2".to_vec())));
    }

    #[test]
    fn s04_set_overwrites_across_types() {
        // SET 覆盖任意类型：Str → VectorIndex → Str
        let db = Db::new();
        db.set("k", Value::Str(b"v".to_vec()));
        db.set("k", vec_index(3));
        assert_eq!(db.get("k"), Some(vec_index(3)));
        db.set("k", Value::Str(b"back".to_vec()));
        assert_eq!(db.get("k"), Some(Value::Str(b"back".to_vec())));
    }

    #[test]
    fn s05_del_counts_existing_only() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec()));
        db.set("b", Value::Str(b"2".to_vec()));
        assert_eq!(db.del(&["a".to_string(), "x".to_string()]), 1);
        assert_eq!(db.del(&["a".to_string()]), 0);
        assert_eq!(db.del(&["b".to_string(), "x".to_string(), "y".to_string()]), 1);
    }

    #[test]
    fn s06_exists_counts_duplicates() {
        // 与 Redis EXISTS 一致：重复 key 重复计数
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec()));
        assert_eq!(
            db.exists_any(&["a".to_string(), "a".to_string(), "x".to_string()]),
            2
        );
    }

    #[test]
    fn s07_keys_lists_all() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec()));
        db.set("b", vec_index(2));
        let mut keys = db.keys();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn s08_flush_clears_everything() {
        let db = Db::new();
        db.set("a", Value::Str(b"1".to_vec()));
        db.set("b", vec_index(2));
        db.flush();
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
                        db.set(format!("k{t}-{i}"), Value::Str(b"v".to_vec()));
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
        assert_eq!(db.probe_index("ix"), IndexProbe::Dim(3));
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
        db.set("s", Value::Str(b"text".to_vec()));
        assert_eq!(db.vector_add("s", vec![1.0]), Err(AddVectorError::NotAnIndex));
    }

    #[test]
    fn v04_get_vector_states() {
        let db = Db::new();
        db.vector_add("ix", vec![1.0, 2.0, 3.0]).expect("setup: add must succeed");
        assert_eq!(db.get_vector("ix", "0"), Ok(Some(vec![1.0, 2.0, 3.0])));
        assert_eq!(db.get_vector("ix", "99"), Ok(None));       // id 不存在
        assert_eq!(db.get_vector("nope", "0"), Ok(None));      // 索引不存在
        db.set("s", Value::Str(b"x".to_vec()));
        assert_eq!(db.get_vector("s", "0"), Err(GetVectorError::NotAnIndex));
    }

    #[test]
    fn v05_probe_index_states() {
        let db = Db::new();
        assert_eq!(db.probe_index("nope"), IndexProbe::Missing);
        db.vector_add("ix", vec![1.0, 0.0]).expect("setup: add must succeed");
        assert_eq!(db.probe_index("ix"), IndexProbe::Dim(2));
        db.set("s", Value::Str(b"x".to_vec()));
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
        db.set("s", Value::Str(b"x".to_vec()));
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
}
