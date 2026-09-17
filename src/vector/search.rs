//! 暴力 top-k 搜索（MVP 无 HNSW，docs/design.md §4.2）。
//!
//! ★ 锁纪律（阶段 5 核心约束）：距离计算与 top-k 收集全部发生在
//! [`crate::storage::Db::for_each_vector`] 的单次持锁回调内；
//! 本模块绝不调用会克隆整个索引的 `db.get()`。

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::distance::{distance, Metric};
use crate::storage::{Db, IndexProbe, VisitOutcome};

/// 单条搜索结果：向量 id + 距离（越小越相似）。
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub id: String,
    pub dist: f64,
}

/// 搜索失败原因（命令层映射为 RESP2 回复）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    /// 索引不存在 → 命令层回复 nil
    IndexMissing,
    /// key 是字符串 → 命令层回复 WRONGTYPE
    WrongType,
    /// 查询维度 ≠ 索引维度（携带索引已锁定维度）
    DimensionMismatch(usize),
}

/// f64 的全序包装：`BinaryHeap` 需要 `Ord`，而 f64 只有 `PartialOrd`。
/// 用 `total_cmp` 给 NaN 一个确定性全序（距离计算不应产生 NaN，防御性处理）。
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdDist(f64);
impl Eq for OrdDist {}
impl Ord for OrdDist {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl PartialOrd for OrdDist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 大小为 k 的大顶堆：堆顶始终是当前 top-k 里最差的距离。
struct TopK {
    k: usize,
    heap: BinaryHeap<(OrdDist, String)>,
}

impl TopK {
    fn new(k: usize) -> Self {
        Self { k, heap: BinaryHeap::new() }
    }

    fn offer(&mut self, id: &str, dist: f64) {
        // 堆满时先与堆顶比较再分配，避免为注定淘汰的向量分配 String
        if self.heap.len() < self.k {
            self.heap.push((OrdDist(dist), id.to_string()));
        } else if let Some(top) = self.heap.peek() {
            if OrdDist(dist) < top.0 {
                self.heap.pop();
                self.heap.push((OrdDist(dist), id.to_string()));
            }
        }
    }

    /// 按距离升序输出；相等距离之间的顺序不保证（与 Redis 语义一致）。
    fn into_sorted(self) -> Vec<Hit> {
        let mut hits: Vec<Hit> =
            self.heap.into_iter().map(|(d, id)| Hit { id, dist: d.0 }).collect();
        hits.sort_by(|a, b| a.dist.total_cmp(&b.dist));
        hits
    }
}

/// 在指定索引内暴力搜索 top-k。
///
/// 流程：probe 前置校验（短临界区）→ 遍历 + top-k 收集（单次持锁，回调内完成）。
/// probe 与遍历之间索引被删除的竞态 → 按 [`SearchError::IndexMissing`] 处理；
/// 维度创建即永久锁定，两次临界区之间不存在维度漂移。
pub fn search(
    db: &Db,
    index: &str,
    query: &[f32],
    k: usize,
    metric: Metric,
) -> Result<Vec<Hit>, SearchError> {
    debug_assert!(k >= 1, "k >= 1 is validated at the command layer");
    // 前置校验：存在性 + 维度（短临界区）
    match db.probe_index(index) {
        IndexProbe::Missing => return Err(SearchError::IndexMissing),
        IndexProbe::NotAnIndex => return Err(SearchError::WrongType),
        IndexProbe::Dim(dim) if dim == query.len() => {}
        IndexProbe::Dim(dim) => return Err(SearchError::DimensionMismatch(dim)),
    }
    // 遍历 + top-k 收集：闭包在 for_each_vector 的持锁临界区内执行
    let mut topk = TopK::new(k);
    let outcome = db.for_each_vector(index, |id, data| {
        topk.offer(id, distance(metric, query, data));
    });
    match outcome {
        VisitOutcome::Traversed => Ok(topk.into_sorted()),
        VisitOutcome::Missing => Err(SearchError::IndexMissing),
        VisitOutcome::NotAnIndex => Err(SearchError::WrongType),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Db;

    /// 构造 3 维测试索引：id "0"=[1,0,0]，"1"=[0,1,0]，"2"=[1,1,0]。
    fn db_with_3d() -> Db {
        let db = Db::new();
        db.vector_add("ix", vec![1.0, 0.0, 0.0]).expect("setup");
        db.vector_add("ix", vec![0.0, 1.0, 0.0]).expect("setup");
        db.vector_add("ix", vec![1.0, 1.0, 0.0]).expect("setup");
        db
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "expected {expected}, got {actual}");
    }

    // —— 暴力搜索（sr01–sr05）——

    #[test]
    fn sr01_cosine_topk_ascending() {
        let db = db_with_3d();
        // 查询 [1,0,0]：cos 距离 id0=0，id2=1−1/√2≈0.2929，id1=1
        let hits = search(&db, "ix", &[1.0, 0.0, 0.0], 2, Metric::Cosine).expect("search");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "0");
        assert_close(hits[0].dist, 0.0);
        assert_eq!(hits[1].id, "2");
        assert_close(hits[1].dist, 1.0 - 1.0 / 2.0f64.sqrt());
    }

    #[test]
    fn sr02_k_larger_than_count_returns_all() {
        let db = db_with_3d();
        let hits = search(&db, "ix", &[1.0, 0.0, 0.0], 10, Metric::Cosine).expect("search");
        assert_eq!(hits.len(), 3);
        // 升序：0 ≤ 0.2929 ≤ 1
        assert!(hits[0].dist <= hits[1].dist && hits[1].dist <= hits[2].dist);
    }

    #[test]
    fn sr03_l2_and_dot_orderings() {
        let db = Db::new();
        db.vector_add("m", vec![1.0, 0.0]).expect("setup");
        db.vector_add("m", vec![2.0, 0.0]).expect("setup");
        // l2 到 [1,0]：id0=0，id1=1 → 升序 [0, 1]
        let l2 = search(&db, "m", &[1.0, 0.0], 2, Metric::L2).expect("search");
        assert_eq!(&[l2[0].id.as_str(), l2[1].id.as_str()], &["0", "1"]);
        // dot：id0 距离 −1，id1 距离 −2 → 升序 [1, 0]（内积大的更近）
        let dot = search(&db, "m", &[1.0, 0.0], 2, Metric::Dot).expect("search");
        assert_eq!(&[dot[0].id.as_str(), dot[1].id.as_str()], &["1", "0"]);
    }

    #[test]
    fn sr04_error_states() {
        let db = db_with_3d();
        assert_eq!(search(&db, "nope", &[1.0, 0.0, 0.0], 1, Metric::Cosine), Err(SearchError::IndexMissing));
        db.set("s", crate::storage::Value::Str(b"x".to_vec()));
        assert_eq!(search(&db, "s", &[1.0, 0.0, 0.0], 1, Metric::Cosine), Err(SearchError::WrongType));
        assert_eq!(
            search(&db, "ix", &[1.0, 0.0], 1, Metric::Cosine),
            Err(SearchError::DimensionMismatch(3))
        );
    }

    #[test]
    fn sr05_heap_eviction_picks_smallest_k() {
        // 20 个 1 维向量，分量 = id 序号；查询 [0]（l2 距离 = 分量值）
        // top-3 必须是 id "0","1","2" —— 验证堆淘汰路径
        let db = Db::new();
        for i in 0..20usize {
            db.vector_add("n", vec![i as f32]).expect("setup");
        }
        let hits = search(&db, "n", &[0.0], 3, Metric::L2).expect("search");
        assert_eq!(
            hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
            vec!["0", "1", "2"]
        );
    }
}
