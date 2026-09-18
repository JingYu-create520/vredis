//! 自研 HNSW 分层图近似最近邻索引（进阶 3，Malkov & Yashunin 2016）。
//!
//! ## 层的作用
//! - **层 0**：包含全部向量——保证可达性与搜索精度，边数上限 `m_max0 = 2m`；
//! - **层 l > 0**：指数递减的「高速公路」子集（`P(level ≥ l) = m^(-l)`）——
//!   贪心下降时在高层大步跳到目标区域，搜索复杂度 O(log N)。
//!
//! ## 状态（进阶 3 第 1 步）
//! 独立模块：未接入 VSEARCH（第 4 步），暴力搜索 `search.rs` 未改动。
//!
//! ## MVP 取舍（写入测试与集成文档）
//! - **自持向量副本**：向量在 Db 与索引各存一份，空间换实现简单；
//! - **邻居超限裁剪用简单距离截断**（保留最近 m_max 个）：论文的
//!   SELECT-NEIGHBORS-HEURISTIC（算法 4）列为未来优化；
//! - **OrdDist 与 search.rs 各持一份**：「不动 search.rs」约束下的小段重复；
//! - **SplitMix64**：自研 ~8 行 PRNG（非密码学安全，仅用于层数抽样），零依赖。

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use super::distance::{distance, Metric};

/// f64 全序包装：堆需要 `Ord`，而 f64 只有 `PartialOrd`。
/// `total_cmp` 给 NaN 确定性全序（距离不应产生 NaN，防御性处理）。
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdDist(f64);
impl Eq for OrdDist {}
impl Ord for OrdDist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl PartialOrd for OrdDist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 自研 SplitMix64（零依赖 PRNG，仅用于层数抽样；非密码学安全）。
#[derive(Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// 一个节点的邻居表：`neighbors[l]` = 第 l 层的邻居节点下标列表，
/// 长度 = level + 1（层 0 必有，上层可为空——首个高 level 节点在其上层暂无邻居）。
/// 节点层数即 `neighbors.len() - 1`（单一事实来源，不另存冗余字段）。
#[derive(Debug)]
struct HnswNode {
    id: String,
    neighbors: Vec<Vec<usize>>,
}

/// HNSW 索引。线程模型：非 Send/Sync 共享设计，`&mut self` 插入
/// （接入 storage 时由调用方持锁，见第 4 步）。
#[derive(Debug)]
pub struct HnswIndex {
    /// 节点表，下标即内部节点号 NodeId(usize)
    nodes: Vec<HnswNode>,
    /// 与 nodes 平行的向量副本（下标一致）
    vectors: Vec<Vec<f32>>,
    /// 已插入的外部 id 集合（O(1) 重复检查）
    ids: HashSet<String>,
    /// 最高层入口
    entry_point: Option<usize>,
    /// 当前最高层（== entry_point 的 level）
    top_level: usize,
    metric: Metric,
    /// 上层最大邻居数（论文默认 16）
    m: usize,
    /// 层 0 最大邻居数（= 2m）
    m_max0: usize,
    /// 建图搜索宽度
    ef_construction: usize,
    rng: SplitMix64,
}

impl HnswIndex {
    /// 创建空索引。`seed` 注入保证测试可复现。
    ///
    /// 参数校验：`m ≥ 2`（层数抽样 mL = 1/ln(m) 需要）、
    /// `ef_construction ≥ m`（否则建图搜索窄于连接数，图质量崩坏）。
    pub fn new(metric: Metric, m: usize, ef_construction: usize, seed: u64) -> Result<Self, String> {
        if m < 2 {
            return Err(format!("m 必须 ≥ 2（当前 {m}）：层数抽样 mL = 1/ln(m) 需要"));
        }
        if ef_construction < m {
            return Err(format!(
                "ef_construction 必须 ≥ m（当前 ef={ef_construction}, m={m}）"
            ));
        }
        Ok(Self {
            nodes: Vec::new(),
            vectors: Vec::new(),
            ids: HashSet::new(),
            entry_point: None,
            top_level: 0,
            metric,
            m,
            m_max0: m * 2,
            ef_construction,
            rng: SplitMix64::new(seed),
        })
    }

    /// 已插入向量数。
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// 索引是否为空。
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// 抽样新节点层数：`level = floor(−ln(u) × mL)`，`mL = 1/ln(m)`（论文参数）。
    /// 由此 `P(level ≥ l) = m^(-l)`：绝大多数节点落在层 0。
    fn sample_level(&mut self) -> usize {
        // u ∈ (0, 1)：取 53 位随机映射到 [0,1) 并拒绝 0（ln(0) 发散）
        let u = loop {
            let v = (self.rng.next() >> 11) as f64 / (1u64 << 53) as f64;
            if v > 0.0 {
                break v;
            }
        };
        let level_mult = 1.0 / (self.m as f64).ln();
        (-u.ln() * level_mult).floor() as usize
    }

    /// 与节点向量的距离。
    fn dist(&self, query: &[f32], node: usize) -> f64 {
        distance(self.metric, query, &self.vectors[node])
    }

    /// 贪心下降（上层用，ef=1）：每轮扫当前节点的全部邻居，有更近就跳过去，
    /// 无改进即收敛。返回 (最短距离, 节点)。
    fn greedy_search(&self, query: &[f32], mut ep: usize, layer: usize) -> (f64, usize) {
        let mut best = self.dist(query, ep);
        let mut visited = HashSet::new();
        loop {
            let mut improved = false;
            for &nb in &self.nodes[ep].neighbors[layer] {
                if visited.insert(nb) {
                    let d = self.dist(query, nb);
                    if d < best {
                        best = d;
                        ep = nb;
                        improved = true;
                    }
                }
            }
            if !improved {
                return (best, ep);
            }
        }
    }

    /// 论文 Algorithm 2（SEARCH-LAYER）：从入口集出发，在单层内探索，
    /// 返回至多 ef 个最近 (距离, 节点)，按距离升序。
    fn search_layer(
        &self,
        query: &[f32],
        eps: &[usize],
        ef: usize,
        layer: usize,
    ) -> Vec<(f64, usize)> {
        debug_assert!(ef >= 1);
        let mut visited = HashSet::new();
        // 候选堆：最小堆（按距离升序弹出）——Reverse 包装
        let mut candidates: BinaryHeap<Reverse<(OrdDist, usize)>> = BinaryHeap::new();
        // 结果堆：最大堆（堆顶 = 当前 ef 个最优里最差的）
        let mut results: BinaryHeap<(OrdDist, usize)> = BinaryHeap::new();
        for &ep in eps {
            if visited.insert(ep) {
                let d = OrdDist(self.dist(query, ep));
                candidates.push(Reverse((d, ep)));
                results.push((d, ep));
            }
        }
        while results.len() > ef {
            results.pop();
        }
        while let Some(Reverse((d_c, c))) = candidates.pop() {
            // 终止条件（hnswlib 变体）：候选最近距离比结果最差还远，且结果已满
            if results.len() >= ef && results.peek().is_some_and(|(worst, _)| d_c > *worst) {
                break;
            }
            for &nb in &self.nodes[c].neighbors[layer] {
                if visited.insert(nb) {
                    let d = OrdDist(self.dist(query, nb));
                    let not_full = results.len() < ef;
                    let better_than_worst = results.peek().is_some_and(|(worst, _)| d < *worst);
                    if not_full || better_than_worst {
                        candidates.push(Reverse((d, nb)));
                        results.push((d, nb));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        let mut out: Vec<(f64, usize)> =
            results.into_iter().map(|(d, n)| (d.0, n)).collect();
        out.sort_by(|a, b| a.0.total_cmp(&b.0));
        out
    }

    /// 添加单向边 from → to；邻居列表超过 `m_max` 时按与 from 向量的距离
    /// 截断到 `m_max`（简单裁剪版；启发式选择为未来优化）。
    fn link(&mut self, from: usize, to: usize, layer: usize, m_max: usize) {
        let list = &mut self.nodes[from].neighbors[layer];
        if list.contains(&to) {
            return; // 双向连接可能重复到达，防御
        }
        list.push(to);
        if list.len() > m_max {
            let from_vec = &self.vectors[from];
            let mut scored: Vec<(f64, usize)> = list
                .iter()
                .map(|&n| (distance(self.metric, from_vec, &self.vectors[n]), n))
                .collect();
            scored.sort_by(|a, b| a.0.total_cmp(&b.0));
            list.clear();
            list.extend(scored.into_iter().take(m_max).map(|(_, n)| n));
        }
    }

    /// 插入一个向量（论文 Algorithm 1）。
    ///
    /// 1. 空图 → 自成 entry_point；
    /// 2. 从 entry_point 贪心下降到目标层（严格高于新层，每层 ef=1）；
    /// 3. 自 `min(top_level, level)` 向下逐层 `search_layer(ef_construction)`，
    ///    取前 m 个近邻双向连接，超限裁剪；本层结果作为下一层入口；
    /// 4. 新层 > top_level → 更新 entry_point。
    ///
    /// 重复 id / 维度与首条不符 → `Err`（与 VADD 的 id 唯一 / 维度锁定语义一致）。
    pub fn insert(&mut self, id: &str, data: &[f32]) -> Result<(), String> {
        if self.ids.contains(id) {
            return Err(format!("重复的向量 id: {id}"));
        }
        if let Some(first) = self.vectors.first() {
            if first.len() != data.len() {
                return Err(format!(
                    "向量维度 {} 与索引维度 {} 不符",
                    data.len(),
                    first.len()
                ));
            }
        }
        let level = self.sample_level();
        let node = self.nodes.len();
        self.nodes.push(HnswNode {
            id: id.to_string(),
            neighbors: vec![Vec::new(); level + 1],
        });
        self.vectors.push(data.to_vec());
        self.ids.insert(id.to_string());

        let Some(ep0) = self.entry_point else {
            // 第一个向量：自成为入口（可能带高层但上层暂无邻居，见 h02 注释）
            self.entry_point = Some(node);
            self.top_level = level;
            return Ok(());
        };
        let mut ep = ep0;
        // 上层贪心下降（严格高于新节点的层）
        if self.top_level > level {
            for layer in (level + 1..=self.top_level).rev() {
                ep = self.greedy_search(data, ep, layer).1;
            }
        }
        // 逐层搜索 + 连接；本层结果全体作为下一层入口（论文 Algorithm 1）
        let mut eps = vec![ep];
        for layer in (0..=level.min(self.top_level)).rev() {
            let found = self.search_layer(data, &eps, self.ef_construction, layer);
            let m_max = if layer == 0 { self.m_max0 } else { self.m };
            for &(_, nb) in found.iter().take(self.m) {
                self.link(node, nb, layer, m_max);
                self.link(nb, node, layer, m_max);
            }
            eps = found.iter().map(|&(_, n)| n).collect();
        }
        if level > self.top_level {
            self.entry_point = Some(node);
            self.top_level = level;
        }
        Ok(())
    }

    /// 近似最近邻搜索：从 entry_point 贪心下降到底，层 0 以 `ef` 宽度搜索，
    /// 返回至多 k 个 (id, 距离)，按距离升序。
    ///
    /// **ef 决定查询精度**（ef_construction 只影响建图质量）：
    /// `ef ≥ 索引内向量数` 时搜索覆盖全部可达节点，结果与暴力完全一致。
    /// ef < k 时内部按 ef = max(ef, k) 处理（hnswlib 兼容；HNSW 要求 ef ≥ k 才有意义）。
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<(String, f64)>, String> {
        if let Some(first) = self.vectors.first() {
            if first.len() != query.len() {
                return Err(format!(
                    "查询维度 {} 与索引维度 {} 不符",
                    query.len(),
                    first.len()
                ));
            }
        }
        let Some(ep) = self.entry_point else {
            return Ok(Vec::new());
        };
        let mut ep = ep;
        for layer in (1..=self.top_level).rev() {
            ep = self.greedy_search(query, ep, layer).1;
        }
        let found = self.search_layer(query, &[ep], ef.max(k), 0);
        Ok(found
            .into_iter()
            .take(k)
            .map(|(d, n)| (self.nodes[n].id.clone(), d))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定参数索引（测试辅助）：m=16, ef_construction=64, m_max0=32, l2。
    fn new_index(seed: u64) -> HnswIndex {
        HnswIndex::new(Metric::L2, 16, 64, seed).expect("valid params")
    }

    /// 均匀随机 f32 向量（测试辅助）。
    fn rand_vec(rng: &mut SplitMix64, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|_| ((rng.next() >> 11) as f64 / (1u64 << 53) as f64) as f32)
            .collect()
    }

    /// 测试内暴力对照：返回最近 k 个 id。
    fn brute_top(vectors: &[(String, Vec<f32>)], query: &[f32], k: usize) -> Vec<String> {
        let mut scored: Vec<(f64, &str)> = vectors
            .iter()
            .map(|(id, v)| (distance(Metric::L2, query, v), id.as_str()))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, id)| id.to_string()).collect()
    }

    // —— h01–h10 ——

    #[test]
    fn h01_first_insert_becomes_entry_point() {
        let mut ix = new_index(1);
        assert!(ix.is_empty());
        ix.insert("a", &[1.0, 2.0, 3.0]).expect("insert");
        assert_eq!(ix.len(), 1);
        assert_eq!(ix.entry_point, Some(0));
    }

    #[test]
    fn h02_ten_nodes_reachable_on_layer0() {
        let mut ix = new_index(2);
        let mut rng = SplitMix64::new(1000);
        for i in 0..10 {
            ix.insert(&format!("n{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        // BFS 只在 layer 0 邻接表上遍历（层 0 包含全部节点）；
        // 上层节点在其上层可能没有邻居（第一个高 level 节点），故 BFS 不跨层。
        let entry = ix.entry_point.expect("entry");
        let mut queue = std::collections::VecDeque::new();
        let mut seen = HashSet::new();
        queue.push_back(entry);
        seen.insert(entry);
        while let Some(n) = queue.pop_front() {
            for &nb in &ix.nodes[n].neighbors[0] {
                if seen.insert(nb) {
                    queue.push_back(nb);
                }
            }
        }
        assert_eq!(seen.len(), 10, "layer 0 must be fully reachable");
        // 双向连接 ⇒ 除语义上允许的空图首点外，每个节点至少一条层 0 边
        assert!(ix.nodes.iter().all(|n| !n.neighbors[0].is_empty()));
    }

    #[test]
    fn h03_level_distribution() {
        let mut ix = new_index(3);
        let mut rng = SplitMix64::new(2000);
        for i in 0..200 {
            ix.insert(&format!("n{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        // P(level ≥ l) = m^(-l)（m=16）：绝大多数节点在层 0，层数不爆炸。
        // 节点层数 = neighbors.len() - 1，level 0 ⇔ 只有 1 层邻居表。
        let level0 = ix.nodes.iter().filter(|n| n.neighbors.len() == 1).count();
        assert!(level0 * 2 > ix.nodes.len(), "level 0 must hold the majority");
        assert!(ix.top_level <= 6, "top level must not explode");
    }

    #[test]
    fn h04_matches_bruteforce_on_data_points() {
        let mut ix = new_index(4);
        let mut rng = SplitMix64::new(3000);
        let mut points: Vec<(String, Vec<f32>)> = Vec::new();
        for i in 0..64 {
            let v = rand_vec(&mut rng, 8);
            ix.insert(&format!("p{i}"), &v).expect("insert");
            points.push((format!("p{i}"), v));
        }
        // ★ 查询精度取决于 search 的 ef（ef_construction 只影响建图）：
        // 显式传 ef = N = 64 ⇒ search_layer 探索全部可达节点 ⇒ 与暴力完全一致
        let n = ix.len();
        for (id, vec) in points.iter().take(20) {
            let hits = ix.search(vec, 5, n).expect("search");
            let got: Vec<String> = hits.into_iter().map(|(id, _)| id).collect();
            assert_eq!(got, brute_top(&points, vec, 5), "query = {id}");
        }
    }

    #[test]
    fn h05_matches_bruteforce_on_random_queries() {
        let mut ix = new_index(5);
        let mut rng = SplitMix64::new(4000);
        let mut points: Vec<(String, Vec<f32>)> = Vec::new();
        for i in 0..64 {
            let v = rand_vec(&mut rng, 8);
            ix.insert(&format!("p{i}"), &v).expect("insert");
            points.push((format!("p{i}"), v));
        }
        // ★ 同 h04：显式 ef = N = 64 ⇒ 与暴力完全一致
        let n = ix.len();
        for _ in 0..20 {
            let q = rand_vec(&mut rng, 8);
            let hits = ix.search(&q, 5, n).expect("search");
            let got: Vec<String> = hits.into_iter().map(|(id, _)| id).collect();
            assert_eq!(got, brute_top(&points, &q, 5));
        }
    }

    #[test]
    fn h06_k_truncation_ascending() {
        let mut ix = new_index(6);
        let mut rng = SplitMix64::new(5000);
        for i in 0..32 {
            ix.insert(&format!("p{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        let q = rand_vec(&mut rng, 8);
        let n = ix.len();
        let hits = ix.search(&q, 3, n).expect("search");
        assert_eq!(hits.len(), 3);
        for w in hits.windows(2) {
            assert!(w[0].1 <= w[1].1, "results must be ascending by distance");
        }
    }

    #[test]
    fn h07_duplicate_id_is_error() {
        let mut ix = new_index(7);
        ix.insert("dup", &[1.0, 2.0]).expect("first");
        assert!(ix.insert("dup", &[3.0, 4.0]).is_err());
    }

    #[test]
    fn h08_dimension_lock() {
        let mut ix = new_index(8);
        ix.insert("a", &[1.0, 2.0, 3.0]).expect("first 3-dim");
        assert!(ix.insert("b", &[1.0, 2.0]).is_err());
    }

    #[test]
    fn h09_neighbor_cap_per_layer() {
        let mut ix = new_index(9);
        let mut rng = SplitMix64::new(6000);
        for i in 0..100 {
            ix.insert(&format!("p{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        // 每层每节点：层 0 ≤ m_max0(32)，层 >0 ≤ m(16)
        for node in &ix.nodes {
            for (layer, list) in node.neighbors.iter().enumerate() {
                let cap = if layer == 0 { ix.m_max0 } else { ix.m };
                assert!(
                    list.len() <= cap,
                    "node {} layer {} has {} > {cap}",
                    node.id,
                    layer,
                    list.len()
                );
            }
        }
    }

    #[test]
    fn h10_empty_and_single_node_edges() {
        // 空索引：search 返回空
        let empty = new_index(10);
        assert!(empty.search(&[1.0], 5, 16).expect("empty search").is_empty());
        // 单点索引：查询自身 → 恰该点、距离 0
        let mut ix = new_index(11);
        ix.insert("only", &[1.0]).expect("insert");
        let hits = ix.search(&[1.0], 5, 16).expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "only");
        assert!(hits[0].1.abs() < 1e-12);
    }

    // —— 进阶 3 第 2 步：召回率与参数验证（h11–h19；h17 由 h10 覆盖，跳过）——

    #[test]
    fn h11_recall_1k_above_threshold() {
        let mut ix = HnswIndex::new(Metric::L2, 16, 200, 7000).expect("valid params");
        let mut rng = SplitMix64::new(7001);
        let mut points: Vec<(String, Vec<f32>)> = Vec::new();
        for i in 0..1000 {
            let v = rand_vec(&mut rng, 10);
            ix.insert(&format!("p{i}"), &v).expect("insert");
            points.push((format!("p{i}"), v));
        }
        let mut q_rng = SplitMix64::new(7002);
        let mut total_overlap = 0usize;
        let mut worst = usize::MAX;
        for _ in 0..100 {
            let q = rand_vec(&mut q_rng, 10);
            let hits = ix.search(&q, 10, 100).expect("search");
            let got: Vec<String> = hits.into_iter().map(|(id, _)| id).collect();
            let truth = brute_top(&points, &q, 10);
            let overlap = got.iter().filter(|id| truth.contains(id)).count();
            total_overlap += overlap;
            worst = worst.min(overlap);
        }
        let recall = total_overlap as f64 / (100.0 * 10.0);
        eprintln!("h11 recall@10 (1000 pts, m=16, efc=200, ef=100) = {recall:.4}, worst query = {worst}/10");
        assert!(recall >= 0.95, "recall {recall:.4} below 0.95 threshold");
    }

    #[test]
    #[ignore = "1 万点建图在 debug 下较慢；手动 cargo test -- --ignored --nocapture 运行"]
    fn h12_recall_10k_smoke() {
        let mut ix = HnswIndex::new(Metric::L2, 16, 200, 8000).expect("valid params");
        let mut rng = SplitMix64::new(8001);
        let mut points: Vec<(String, Vec<f32>)> = Vec::new();
        let t0 = std::time::Instant::now();
        for i in 0..10_000 {
            let v = rand_vec(&mut rng, 10);
            ix.insert(&format!("p{i}"), &v).expect("insert");
            points.push((format!("p{i}"), v));
        }
        eprintln!("h12: 10000 inserts in {:.1?}", t0.elapsed());
        let mut q_rng = SplitMix64::new(8002);
        let t1 = std::time::Instant::now();
        let mut total_overlap = 0usize;
        let mut worst = usize::MAX;
        for _ in 0..100 {
            let q = rand_vec(&mut q_rng, 10);
            let hits = ix.search(&q, 10, 100).expect("search");
            let got: Vec<String> = hits.into_iter().map(|(id, _)| id).collect();
            let truth = brute_top(&points, &q, 10);
            let overlap = got.iter().filter(|id| truth.contains(id)).count();
            total_overlap += overlap;
            worst = worst.min(overlap);
        }
        let recall = total_overlap as f64 / (100.0 * 10.0);
        eprintln!("h12: recall@10 = {recall:.4}, worst query = {worst}/10, 100 queries in {:.1?}", t1.elapsed());
        assert!(recall >= 0.90, "recall {recall:.4} below 0.90 threshold");
    }

    #[test]
    fn h13_ef_below_k_is_promoted() {
        // ef < k 时语义定义为 ef := max(ef, k)（hnswlib 兼容）：
        // HNSW 要求 ef ≥ k 才有意义，误传参数时防御性提升而非返回残缺结果
        let mut ix = new_index(13);
        let mut rng = SplitMix64::new(13000);
        for i in 0..50 {
            ix.insert(&format!("p{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        let q = rand_vec(&mut rng, 8);
        let hits = ix.search(&q, 10, 5).expect("search");
        assert_eq!(hits.len(), 10, "ef must be promoted to k");
        for w in hits.windows(2) {
            assert!(w[0].1 <= w[1].1, "results must be ascending");
        }
    }

    #[test]
    fn h14_k_zero_returns_empty() {
        let mut ix = new_index(14);
        let mut rng = SplitMix64::new(14000);
        for i in 0..10 {
            ix.insert(&format!("p{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        let q = rand_vec(&mut rng, 8);
        assert!(ix.search(&q, 0, 16).expect("search").is_empty());
    }

    #[test]
    fn h15_duplicate_vectors_and_one_distinct() {
        // 100 个完全相同的向量聚簇 + 1 个不同的；查询 = 那个不同的向量本身：
        // dist(distinct) = 0，其余 ≥ sqrt(8)，ef = N 保证穷举命中
        let mut ix = new_index(15);
        for i in 0..100 {
            ix.insert(&format!("same{i}"), &[1.0; 8]).expect("insert same");
        }
        ix.insert("distinct", &[2.0; 8]).expect("insert distinct");
        let query = [2.0f32; 8];
        let hits = ix.search(&query, 1, 101).expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "distinct");
        assert!(hits[0].1.abs() < 1e-12);
    }

    #[test]
    fn h16_zero_vector_semantics() {
        // l2：零向量插入不 panic；查询=零向量 → top-1 = 零向量、距离 0
        let mut ix = new_index(16);
        let mut rng = SplitMix64::new(16000);
        for i in 0..20 {
            ix.insert(&format!("p{i}"), &rand_vec(&mut rng, 8)).expect("insert");
        }
        ix.insert("zero", &[0.0; 8]).expect("insert zero");
        let hits = ix.search(&[0.0; 8], 1, 32).expect("search");
        assert_eq!(hits[0].0, "zero");
        assert!(hits[0].1.abs() < 1e-12);
        // cos：零模长守卫（distance.rs）→ 与所有向量距离 1.0，全部有限（无 NaN）
        let mut cix = HnswIndex::new(Metric::Cosine, 16, 64, 16001).expect("valid params");
        cix.insert("zero", &[0.0; 8]).expect("insert zero");
        let mut rng2 = SplitMix64::new(16002);
        for i in 0..20 {
            cix.insert(&format!("p{i}"), &rand_vec(&mut rng2, 8)).expect("insert");
        }
        let hits = cix.search(&[0.0; 8], 5, 32).expect("search");
        assert_eq!(hits.len(), 5);
        for (id, d) in &hits {
            assert!(d.is_finite(), "distance must be finite for {id}");
            assert!((d - 1.0).abs() < 1e-12, "zero-norm guard must yield 1.0, got {d}");
        }
    }

    #[test]
    fn h18_same_seed_same_graph() {
        let mut rng = SplitMix64::new(18000);
        let data: Vec<Vec<f32>> = (0..200).map(|_| rand_vec(&mut rng, 8)).collect();
        // 同 seed + 同数据 + 同插入顺序 ⇒ 图结构逐项一致
        let build = || {
            let mut ix = new_index(18001);
            for (i, v) in data.iter().enumerate() {
                ix.insert(&format!("p{i}"), v).expect("insert");
            }
            ix
        };
        let a = build();
        let b = build();
        assert_eq!(a.len(), b.len());
        for (na, nb) in a.nodes.iter().zip(b.nodes.iter()) {
            assert_eq!(na.id, nb.id);
            assert_eq!(na.neighbors, nb.neighbors);
        }
    }

    #[test]
    fn h19_same_seed_same_search_results() {
        let mut rng = SplitMix64::new(19000);
        let data: Vec<Vec<f32>> = (0..200).map(|_| rand_vec(&mut rng, 8)).collect();
        let mut q_rng = SplitMix64::new(19001);
        let queries: Vec<Vec<f32>> = (0..20).map(|_| rand_vec(&mut q_rng, 8)).collect();
        let run = || {
            let mut ix = new_index(19002);
            for (i, v) in data.iter().enumerate() {
                ix.insert(&format!("p{i}"), v).expect("insert");
            }
            queries
                .iter()
                .map(|q| ix.search(q, 5, 64).expect("search"))
                .collect::<Vec<_>>()
        };
        let a = run();
        let b = run();
        assert_eq!(a, b);
    }
}
