//! 向量索引层：距离度量、暴力 top-k 搜索与自研 HNSW 分层图索引（进阶 3）。
//!
//! 分层约束（阶段 5 锁纪律）：本层对存储层**只读**（经 [`crate::storage::Db`] 的
//! probe_index / for_each_vector / get_vector），距离与 top-k 收集全部发生在
//! storage 持锁回调内；一切写操作都经由 storage 完成。
//! VSEARCH 默认仍走暴力搜索（`search.rs`）；HNSW（`hnsw.rs`）尚未接入命令层，
//! 由后续步骤按参数/feature 切换。

pub mod distance;
pub mod hnsw;
pub mod search;

pub use distance::Metric;
pub use hnsw::HnswIndex;
pub use search::{search, Hit, SearchError};
