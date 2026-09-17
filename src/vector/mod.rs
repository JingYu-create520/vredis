//! 向量索引层：距离度量与暴力 top-k 搜索（MVP 无 HNSW，docs/design.md §1/§4.2）。
//!
//! 分层约束：本层**只读**存储层（经 [`crate::storage::Db`] 的 probe_index /
//! for_each_vector / get_vector），距离与 top-k 收集全部发生在 storage 持锁回调内；
//! 一切写操作（VADD）都经由 storage 完成。

pub mod distance;
pub mod search;

pub use distance::Metric;
pub use search::{search, Hit, SearchError};
