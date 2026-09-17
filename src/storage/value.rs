//! 存储值类型：key 背后存的东西（docs/design.md §3 v0.2 索引模型）。

use std::collections::HashMap;

/// key 背后存储的值。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 字符串：SET / GET 管理；字节内容二进制安全。
    Str(Vec<u8>),
    /// 向量索引：VADD 管理。一个 key 即一个索引（集合），
    /// 维度在首次 VADD 时锁定；内部为 向量 id → 分量 的映射，
    /// `next_id` 为自增 id 计数器（id 为十进制串，从 "0" 起，design.md D8）。
    VectorIndex {
        dim: usize,
        vectors: HashMap<String, Vec<f32>>,
        next_id: u64,
    },
}
