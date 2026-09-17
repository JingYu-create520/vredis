//! vredis —— 用 Rust 实现的、兼容 Redis RESP2 协议的迷你向量数据库。
//!
//! 分层架构见 `docs/design.md`：
//! 网络层(net) → 协议层(protocol) → 命令分发(command) → 存储(storage) / 向量索引(vector)。
//! 当前阶段已实现：协议层、命令分发骨架（PING/ECHO）、网络层。

pub mod command;
pub mod config;
pub mod net;
pub mod persist;
pub mod protocol;
pub mod storage;
pub mod vector;
