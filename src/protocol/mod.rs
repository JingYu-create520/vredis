//! 协议解析层：RESP2 值的表示、流式解析与编码。
//!
//! 与 TCP 完全解耦：输入输出都是内存字节缓冲区，
//! 因此本层可以脱离网络独立单测（分层约束见 docs/design.md §1）。

pub mod encoder;
pub mod parser;
pub mod value;

pub use encoder::encode;
pub use parser::{parse, ParseOutcome};
pub use value::RespValue;
