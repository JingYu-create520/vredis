//! RESP2 协议值的统一表示。
//!
//! 参考规范：Redis RESP2 protocol spec（5 种基础类型 + Null 批量字符串）。
//! 实现取舍：`*-1`（null 数组）解析时折叠为 [`RespValue::Null`]——
//! 本服务器永远不会主动回复 null 数组，不值得为其单设变体（见 docs/design.md §3 D6）。

/// RESP2 协议值的统一表示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// 简单字符串：`+OK\r\n`。按协议，内容不得包含 `\r` 或 `\n`。
    Simple(String),
    /// 错误回复：`-ERR unknown command 'foo'\r\n`。内容同样不得包含 CRLF。
    Error(String),
    /// 有符号 64 位整数：`:42\r\n`。
    Integer(i64),
    /// 批量字符串：`$5\r\nhello\r\n`。
    /// 用 `Vec<u8>` 承载保证二进制安全：payload 内允许 `\r`、`\n`、`\0` 等任意字节。
    Bulk(Vec<u8>),
    /// RESP2 的 null 批量字符串：`$-1\r\n`。
    Null,
    /// 数组：`*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`。可任意嵌套（解析侧有深度上限）。
    Array(Vec<RespValue>),
}
