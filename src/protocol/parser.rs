//! RESP2 流式解析器：字节缓冲区 → [`RespValue`]。
//!
//! 设计要点（详见 docs/design.md §3）：
//! - **无状态**：每次对当前缓冲区做完整解析；数据不足返回 [`ParseOutcome::Incomplete`]，
//!   调用方（未来的连接层）把 socket 新数据 append 进缓冲区后再次调用。
//!   MVP 阶段消息小，O(n) 重解析完全可接受；有状态增量解析留作后续优化。
//! - **绝不 panic**：所有畸形/恶意输入要么 Error 要么 Incomplete，不做任何按声明数的
//!   预分配（内存增长始终与实际收到的字节数成正比）。
//! - **inline 命令**：非五种类型前缀开头的行按空格/Tab 切分为 Bulk 数组，
//!   方便 `nc` 手测；已知简化：不做引号处理（写入 README）。

use super::value::RespValue;

/// 批量字符串最大长度，对齐 Redis 默认 `proto-max-bulk-len`（512MB）。
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// 数组元素数上限：合法命令的参数远小于此，超限视为恶意输入直接拒绝。
const MAX_ARRAY_LEN: usize = 1024 * 1024;
/// 数组嵌套深度上限：防止 `*1\r\n*1\r\n...` 深度递归打爆调用栈。
const MAX_DEPTH: usize = 64;

/// 单次解析的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    /// 解析成功：值 + 从缓冲区头部消费的字节数；剩余字节属于下一条消息。
    Complete(RespValue, usize),
    /// 缓冲区数据不足，等待下一次读取（不是错误）。
    Incomplete,
    /// 协议损坏，连接层必须断开连接。
    Error(String),
}

/// 解析缓冲区头部的一条 RESP2 消息（或一条 inline 命令）。
pub fn parse(buf: &[u8]) -> ParseOutcome {
    parse_at(buf, 0, 0)
}

/// 从 `pos` 开始解析一条消息。约定：`Complete` 的 usize 是消息结束的
/// **绝对位置**（相对 `buf` 开头）——各子解析函数都按绝对位置返回，
/// 顶层 `parse()`（pos=0）时它恰好等于消费的字节数。
fn parse_at(buf: &[u8], pos: usize, depth: usize) -> ParseOutcome {
    // 递归入口必须先判界：数组元素可能恰好到缓冲区末尾
    if pos >= buf.len() {
        return ParseOutcome::Incomplete;
    }
    match buf[pos] {
        b'+' => parse_simple(buf, pos),
        b'-' => parse_error(buf, pos),
        b':' => parse_integer(buf, pos),
        b'$' => parse_bulk(buf, pos),
        b'*' => parse_array(buf, pos, depth),
        _ => parse_inline(buf, pos),
    }
}

/// 在 `buf[pos..]` 中找一条严格 CRLF 结尾的行（用于 `+ - : $ *` 五类消息）。
/// 返回 `None` 表示数据不足；`Some(Err)` 表示裸 `\n`（协议要求 CRLF）。
/// `Ok((行内容不含CRLF, 行尾之后的位置))`。
fn find_crlf_line(buf: &[u8], pos: usize) -> Option<Result<(&[u8], usize), String>> {
    let rest = &buf[pos..];
    let lf = rest.iter().position(|&b| b == b'\n')?;
    if lf == 0 || rest[lf - 1] != b'\r' {
        return Some(Err("protocol error: expected CRLF line ending".to_string()));
    }
    Some(Ok((&rest[..lf - 1], pos + lf + 1)))
}

/// 简单字符串：`+内容\r\n`。内容按字节收下，非 UTF-8 宽容处理（lossy）。
fn parse_simple(buf: &[u8], pos: usize) -> ParseOutcome {
    match find_crlf_line(buf, pos + 1) {
        None => ParseOutcome::Incomplete,
        Some(Err(e)) => ParseOutcome::Error(e),
        Some(Ok((line, end))) => ParseOutcome::Complete(
            RespValue::Simple(String::from_utf8_lossy(line).into_owned()),
            end,
        ),
    }
}

/// 错误回复：`-内容\r\n`。结构与简单字符串相同。
fn parse_error(buf: &[u8], pos: usize) -> ParseOutcome {
    match find_crlf_line(buf, pos + 1) {
        None => ParseOutcome::Incomplete,
        Some(Err(e)) => ParseOutcome::Error(e),
        Some(Ok((line, end))) => ParseOutcome::Complete(
            RespValue::Error(String::from_utf8_lossy(line).into_owned()),
            end,
        ),
    }
}

/// 整数：`:数值\r\n`。非数字或超出 i64 范围 → Error。
fn parse_integer(buf: &[u8], pos: usize) -> ParseOutcome {
    let (line, end) = match find_crlf_line(buf, pos + 1) {
        None => return ParseOutcome::Incomplete,
        Some(Err(e)) => return ParseOutcome::Error(e),
        Some(Ok(x)) => x,
    };
    match std::str::from_utf8(line).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(i) => ParseOutcome::Complete(RespValue::Integer(i), end),
        None => ParseOutcome::Error("protocol error: invalid integer".to_string()),
    }
}

/// 批量字符串：`$长度\r\npayload\r\n`；`$-1\r\n` 为 Null。
/// payload 内允许任意字节（含 CRLF/NUL），二进制安全由声明长度保证。
fn parse_bulk(buf: &[u8], pos: usize) -> ParseOutcome {
    let (line, after_len) = match find_crlf_line(buf, pos + 1) {
        None => return ParseOutcome::Incomplete,
        Some(Err(e)) => return ParseOutcome::Error(e),
        Some(Ok(x)) => x,
    };
    let len: i64 = match std::str::from_utf8(line).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return ParseOutcome::Error("protocol error: invalid bulk length".to_string()),
    };
    if len == -1 {
        return ParseOutcome::Complete(RespValue::Null, after_len);
    }
    if !(0..=MAX_BULK_LEN as i64).contains(&len) {
        // 涵盖负数与超过 512MB 上限的声明：直接拒绝，绝不分配内存
        return ParseOutcome::Error("protocol error: invalid bulk length".to_string());
    }
    let len = len as usize;
    // checked_add 防御性处理：任何溢出等价于超出上限，按协议错误拒绝
    let payload_end = match after_len.checked_add(len).and_then(|e| e.checked_add(2)) {
        Some(e) => e,
        None => return ParseOutcome::Error("protocol error: invalid bulk length".to_string()),
    };
    if buf.len() < payload_end {
        return ParseOutcome::Incomplete;
    }
    if &buf[payload_end - 2..payload_end] != b"\r\n" {
        return ParseOutcome::Error("protocol error: expected CRLF after bulk payload".to_string());
    }
    ParseOutcome::Complete(
        RespValue::Bulk(buf[after_len..after_len + len].to_vec()),
        payload_end,
    )
}

/// 数组：`*元素数\r\n元素...\r\n`；`*-1\r\n` 折叠为 Null。
/// 逐元素解析并 push，绝不按声明数预分配容量（OOM 防护，见模块注释）。
fn parse_array(buf: &[u8], pos: usize, depth: usize) -> ParseOutcome {
    if depth >= MAX_DEPTH {
        return ParseOutcome::Error("protocol error: array nesting too deep".to_string());
    }
    let (line, mut cur) = match find_crlf_line(buf, pos + 1) {
        None => return ParseOutcome::Incomplete,
        Some(Err(e)) => return ParseOutcome::Error(e),
        Some(Ok(x)) => x,
    };
    let count: i64 = match std::str::from_utf8(line).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return ParseOutcome::Error("protocol error: invalid array length".to_string()),
    };
    if count == -1 {
        return ParseOutcome::Complete(RespValue::Null, cur);
    }
    if !(0..=MAX_ARRAY_LEN as i64).contains(&count) {
        return ParseOutcome::Error("protocol error: too many array elements".to_string());
    }
    let mut items = Vec::new();
    for _ in 0..count {
        match parse_at(buf, cur, depth + 1) {
            ParseOutcome::Complete(v, n) => {
                items.push(v);
                cur = n; // n 是元素结束的绝对位置，直接推进（不是增量）
            }
            // 任一元素数据不足：整体回退为 Incomplete，下次从缓冲区头部重新解析
            ParseOutcome::Incomplete => return ParseOutcome::Incomplete,
            ParseOutcome::Error(e) => return ParseOutcome::Error(e),
        }
    }
    ParseOutcome::Complete(RespValue::Array(items), cur)
}

/// inline 命令：非五种前缀开头的行，按 ASCII 空白（空格/Tab）切分为 Bulk 数组。
/// 行尾 `\n` 即可、`\r` 可选（与 Redis inline 行为一致）；空行解析为空数组。
fn parse_inline(buf: &[u8], pos: usize) -> ParseOutcome {
    let rest = &buf[pos..];
    let Some(lf) = rest.iter().position(|&b| b == b'\n') else {
        return ParseOutcome::Incomplete;
    };
    let mut line = &rest[..lf];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let items = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty())
        .map(|s| RespValue::Bulk(s.to_vec()))
        .collect();
    ParseOutcome::Complete(RespValue::Array(items), pos + lf + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::encoder::encode;

    /// 测试辅助：断言 Complete 并解包为 (值, 消费字节数)。
    fn complete(out: ParseOutcome) -> (RespValue, usize) {
        match out {
            ParseOutcome::Complete(v, n) => (v, n),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    /// 测试辅助：断言 Error 并取出错误信息。
    fn error_of(out: ParseOutcome) -> String {
        match out {
            ParseOutcome::Error(e) => e,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    // —— B. 解析正常路径 ——

    #[test]
    fn b01_parse_simple() {
        let (v, n) = complete(parse(b"+OK\r\n"));
        assert_eq!(v, RespValue::Simple("OK".into()));
        assert_eq!(n, 5);
    }

    #[test]
    fn b02_parse_error() {
        let (v, n) = complete(parse(b"-ERR unknown command 'foo'\r\n"));
        assert_eq!(v, RespValue::Error("ERR unknown command 'foo'".into()));
        assert_eq!(n, 28);
    }

    #[test]
    fn b03_parse_integer() {
        assert_eq!(complete(parse(b":42\r\n")).0, RespValue::Integer(42));
        assert_eq!(complete(parse(b":-7\r\n")).0, RespValue::Integer(-7));
        assert_eq!(complete(parse(b":9223372036854775807\r\n")).0, RespValue::Integer(i64::MAX));
        assert_eq!(complete(parse(b":-9223372036854775808\r\n")).0, RespValue::Integer(i64::MIN));
    }

    #[test]
    fn b04_parse_bulk() {
        let (v, n) = complete(parse(b"$5\r\nhello\r\n"));
        assert_eq!(v, RespValue::Bulk(b"hello".to_vec()));
        assert_eq!(n, 11);
    }

    #[test]
    fn b05_parse_bulk_empty() {
        let (v, n) = complete(parse(b"$0\r\n\r\n"));
        assert_eq!(v, RespValue::Bulk(Vec::new()));
        assert_eq!(n, 6);
    }

    #[test]
    fn b06_parse_bulk_binary_payload() {
        // payload 含 CRLF 与 NUL：二进制安全由声明长度保证，不被行解析截断
        let (v, n) = complete(parse(b"$5\r\nab\r\nc\r\n"));
        assert_eq!(v, RespValue::Bulk(b"ab\r\nc".to_vec()));
        assert_eq!(n, 11);
    }

    #[test]
    fn b07_parse_null_bulk() {
        let (v, n) = complete(parse(b"$-1\r\n"));
        assert_eq!(v, RespValue::Null);
        assert_eq!(n, 5);
    }

    #[test]
    fn b08_parse_flat_array() {
        let (v, n) = complete(parse(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nhello\r\n"));
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Bulk(b"SET".to_vec()),
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Bulk(b"hello".to_vec()),
            ])
        );
        assert_eq!(n, 31);
    }

    #[test]
    fn b09_parse_nested_array() {
        let (v, _) = complete(parse(b"*2\r\n*2\r\n:1\r\n$-1\r\n+hi\r\n"));
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::Integer(1), RespValue::Null]),
                RespValue::Simple("hi".into()),
            ])
        );
    }

    #[test]
    fn b10_parse_null_array_folds_to_null() {
        let (v, n) = complete(parse(b"*-1\r\n"));
        assert_eq!(v, RespValue::Null);
        assert_eq!(n, 5);
    }

    #[test]
    fn b11_parse_inline_single() {
        let (v, n) = complete(parse(b"PING\r\n"));
        assert_eq!(v, RespValue::Array(vec![RespValue::Bulk(b"PING".to_vec())]));
        assert_eq!(n, 6);
    }

    #[test]
    fn b12_parse_inline_multiple_whitespace() {
        // 连续空格/Tab 视为分隔符，不产生空 token
        let (v, _) = complete(parse(b"SET  a\t b\r\n"));
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Bulk(b"SET".to_vec()),
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Bulk(b"b".to_vec()),
            ])
        );
    }

    #[test]
    fn b13_parse_inline_empty_line() {
        let (v, n) = complete(parse(b"\r\n"));
        assert_eq!(v, RespValue::Array(vec![]));
        assert_eq!(n, 2);
    }

    #[test]
    fn b14_parse_inline_bare_lf() {
        // inline 行容许裸 \n 结尾（与 Redis inline 行为一致）
        let (v, n) = complete(parse(b"PING\n"));
        assert_eq!(v, RespValue::Array(vec![RespValue::Bulk(b"PING".to_vec())]));
        assert_eq!(n, 5);
    }

    // —— C. 截断 / Incomplete ——

    #[test]
    fn c01_every_truncated_prefix_is_incomplete() {
        // 对每类合法消息：任何截断前缀都必须是 Incomplete，完整消息必须 Complete
        let messages: [&[u8]; 7] = [
            b"+OK\r\n",
            b"-ERR x\r\n",
            b":123\r\n",
            b"$3\r\nabc\r\n",
            b"$-1\r\n",
            b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            b"*1\r\n*1\r\n:5\r\n",
        ];
        for msg in messages {
            for cut in 0..msg.len() {
                let out = parse(&msg[..cut]);
                assert_eq!(out, ParseOutcome::Incomplete, "prefix of {msg:?}");
            }
            assert!(matches!(parse(msg), ParseOutcome::Complete(_, _)));
        }
    }

    #[test]
    fn c02_crlf_split_across_reads() {
        // CRLF 被拆在两次读取之间：只见到 \r 时必须等待 \n
        assert_eq!(parse(b"+OK\r"), ParseOutcome::Incomplete);
        assert_eq!(
            parse(b"+OK\r\n"),
            ParseOutcome::Complete(RespValue::Simple("OK".into()), 5)
        );
        // inline 空行的 \r 同理
        assert_eq!(parse(b"\r"), ParseOutcome::Incomplete);
        assert_eq!(parse(b"\r\n"), ParseOutcome::Complete(RespValue::Array(vec![]), 2));
    }

    #[test]
    fn c03_array_with_incomplete_element() {
        assert_eq!(parse(b"*2\r\n$3\r\nSET\r\n"), ParseOutcome::Incomplete);
        assert_eq!(parse(b"*2\r\n$3\r\nSET\r\n$1"), ParseOutcome::Incomplete);
    }

    #[test]
    fn c04_inline_without_newline() {
        assert_eq!(parse(b"PING"), ParseOutcome::Incomplete);
    }

    // —— D. 协议错误 / OOM 防护 ——

    #[test]
    fn d01_invalid_bulk_length() {
        assert!(error_of(parse(b"$-2\r\n")).contains("bulk"));
        assert!(error_of(parse(b"$abc\r\n")).contains("bulk"));
        assert!(error_of(parse(b"$\r\n")).contains("bulk"));
    }

    #[test]
    fn d02_invalid_array_length() {
        assert!(error_of(parse(b"*-2\r\n")).contains("array"));
        assert!(error_of(parse(b"*x\r\n")).contains("array"));
    }

    #[test]
    fn d03_invalid_integer() {
        assert!(error_of(parse(b":abc\r\n")).contains("integer"));
        assert!(error_of(parse(b":\r\n")).contains("integer"));
        // 超出 i64 范围（溢出）→ 拒绝
        assert!(error_of(parse(b":99999999999999999999\r\n")).contains("integer"));
    }

    #[test]
    fn d04_missing_crlf_after_bulk_payload() {
        // 声明长度之后紧跟的不是 CRLF → 协议错误
        assert!(error_of(parse(b"$3\r\nabcXY\r\n")).contains("CRLF"));
        assert!(error_of(parse(b"$3\r\nabc\rX\r\n")).contains("CRLF"));
        // 终止符本身被截断 → 仍是 Incomplete 而非 Error
        assert_eq!(parse(b"$3\r\nabc\r"), ParseOutcome::Incomplete);
    }

    #[test]
    fn d05_scalar_bare_lf_is_error() {
        // 类型消息必须 CRLF，裸 LF 视为协议损坏
        assert!(error_of(parse(b"+OK\n")).contains("CRLF"));
        assert!(error_of(parse(b":5\n")).contains("CRLF"));
    }

    #[test]
    fn d06_bulk_too_large() {
        // 声明 1GB > 512MB 上限 → 直接拒绝，不分配内存
        assert!(error_of(parse(b"$1073741824\r\n")).contains("bulk"));
    }

    #[test]
    fn d07_array_oom_guard() {
        // 恶意大数组：超过元素上限直接 Error（不预分配、不 OOM）
        assert!(error_of(parse(b"*2000000000\r\n")).contains("array"));
        // 上限内但数据不足：Incomplete，绝不因声明数大而分配内存
        assert_eq!(parse(b"*1000\r\n"), ParseOutcome::Incomplete);
    }

    #[test]
    fn d08_nesting_depth_guard() {
        // 深度上限 64：64 层嵌套可解析，65 层拒绝（防递归打爆栈）
        let ok_msg = format!("{}:1\r\n", "*1\r\n".repeat(64));
        assert!(matches!(parse(ok_msg.as_bytes()), ParseOutcome::Complete(_, _)));
        let deep_msg = format!("{}:1\r\n", "*1\r\n".repeat(65));
        assert!(error_of(parse(deep_msg.as_bytes())).contains("deep"));
    }

    // —— E. roundtrip 与消费边界 ——

    #[test]
    fn e01_roundtrip_all_types() {
        let samples = vec![
            RespValue::Simple("OK".into()),
            RespValue::Error("ERR msg".into()),
            RespValue::Integer(0),
            RespValue::Integer(-1),
            RespValue::Integer(i64::MAX),
            RespValue::Integer(i64::MIN),
            RespValue::Bulk(b"hello".to_vec()),
            RespValue::Bulk(b"a\r\n\0b".to_vec()),
            RespValue::Bulk(Vec::new()),
            RespValue::Null,
            RespValue::Array(vec![]),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Null]),
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::Bulk(b"x".to_vec())]),
                RespValue::Simple("hi".into()),
            ]),
        ];
        for v in samples {
            let bytes = encode(&v);
            let (parsed, n) = complete(parse(&bytes));
            assert_eq!(n, bytes.len(), "consumed must equal encoded length for {v:?}");
            assert_eq!(parsed, v, "roundtrip mismatch for {v:?}");
        }
    }

    #[test]
    fn e02_roundtrip_command_like() {
        let cmd = RespValue::Array(vec![
            RespValue::Bulk(b"SET".to_vec()),
            RespValue::Bulk(b"a".to_vec()),
            RespValue::Bulk(b"hello".to_vec()),
        ]);
        let bytes = encode(&cmd);
        let (parsed, n) = complete(parse(&bytes));
        assert_eq!(parsed, cmd);
        assert_eq!(n, bytes.len());
    }

    #[test]
    fn e03_leftover_bytes_preserved() {
        let first = encode(&RespValue::Bulk(b"abc".to_vec())); // $3\r\nabc\r\n
        let second = encode(&RespValue::Array(vec![RespValue::Integer(1)])); // *1\r\n:1\r\n
        let mut buf = first.clone();
        buf.extend_from_slice(&second);
        let (v, n) = complete(parse(&buf));
        assert_eq!(v, RespValue::Bulk(b"abc".to_vec()));
        assert_eq!(n, first.len());
        // 只消费第一条消息，剩余字节原样保留给下一条
        assert_eq!(&buf[n..], &second[..]);
    }
}
