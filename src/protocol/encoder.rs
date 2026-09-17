//! RESP2 编码器：把 [`RespValue`] 序列化为标准 RESP2 字节流。
//!
//! 只负责编码不做校验：Simple/Error 内容含 CRLF 属于调用方违约
//! （本 crate 内部所有调用点都不会构造这种值），
//! debug 构建下用 `debug_assert!` 及时暴露，release 下按原样写出。

use super::value::RespValue;

/// 把一个 RESP2 值编码为完整的 RESP2 字节流。
pub fn encode(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(value, &mut out);
    out
}

/// 递归写入：整棵值树共享同一个输出缓冲区，避免每层重复分配。
fn write_value(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::Simple(s) => {
            debug_assert!(!s.contains('\r') && !s.contains('\n'), "Simple 不得含 CRLF");
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            debug_assert!(!s.contains('\r') && !s.contains('\n'), "Error 不得含 CRLF");
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Bulk(data) => {
            out.push(b'$');
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Null => out.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                write_value(item, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // —— A. 编码测试：每种类型精确到字节 ——

    #[test]
    fn a01_encode_simple() {
        assert_eq!(encode(&RespValue::Simple("OK".into())), b"+OK\r\n");
    }

    #[test]
    fn a02_encode_error() {
        assert_eq!(encode(&RespValue::Error("ERR bad".into())), b"-ERR bad\r\n");
    }

    #[test]
    fn a03_encode_integer() {
        assert_eq!(encode(&RespValue::Integer(42)), b":42\r\n");
        assert_eq!(encode(&RespValue::Integer(-7)), b":-7\r\n");
        assert_eq!(encode(&RespValue::Integer(i64::MIN)), b":-9223372036854775808\r\n");
    }

    #[test]
    fn a04_encode_bulk() {
        assert_eq!(encode(&RespValue::Bulk(b"hello".to_vec())), b"$5\r\nhello\r\n");
    }

    #[test]
    fn a05_encode_bulk_empty() {
        assert_eq!(encode(&RespValue::Bulk(Vec::new())), b"$0\r\n\r\n");
    }

    #[test]
    fn a06_encode_bulk_binary_safe() {
        // payload 含 CRLF 与 NUL：编码必须原样保留，不做任何转义
        let payload = b"a\r\n\0b".to_vec();
        assert_eq!(encode(&RespValue::Bulk(payload)), b"$5\r\na\r\n\0b\r\n");
    }

    #[test]
    fn a07_encode_null() {
        assert_eq!(encode(&RespValue::Null), b"$-1\r\n");
    }

    #[test]
    fn a08_encode_flat_array() {
        let v = RespValue::Array(vec![
            RespValue::Bulk(b"foo".to_vec()),
            RespValue::Bulk(b"bar".to_vec()),
        ]);
        assert_eq!(encode(&v), b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn a09_encode_nested_array() {
        let v = RespValue::Array(vec![
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Null]),
            RespValue::Simple("hi".into()),
        ]);
        assert_eq!(encode(&v), b"*2\r\n*2\r\n:1\r\n$-1\r\n+hi\r\n");
    }
}
