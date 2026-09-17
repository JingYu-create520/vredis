//! 命令分发层：命令名 → 处理函数的静态命令表、参数校验与 Redis 风格错误文案。
//!
//! 分层约束（docs/design.md §1）：本层只依赖 protocol / storage（阶段 5 起再加 vector），
//! 不感知 TCP。`execute` 持有存储层的共享句柄，阶段 5 的向量命令同样经它访问存储。

mod kv;
mod vector;

use std::sync::Arc;

use crate::persist::PersistError;
use crate::protocol::RespValue;
use crate::storage::Db;

/// 统一入口：执行一条已解析的命令请求。
///
/// `req[0]` 必须是命令名（Bulk），按 Redis 语义大小写不敏感。
/// 调用方（connection 层）保证空请求不会到达这里；仍做防御处理。
pub fn execute(db: &Arc<Db>, req: &[RespValue]) -> RespValue {
    let name = match req.first().and_then(arg_bytes) {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => return unknown_command(""),
    };
    match name.to_ascii_uppercase().as_str() {
        "PING" => kv::ping(&req[1..]),
        "ECHO" => kv::echo(&req[1..]),
        "SET" => kv::set(db, &req[1..]),
        "GET" => kv::get(db, &req[1..]),
        "DEL" => kv::del(db, &req[1..]),
        "EXISTS" => kv::exists(db, &req[1..]),
        "KEYS" => kv::keys(db, &req[1..]),
        "TYPE" => kv::type_of(db, &req[1..]),
        "FLUSHALL" => kv::flush_all(db, &req[1..]),
        "BGSAVE" => kv::bgsave(db, &req[1..]),
        "VADD" => vector::vadd(db, &req[1..]),
        "VGET" => vector::vget(db, &req[1..]),
        "VDIM" => vector::vdim(db, &req[1..]),
        "VSEARCH" => vector::vsearch(db, &req[1..]),
        _ => unknown_command(&name),
    }
}

/// 取参数的原始字节。只有 Bulk 携带参数值（RESP2 数组与 inline 解析的产物都是 Bulk），
/// 非 Bulk 参数属坏客户端，按缺失处理走参数错误路径。
pub(crate) fn arg_bytes(value: &RespValue) -> Option<&[u8]> {
    match value {
        RespValue::Bulk(data) => Some(data),
        _ => None,
    }
}

/// `-ERR unknown command 'FOO'`（design.md §4.3 的 MVP 简化文案）。
pub(crate) fn unknown_command(name: &str) -> RespValue {
    RespValue::Error(format!("ERR unknown command '{name}'"))
}

/// 取 key 参数并转 `&str`（storage 层 key 为 String，见 design.md D1）：
/// Bulk + 合法 UTF-8 → Ok；Bulk 非 UTF-8 → `ERR key must be valid UTF-8`（§4.3）；
/// 非 Bulk → 按参数数量错误处理（坏客户端防御路径）。
pub(crate) fn key_of<'a>(value: &'a RespValue, cmd: &str) -> Result<&'a str, RespValue> {
    match value {
        RespValue::Bulk(data) => std::str::from_utf8(data)
            .map_err(|_| RespValue::Error("ERR key must be valid UTF-8".to_string())),
        _ => Err(wrong_arg_count(cmd)),
    }
}

/// `-ERR wrong number of arguments for 'ping' command`（命令名小写，与 Redis 一致）。
pub(crate) fn wrong_arg_count(cmd: &str) -> RespValue {
    RespValue::Error(format!("ERR wrong number of arguments for '{cmd}' command"))
}

/// `-WRONGTYPE ...`：对持有错误类型值的 key 操作（与 Redis 文案一致，§4.3）。
pub(crate) fn wrong_type() -> RespValue {
    RespValue::Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
    )
}

/// `-ERR persist failed: <reason>`（design.md §4.3 v0.4：WAL/快照失败时操作必须失败）。
pub(crate) fn persist_failed(e: PersistError) -> RespValue {
    RespValue::Error(format!("ERR persist failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // —— 分发层单元测试（签名重构后均构造独立 Db） ——

    #[test]
    fn d01_execute_ping_array_form() {
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"PING".to_vec())]),
            RespValue::Simple("PONG".into())
        );
    }

    #[test]
    fn d02_unknown_command() {
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"FOO".to_vec())]),
            RespValue::Error("ERR unknown command 'FOO'".into())
        );
    }

    #[test]
    fn d03_case_insensitive_dispatch() {
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"eChO".to_vec()), RespValue::Bulk(b"hi".to_vec())]),
            RespValue::Bulk(b"hi".to_vec())
        );
    }

    #[test]
    fn d04_empty_request_defensive() {
        // 正常情况下 connection 层会跳过空请求，这里只做防御
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[]),
            RespValue::Error("ERR unknown command ''".into())
        );
    }

    #[test]
    fn d05_non_bulk_command_name() {
        // 命令名不是 Bulk（如顶层 Simple）：按未知命令处理
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Simple("PING".into())]),
            RespValue::Error("ERR unknown command ''".into())
        );
    }
}
