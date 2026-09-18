//! 基础 KV 命令（docs/design.md §4.1）：PING ECHO SET GET DEL EXISTS KEYS TYPE FLUSHALL。
//!
//! 错误文案与语义严格对齐 design.md §4.1/§4.3：
//! - SET 覆盖任意已存在类型（与 Redis 一致）；value 二进制安全；
//! - GET 对向量索引 key 返回 WRONGTYPE；
//! - EXISTS 重复 key 重复计数；DEL 返回实际删除数；
//! - KEYS 仅支持 `*` 与 `?` 通配符，不支持 `[...]`；
//! - key 必须是合法 UTF-8（design.md D1 的已知简化）。

use std::sync::Arc;

use super::{arg_bytes, key_of, persist_failed, wrong_arg_count, wrong_type};
use crate::protocol::RespValue;
use crate::storage::{Db, Value};

/// PING [message]：无参 → `+PONG`；带一个参数 → 原样以 bulk 返回（与 Redis 一致）。
pub(crate) fn ping(args: &[RespValue]) -> RespValue {
    match args {
        [] => RespValue::Simple("PONG".into()),
        [only] => match arg_bytes(only) {
            Some(data) => RespValue::Bulk(data.to_vec()),
            None => wrong_arg_count("ping"),
        },
        _ => wrong_arg_count("ping"),
    }
}

/// ECHO message：恰一个参数，原样以 bulk 返回。
pub(crate) fn echo(args: &[RespValue]) -> RespValue {
    match args {
        [only] => match arg_bytes(only) {
            Some(data) => RespValue::Bulk(data.to_vec()),
            None => wrong_arg_count("echo"),
        },
        _ => wrong_arg_count("echo"),
    }
}

/// SET key value：覆盖任意已存在类型；value 为任意字节（二进制安全）。
pub(crate) fn set(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg, value_arg] = args else {
        return wrong_arg_count("set");
    };
    let key = match key_of(key_arg, "set") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    let data = match arg_bytes(value_arg) {
        Some(bytes) => bytes,
        None => return wrong_arg_count("set"),
    };
    // WAL 写失败 → 操作必须失败（design.md D11），内存未动
    if let Err(e) = db.set(key.to_string(), Value::Str(data.to_vec())) {
        return persist_failed(e);
    }
    RespValue::Simple("OK".into())
}

/// GET key：缺失 → nil；Str → bulk；向量索引 → WRONGTYPE。
pub(crate) fn get(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg] = args else {
        return wrong_arg_count("get");
    };
    let key = match key_of(key_arg, "get") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    match db.get(key) {
        None => RespValue::Null,
        Some(Value::Str(data)) => RespValue::Bulk(data),
        Some(Value::VectorIndex { .. }) => wrong_type(),
    }
}

/// DEL key [key ...]：返回实际删除数。先整体校验再删除，不做部分删除。
pub(crate) fn del(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let keys = match key_list(args, "del") {
        Ok(keys) => keys,
        Err(reply) => return reply,
    };
    match db.del(&keys) {
        Ok(count) => RespValue::Integer(count as i64),
        Err(e) => persist_failed(e),
    }
}

/// EXISTS key [key ...]：存在的数量；重复 key 重复计数（与 Redis 一致）。
pub(crate) fn exists(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let keys = match key_list(args, "exists") {
        Ok(keys) => keys,
        Err(reply) => return reply,
    };
    RespValue::Integer(db.exists_any(&keys) as i64)
}

/// 批量 key 参数转换：至少 1 个；任一非法即整批失败（先校验后执行）。
fn key_list(args: &[RespValue], cmd: &str) -> Result<Vec<String>, RespValue> {
    if args.is_empty() {
        return Err(wrong_arg_count(cmd));
    }
    args.iter().map(|a| key_of(a, cmd).map(str::to_string)).collect()
}

/// KEYS pattern：仅支持 `*` 与 `?`；返回顺序不保证（与 Redis 一致不排序）。
pub(crate) fn keys(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [pattern_arg] = args else {
        return wrong_arg_count("keys");
    };
    let pattern = match key_of(pattern_arg, "keys") {
        Ok(p) => p,
        Err(reply) => return reply,
    };
    let matched: Vec<RespValue> = db
        .keys()
        .into_iter()
        .filter(|k| glob_match(pattern, k))
        .map(|k| RespValue::Bulk(k.into_bytes()))
        .collect();
    RespValue::Array(matched)
}

/// KEYS 通配符匹配：`*` 匹配任意序列（含空），`?` 匹配恰一字符，不支持 `[...]`。
///
/// 采用双指针 + 单星回溯的教科书算法，刻意不用朴素递归——`*a*a*a*` 类模式
/// 会让递归匹配指数爆炸，挂死连接线程；本算法最坏 O(pattern × key)。
fn glob_match(pattern: &str, key: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let key: Vec<char> = key.chars().collect();
    let (mut pi, mut ki) = (0usize, 0usize);
    let mut star: Option<usize> = None; // 最近一个 '*' 在 pattern 中的下标
    let mut mark = 0usize; // 该 '*' 当前已匹配到的 key 位置
    while ki < key.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == key[ki]) {
            pi += 1;
            ki += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star = Some(pi);
            mark = ki;
            pi += 1; // 先让 '*' 匹配空序列
        } else if let Some(sp) = star {
            // 回溯：让最近的 '*' 多吃一个字符后重新对齐后续匹配
            pi = sp + 1;
            mark += 1;
            ki = mark;
        } else {
            return false;
        }
    }
    // key 耗尽：pattern 剩余部分必须全是 '*'
    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// TYPE key：`+string` / `+vector` / `+none`（vector 为本库扩展类型）。
pub(crate) fn type_of(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg] = args else {
        return wrong_arg_count("type");
    };
    let key = match key_of(key_arg, "type") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    match db.get(key) {
        Some(Value::Str(_)) => RespValue::Simple("string".into()),
        Some(Value::VectorIndex { .. }) => RespValue::Simple("vector".into()),
        None => RespValue::Simple("none".into()),
    }
}

/// FLUSHALL：清空全部数据。MVP 不接受任何参数（ASYNC/SYNC 选项不支持）。
pub(crate) fn flush_all(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    if !args.is_empty() {
        return wrong_arg_count("flushall");
    }
    match db.flush() {
        Ok(()) => RespValue::Simple("OK".into()),
        Err(e) => persist_failed(e),
    }
}

/// BGSAVE：触发一次快照并截断 WAL（同步实现，design.md §4.1 v0.4）。
pub(crate) fn bgsave(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    if !args.is_empty() {
        return wrong_arg_count("bgsave");
    }
    match db.bgsave() {
        Ok(()) => RespValue::Simple("OK".into()),
        Err(e) => persist_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::command::execute;
    use crate::vector::Metric;

    /// 把字符串数组构造为 RESP2 数组形式请求（测试辅助）。
    fn arr<const N: usize>(args: [&str; N]) -> Vec<RespValue> {
        args.iter()
            .map(|s| RespValue::Bulk(s.as_bytes().to_vec()))
            .collect()
    }

    /// 构造并预置数据的 Db（测试辅助）。
    fn db_with(entries: &[(&str, Value)]) -> Arc<Db> {
        let db = Arc::new(Db::new());
        for (k, v) in entries {
            db.set(k.to_string(), v.clone()).expect("setup: set");
        }
        db
    }

    fn vec_index(dim: usize) -> Value {
        Value::VectorIndex { dim, metric: Metric::Cosine, vectors: HashMap::new(), next_id: 0 }
    }

    /// 断言结果为 Array 且全部是 Bulk，排序后返回（KEYS 顺序不保证）。
    fn sorted_bulks(reply: RespValue) -> Vec<Vec<u8>> {
        match reply {
            RespValue::Array(items) => {
                let mut out: Vec<Vec<u8>> = items
                    .into_iter()
                    .map(|item| match item {
                        RespValue::Bulk(data) => data,
                        other => panic!("expected bulk, got {other:?}"),
                    })
                    .collect();
                out.sort();
                out
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    // —— PING / ECHO ——

    #[test]
    fn k01_ping_no_argument() {
        assert_eq!(ping(&[]), RespValue::Simple("PONG".into()));
    }

    #[test]
    fn k02_ping_with_argument() {
        assert_eq!(
            ping(&[RespValue::Bulk(b"hello".to_vec())]),
            RespValue::Bulk(b"hello".to_vec())
        );
    }

    #[test]
    fn k03_ping_too_many_arguments() {
        assert_eq!(
            ping(&[RespValue::Bulk(b"a".to_vec()), RespValue::Bulk(b"b".to_vec())]),
            wrong_arg_count("ping")
        );
    }

    #[test]
    fn k04_echo_ok() {
        assert_eq!(
            echo(&[RespValue::Bulk(b"hello world".to_vec())]),
            RespValue::Bulk(b"hello world".to_vec())
        );
    }

    #[test]
    fn k05_echo_missing_argument() {
        assert_eq!(echo(&[]), wrong_arg_count("echo"));
    }

    #[test]
    fn k06_echo_non_bulk_argument() {
        assert_eq!(echo(&[RespValue::Integer(1)]), wrong_arg_count("echo"));
    }

    // —— SET / GET ——

    #[test]
    fn e01_set_then_get_roundtrip() {
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["SET", "a", "hello"])), RespValue::Simple("OK".into()));
        assert_eq!(execute(&db, &arr(["GET", "a"])), RespValue::Bulk(b"hello".to_vec()));
    }

    #[test]
    fn e02_get_missing_is_nil() {
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["GET", "nope"])), RespValue::Null);
    }

    #[test]
    fn e03_get_wrongtype_on_vector_index() {
        let db = db_with(&[("v", vec_index(3))]);
        assert_eq!(execute(&db, &arr(["GET", "v"])), wrong_type());
    }

    #[test]
    fn e04_set_overwrites_vector_index() {
        // SET 覆盖任意类型：向量索引 key 被 SET 后变成 string
        let db = db_with(&[("v", vec_index(3))]);
        assert_eq!(execute(&db, &arr(["SET", "v", "now-a-string"])), RespValue::Simple("OK".into()));
        assert_eq!(execute(&db, &arr(["TYPE", "v"])), RespValue::Simple("string".into()));
    }

    #[test]
    fn e05_del_counts_existing() {
        let db = db_with(&[("a", Value::Str(b"1".to_vec())), ("b", Value::Str(b"2".to_vec()))]);
        assert_eq!(execute(&db, &arr(["DEL", "a", "x", "b"])), RespValue::Integer(2));
        assert_eq!(execute(&db, &arr(["GET", "a"])), RespValue::Null);
    }

    #[test]
    fn e06_del_all_missing_is_zero() {
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["DEL", "x", "y"])), RespValue::Integer(0));
    }

    #[test]
    fn e07_exists_counts_duplicates() {
        let db = db_with(&[("a", Value::Str(b"1".to_vec()))]);
        assert_eq!(execute(&db, &arr(["EXISTS", "a", "a", "x"])), RespValue::Integer(2));
    }

    #[test]
    fn e08_exists_none_is_zero() {
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["EXISTS", "x"])), RespValue::Integer(0));
    }

    // —— KEYS / TYPE / FLUSHALL ——

    #[test]
    fn e09_keys_star_matches_all() {
        let db = db_with(&[
            ("a1", Value::Str(b"1".to_vec())),
            ("a2", Value::Str(b"2".to_vec())),
            ("b1", Value::Str(b"3".to_vec())),
        ]);
        assert_eq!(sorted_bulks(execute(&db, &arr(["KEYS", "*"]))), vec![
            b"a1".to_vec(), b"a2".to_vec(), b"b1".to_vec()
        ]);
    }

    #[test]
    fn e10_keys_prefix_and_question_mark() {
        let db = db_with(&[
            ("a1", Value::Str(b"1".to_vec())),
            ("a2", Value::Str(b"2".to_vec())),
            ("abc", Value::Str(b"3".to_vec())),
            ("b1", Value::Str(b"4".to_vec())),
        ]);
        // `?` 恰好一个字符：不匹配 3 字符的 abc
        assert_eq!(sorted_bulks(execute(&db, &arr(["KEYS", "a?"]))), vec![
            b"a1".to_vec(), b"a2".to_vec()
        ]);
        // `*` 前缀匹配：包含 abc
        assert_eq!(sorted_bulks(execute(&db, &arr(["KEYS", "a*"]))), vec![
            b"a1".to_vec(), b"a2".to_vec(), b"abc".to_vec()
        ]);
    }

    #[test]
    fn e11_keys_no_match_is_empty_array() {
        let db = db_with(&[("a", Value::Str(b"1".to_vec()))]);
        assert_eq!(execute(&db, &arr(["KEYS", "zzz"])), RespValue::Array(vec![]));
    }

    #[test]
    fn e12_type_string() {
        let db = db_with(&[("a", Value::Str(b"1".to_vec()))]);
        assert_eq!(execute(&db, &arr(["TYPE", "a"])), RespValue::Simple("string".into()));
    }

    #[test]
    fn e13_type_vector() {
        let db = db_with(&[("v", vec_index(3))]);
        assert_eq!(execute(&db, &arr(["TYPE", "v"])), RespValue::Simple("vector".into()));
    }

    #[test]
    fn e14_type_none() {
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["TYPE", "nope"])), RespValue::Simple("none".into()));
    }

    #[test]
    fn e15_flushall_clears_everything() {
        let db = db_with(&[("a", Value::Str(b"1".to_vec())), ("v", vec_index(3))]);
        assert_eq!(execute(&db, &arr(["FLUSHALL"])), RespValue::Simple("OK".into()));
        assert_eq!(execute(&db, &arr(["GET", "a"])), RespValue::Null);
        assert_eq!(execute(&db, &arr(["TYPE", "v"])), RespValue::Simple("none".into()));
    }

    // —— 错误路径 ——

    #[test]
    fn e16_non_utf8_key_error() {
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"GET".to_vec()), RespValue::Bulk(b"\xff\xfe".to_vec())]),
            RespValue::Error("ERR key must be valid UTF-8".into())
        );
    }

    #[test]
    fn e17_arity_errors_for_all_commands() {
        let db = Arc::new(Db::new());
        // SET：1 参与 3 参都错
        assert_eq!(execute(&db, &arr(["SET", "a"])), wrong_arg_count("set"));
        assert_eq!(execute(&db, &arr(["SET", "a", "b", "c"])), wrong_arg_count("set"));
        // GET：0 参与多参
        assert_eq!(execute(&db, &arr(["GET"])), wrong_arg_count("get"));
        assert_eq!(execute(&db, &arr(["GET", "a", "b"])), wrong_arg_count("get"));
        // DEL / EXISTS：0 参
        assert_eq!(execute(&db, &arr(["DEL"])), wrong_arg_count("del"));
        assert_eq!(execute(&db, &arr(["EXISTS"])), wrong_arg_count("exists"));
        // KEYS / TYPE：0 参与多参
        assert_eq!(execute(&db, &arr(["KEYS"])), wrong_arg_count("keys"));
        assert_eq!(execute(&db, &arr(["KEYS", "*", "*"])), wrong_arg_count("keys"));
        assert_eq!(execute(&db, &arr(["TYPE"])), wrong_arg_count("type"));
        assert_eq!(execute(&db, &arr(["TYPE", "a", "b"])), wrong_arg_count("type"));
        // FLUSHALL：不接受参数
        assert_eq!(execute(&db, &arr(["FLUSHALL", "ASYNC"])), wrong_arg_count("flushall"));
    }

    #[test]
    fn e18_set_non_bulk_value() {
        // value 不是 Bulk（坏客户端防御路径）
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"SET".to_vec()), RespValue::Bulk(b"a".to_vec()), RespValue::Integer(1)]),
            wrong_arg_count("set")
        );
    }

    #[test]
    fn e19_set_get_binary_value() {
        // value 含 \r\n 与 \0：存储往返二进制安全
        let db = Arc::new(Db::new());
        let payload = b"a\r\n\0b".to_vec();
        assert_eq!(
            execute(&db, &[RespValue::Bulk(b"SET".to_vec()), RespValue::Bulk(b"a".to_vec()), RespValue::Bulk(payload.clone())]),
            RespValue::Simple("OK".into())
        );
        assert_eq!(execute(&db, &arr(["GET", "a"])), RespValue::Bulk(payload));
    }

    // —— BGSAVE（v0.4）——

    #[test]
    fn w10_bgsave_no_arg_is_ok() {
        // 纯内存（Off）模式：BGSAVE 是 no-op，仍返回 +OK
        let db = Arc::new(Db::new());
        assert_eq!(execute(&db, &arr(["BGSAVE"])), RespValue::Simple("OK".into()));
    }

    #[test]
    fn w11_bgsave_with_arg_is_arity_error() {
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &arr(["BGSAVE", "extra"])),
            wrong_arg_count("bgsave")
        );
    }

    // —— glob 匹配器直接测试（含回溯路径） ——

    #[test]
    fn g01_star_backtracking() {
        // 需要回溯的典型案例：'-' 段强制 '*' 多吃字符
        assert!(glob_match("*ab", "aaab"));
        assert!(glob_match("a*a*b", "a-x-a-y-b"));
        assert!(!glob_match("a*a*b", "a-x-y-b"));
    }

    #[test]
    fn g02_question_mark_exactly_one_char() {
        assert!(glob_match("?", "a"));
        assert!(!glob_match("?", ""));
        assert!(!glob_match("?", "ab"));
    }

    #[test]
    fn g03_empty_pattern_and_key() {
        assert!(glob_match("", ""));
        assert!(!glob_match("", "a"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
    }
}
