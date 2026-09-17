//! 向量命令（docs/design.md §4.2 v0.3）：VADD / VGET / VDIM / VSEARCH。

use std::sync::Arc;

use super::{arg_bytes, key_of, persist_failed, wrong_arg_count, wrong_type};
use crate::protocol::RespValue;
use crate::storage::{AddVectorError, Db, IndexProbe};
use crate::vector::{search, Metric, SearchError};

/// `-ERR value is not an integer or out of range`（Redis 文案，design.md §4.3）。
fn not_integer_error() -> RespValue {
    RespValue::Error("ERR value is not an integer or out of range".to_string())
}

/// `-ERR value is not a valid float`（design.md §4.3）。
fn float_error() -> RespValue {
    RespValue::Error("ERR value is not a valid float".to_string())
}

/// `-ERR invalid vector dimension`（design.md §4.3）。
fn dimension_error() -> RespValue {
    RespValue::Error("ERR invalid vector dimension".to_string())
}

/// `-ERR invalid metric`（design.md §4.3 v0.3 新增）。
fn metric_error() -> RespValue {
    RespValue::Error("ERR invalid metric".to_string())
}

/// 解析正整数参数（VADD 的 dim / VSEARCH 的 k）。
/// 非整数或为 0 → [`not_integer_error`]。
fn parse_usize_nonzero(value: &RespValue) -> Result<usize, RespValue> {
    match arg_bytes(value).and_then(|b| std::str::from_utf8(b).ok()) {
        Some(s) => match s.parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => Err(not_integer_error()),
        },
        None => Err(not_integer_error()),
    }
}

/// 解析浮点分量。非有限值（inf/nan）一律拒绝，避免毒化距离计算。
fn parse_f32(value: &RespValue) -> Option<f32> {
    arg_bytes(value)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|s| s.parse::<f32>().ok())
        .filter(|v| v.is_finite())
}

/// VADD key dim v1 ... vdim → 自动生成的向量 id（索引内自增十进制串，Bulk 返回）。
///
/// 索引不存在时由 storage 层创建并锁定维度；dim 与索引锁定维度不符 → 报错。
pub(crate) fn vadd(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg, dim_arg, rest @ ..] = args else {
        return wrong_arg_count("vadd");
    };
    let key = match key_of(key_arg, "vadd") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    let dim = match parse_usize_nonzero(dim_arg) {
        Ok(d) => d,
        Err(reply) => return reply,
    };
    // 声明维度必须与分量个数一致（design.md §4.2）
    if rest.len() != dim {
        return dimension_error();
    }
    let mut data = Vec::with_capacity(dim);
    for arg in rest {
        match parse_f32(arg) {
            Some(v) => data.push(v),
            None => return float_error(),
        }
    }
    match db.vector_add(key, data) {
        Ok(id) => RespValue::Bulk(id.into_bytes()),
        Err(AddVectorError::NotAnIndex) => wrong_type(),
        Err(AddVectorError::DimensionMismatch(_)) => dimension_error(),
        Err(AddVectorError::Persist(e)) => persist_failed(e),
    }
}

/// VGET key id → 浮点分量数组（bulk）或 nil。
pub(crate) fn vget(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg, id_arg] = args else {
        return wrong_arg_count("vget");
    };
    let key = match key_of(key_arg, "vget") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    // id 与 key 同为字符串参数，复用同一 UTF-8 校验
    let id = match key_of(id_arg, "vget") {
        Ok(i) => i,
        Err(reply) => return reply,
    };
    match db.get_vector(key, id) {
        Ok(Some(data)) => RespValue::Array(
            // 浮点输出用 Rust {} 最短表示（"1.0" → "1"，design.md §4.2 v0.3）
            data.into_iter().map(|v| RespValue::Bulk(v.to_string().into_bytes())).collect(),
        ),
        Ok(None) => RespValue::Null,
        Err(_) => wrong_type(),
    }
}

/// VDIM key → 索引维度（:N）或 nil。
pub(crate) fn vdim(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg] = args else {
        return wrong_arg_count("vdim");
    };
    let key = match key_of(key_arg, "vdim") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    match db.probe_index(key) {
        IndexProbe::Dim(dim) => RespValue::Integer(dim as i64),
        IndexProbe::Missing => RespValue::Null,
        IndexProbe::NotAnIndex => wrong_type(),
    }
}

/// VSEARCH key k v1 ... vdim [METRIC cos|l2|dot] → 扁平 [id, dist, ...] 升序 / nil。
///
/// ★ 参数解析顺序（用户确认的阶段 5 补充，防止 dim=1 时参数计数歧义）：
///   1) 先剥离可选的 METRIC 尾缀（末两个参数 = METRIC 关键字 + 度量名）；
///   2) 再校验剩余分量个数 == 索引维度（经 storage probe）；
///   3) 最后逐分量解析浮点。
///
///   若先查个数再剥尾缀，`VSEARCH ix 1 1.0 METRIC cos` 会被误判为 3 维查询。
pub(crate) fn vsearch(db: &Arc<Db>, args: &[RespValue]) -> RespValue {
    let [key_arg, k_arg, rest @ ..] = args else {
        return wrong_arg_count("vsearch");
    };
    let key = match key_of(key_arg, "vsearch") {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    let k = match parse_usize_nonzero(k_arg) {
        Ok(k) => k,
        Err(reply) => return reply,
    };
    // ── 1) 先剥离 METRIC 尾缀 ──
    let n = rest.len();
    let (components, metric) = if n >= 2 && is_metric_keyword(&rest[n - 2]) {
        let metric = match arg_bytes(&rest[n - 1]).and_then(Metric::parse) {
            Some(m) => m,
            None => return metric_error(),
        };
        (&rest[..n - 2], metric)
    } else {
        (rest, Metric::Cosine) // 默认 cos（design.md §4.2）
    };
    // ── 2) 校验分量个数 == 索引维度（经 storage probe，短临界区）──
    let expected_dim = match db.probe_index(key) {
        IndexProbe::Dim(dim) => dim,
        IndexProbe::Missing => return RespValue::Null,
        IndexProbe::NotAnIndex => return wrong_type(),
    };
    if components.len() != expected_dim {
        return dimension_error();
    }
    // ── 3) 逐分量解析浮点 ──
    let mut query = Vec::with_capacity(components.len());
    for arg in components {
        match parse_f32(arg) {
            Some(v) => query.push(v),
            None => return float_error(),
        }
    }
    // 搜索：遍历 + top-k 收集全部在 storage 持锁回调内完成，不克隆索引
    match search(db, key, &query, k, metric) {
        Ok(hits) => RespValue::Array(
            hits.into_iter()
                .flat_map(|h| {
                    [
                        RespValue::Bulk(h.id.into_bytes()),
                        RespValue::Bulk(h.dist.to_string().into_bytes()),
                    ]
                })
                .collect(),
        ),
        Err(SearchError::IndexMissing) => RespValue::Null, // probe 与遍历之间被删除的竞态
        Err(SearchError::WrongType) => wrong_type(),
        Err(SearchError::DimensionMismatch(_)) => dimension_error(),
    }
}

/// 判断参数是否为 METRIC 关键字（大小写不敏感）。
fn is_metric_keyword(value: &RespValue) -> bool {
    matches!(arg_bytes(value), Some(b) if b.eq_ignore_ascii_case(b"METRIC"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::execute;

    /// 构造并预置一个 3 维索引（含 2 条向量）的 Db（测试辅助）。
    fn db_with_index() -> Arc<Db> {
        let db = Arc::new(Db::new());
        let setup: &[&[&str]] = &[
            &["VADD", "ix", "3", "1.0", "0.0", "0.0"],
            &["VADD", "ix", "3", "0.0", "1.0", "0.0"],
        ];
        for cmd in setup {
            let req: Vec<RespValue> =
                cmd.iter().map(|s| RespValue::Bulk(s.as_bytes().to_vec())).collect();
            let reply = execute(&db, &req);
            assert!(matches!(reply, RespValue::Bulk(_)), "setup failed: {reply:?}");
        }
        db
    }

    fn arr(cmd: &[&str]) -> Vec<RespValue> {
        cmd.iter().map(|s| RespValue::Bulk(s.as_bytes().to_vec())).collect()
    }

    fn ids_of(reply: &RespValue) -> Vec<String> {
        match reply {
            RespValue::Array(items) => items
                .iter()
                .step_by(2)
                .map(|v| match v {
                    RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("expected bulk id, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn dists_of(reply: &RespValue) -> Vec<f64> {
        match reply {
            RespValue::Array(items) => items
                .iter()
                .skip(1)
                .step_by(2)
                .map(|v| match v {
                    RespValue::Bulk(b) => {
                        std::str::from_utf8(b).expect("utf8").parse::<f64>().expect("float")
                    }
                    other => panic!("expected bulk dist, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    // —— VADD（w01–w03）——

    #[test]
    fn w01_vadd_returns_auto_ids() {
        let db = db_with_index();
        assert_eq!(
            execute(&db, &arr(&["VADD", "ix", "3", "1.0", "1.0", "0.0"])),
            RespValue::Bulk(b"2".to_vec())
        );
    }

    #[test]
    fn w02_vadd_dimension_errors() {
        let db = db_with_index();
        // 声明维度与分量个数不一致
        assert_eq!(
            execute(&db, &arr(&["VADD", "ix", "2", "1.0", "0.0"])),
            dimension_error()
        );
        // 与索引锁定维度不一致
        assert_eq!(
            execute(&db, &arr(&["VADD", "ix", "4", "1.0", "0.0", "0.0", "0.0"])),
            dimension_error()
        );
    }

    #[test]
    fn w03_vadd_input_errors() {
        let db = db_with_index();
        // 参数过少 / dim 非整数 / 分量非浮点 / 分量为 inf / 字符串 key
        assert_eq!(execute(&db, &arr(&["VADD", "ix"])), wrong_arg_count("vadd"));
        assert_eq!(execute(&db, &arr(&["VADD", "ix", "abc", "1.0"])), not_integer_error());
        assert_eq!(execute(&db, &arr(&["VADD", "ix", "1", "xyz"])), float_error());
        assert_eq!(execute(&db, &arr(&["VADD", "ix", "1", "inf"])), float_error());
        // 字符串 key（先预置）→ WRONGTYPE
        db.set("s", crate::storage::Value::Str(b"x".to_vec())).expect("setup: set");
        assert_eq!(execute(&db, &arr(&["VADD", "s", "1", "1.0"])), wrong_type());
    }

    // —— VGET / VDIM（w04–w05）——

    #[test]
    fn w04_vget_states() {
        let db = db_with_index();
        // 1.0 → "1"（Rust {} 最短表示）
        assert_eq!(
            execute(&db, &arr(&["VGET", "ix", "0"])),
            RespValue::Array(vec![
                RespValue::Bulk(b"1".to_vec()),
                RespValue::Bulk(b"0".to_vec()),
                RespValue::Bulk(b"0".to_vec()),
            ])
        );
        assert_eq!(execute(&db, &arr(&["VGET", "ix", "99"])), RespValue::Null);
        assert_eq!(execute(&db, &arr(&["VGET", "nope", "0"])), RespValue::Null);
        db.set("s", crate::storage::Value::Str(b"x".to_vec())).expect("setup: set");
        assert_eq!(execute(&db, &arr(&["VGET", "s", "0"])), wrong_type());
        assert_eq!(execute(&db, &arr(&["VGET", "ix"])), wrong_arg_count("vget"));
    }

    #[test]
    fn w05_vdim_states() {
        let db = db_with_index();
        assert_eq!(execute(&db, &arr(&["VDIM", "ix"])), RespValue::Integer(3));
        assert_eq!(execute(&db, &arr(&["VDIM", "nope"])), RespValue::Null);
        db.set("s", crate::storage::Value::Str(b"x".to_vec())).expect("setup: set");
        assert_eq!(execute(&db, &arr(&["VDIM", "s"])), wrong_type());
        assert_eq!(execute(&db, &arr(&["VDIM"])), wrong_arg_count("vdim"));
    }

    // —— VSEARCH（w06–w09）——

    #[test]
    fn w06_vsearch_default_cosine() {
        let db = db_with_index();
        let reply = execute(&db, &arr(&["VSEARCH", "ix", "2", "1.0", "0.0", "0.0"]));
        assert_eq!(ids_of(&reply), vec!["0", "1"]);
        let dists = dists_of(&reply);
        assert!(dists[0].abs() < 1e-12);
        assert!((dists[1] - 1.0).abs() < 1e-12);
    }

    #[test]
    fn w07_vsearch_metric_keyword() {
        let db = db_with_index();
        // METRIC 大小写不敏感；l2 距离 id0=0, id1=√2
        let reply = execute(&db, &arr(&["VSEARCH", "ix", "2", "1.0", "0.0", "0.0", "METRIC", "l2"]));
        assert_eq!(ids_of(&reply), vec!["0", "1"]);
        let dists = dists_of(&reply);
        assert!(dists[0].abs() < 1e-12);
        assert!((dists[1] - 2.0f64.sqrt()).abs() < 1e-12);
        // 小写关键字
        let reply = execute(&db, &arr(&["VSEARCH", "ix", "1", "1.0", "0.0", "0.0", "metric", "COS"]));
        assert_eq!(ids_of(&reply), vec!["0"]);
        // 非法度量名
        assert_eq!(
            execute(&db, &arr(&["VSEARCH", "ix", "1", "1.0", "0.0", "0.0", "METRIC", "euclid"])),
            metric_error()
        );
    }

    #[test]
    fn w08_vsearch_error_states() {
        let db = db_with_index();
        // 索引缺失 → nil
        assert_eq!(execute(&db, &arr(&["VSEARCH", "nope", "1", "1.0", "0.0", "0.0"])), RespValue::Null);
        // 查询维度 ≠ 索引维度
        assert_eq!(
            execute(&db, &arr(&["VSEARCH", "ix", "1", "1.0", "0.0"])),
            dimension_error()
        );
        // 字符串 key → WRONGTYPE
        db.set("s", crate::storage::Value::Str(b"x".to_vec())).expect("setup: set");
        assert_eq!(execute(&db, &arr(&["VSEARCH", "s", "1", "1.0"])), wrong_type());
        // k=0 / k 非整数 / 参数过少
        assert_eq!(execute(&db, &arr(&["VSEARCH", "ix", "0", "1.0", "0.0", "0.0"])), not_integer_error());
        assert_eq!(execute(&db, &arr(&["VSEARCH", "ix", "x", "1.0"])), not_integer_error());
        assert_eq!(execute(&db, &arr(&["VSEARCH", "ix"])), wrong_arg_count("vsearch"));
    }

    #[test]
    fn w09_vsearch_dim1_metric_trailing_no_ambiguity() {
        // dim=1 歧义场景（用户确认的解析顺序）：先剥 METRIC 尾缀，再校验分量数
        let db = Arc::new(Db::new());
        assert_eq!(
            execute(&db, &arr(&["VADD", "one", "1", "1.0"])),
            RespValue::Bulk(b"0".to_vec())
        );
        let reply = execute(&db, &arr(&["VSEARCH", "one", "1", "1.0", "METRIC", "cos"]));
        assert_eq!(ids_of(&reply), vec!["0"]);
        // 剥完尾缀后分量数不匹配（0 ≠ 1）→ 维度错误而非参数数量错误
        assert_eq!(
            execute(&db, &arr(&["VSEARCH", "one", "1", "METRIC", "cos"])),
            dimension_error()
        );
    }
}
