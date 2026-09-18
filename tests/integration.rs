//! 真实 TCP 端到端集成测试：客户端 → 协议层 → 命令分发 → 协议层 → 客户端 全链路。
//!
//! 每个测试用 `bind(port: 0)` 起独立服务器（操作系统分配临时端口，不占用 6379），
//! `accept_loop` 跑在后台线程。bind 在调用线程同步完成后内核 backlog 已在排队，
//! 客户端立即可连接，无竞态。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use vredis::config::ServerConfig;
use vredis::net::{accept_loop, bind};
use vredis::persist::PersistConfig;
use vredis::protocol::{parse, ParseOutcome, RespValue};
use vredis::storage::{Db, Value};
use vredis::vector::Metric;

/// 起一个临时端口的服务器，返回实际端口；accept 循环在后台线程运行。
fn spawn_server() -> u16 {
    let config = ServerConfig { host: "127.0.0.1".to_string(), port: 0 };
    let listener = bind(&config).expect("bind on port 0 must succeed");
    let port = listener.local_addr().expect("local_addr").port();
    let db = Arc::new(Db::new());
    thread::spawn(move || accept_loop(listener, db));
    port
}

/// 起一个真实 TCP 服务器（“进程 B”）：Db::open_with(dir, hnsw_enabled) 恢复
/// + accept_loop，返回端口。
fn spawn_server_with_dir(dir: PathBuf, hnsw_enabled: bool) -> u16 {
    let (db, warnings) =
        Db::open_with(&PersistConfig { dir }, hnsw_enabled).expect("open with recovery");
    assert!(warnings.is_empty(), "unexpected recovery warnings: {warnings:?}");
    let listener = bind(&ServerConfig { host: "127.0.0.1".to_string(), port: 0 })
        .expect("bind on port 0 must succeed");
    let port = listener.local_addr().expect("local_addr").port();
    thread::spawn(move || accept_loop(listener, Arc::new(db)));
    port
}

/// 唯一临时数据目录（测试辅助；幂等清理）。
fn temp_data_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("vredis-e2e-{}-{tag}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp data dir");
    dir
}

/// 最小测试客户端：维护持久读缓冲，支持流水线场景下跨消息保留 leftover。
struct TestClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl TestClient {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        Self { stream, buf: Vec::new() }
    }

    fn send(&mut self, raw: &[u8]) {
        self.stream.write_all(raw).expect("send");
    }

    /// 读取并解析一条完整回复；数据不足时继续读 socket。
    fn read_reply(&mut self) -> RespValue {
        loop {
            match parse(&self.buf) {
                ParseOutcome::Complete(value, consumed) => {
                    self.buf.drain(..consumed);
                    return value;
                }
                ParseOutcome::Incomplete => {
                    let mut chunk = [0u8; 1024];
                    let n = self.stream.read(&mut chunk).expect("read reply");
                    assert!(n > 0, "connection closed before a complete reply");
                    self.buf.extend_from_slice(&chunk[..n]);
                }
                ParseOutcome::Error(e) => panic!("bad reply framing: {e}"),
            }
        }
    }

    /// 断言服务端已关闭连接（读到 EOF，0 字节）。
    fn expect_eof(&mut self) {
        let mut chunk = [0u8; 64];
        let n = self.stream.read(&mut chunk).expect("read for eof");
        assert_eq!(n, 0, "expected EOF, got {n} bytes: {:?}", &chunk[..n]);
    }
}

// —— 全链路测试 ——

#[test]
fn t01_ping_via_resp_array() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t02_ping_via_inline() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"PING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t03_ping_with_argument() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"hello".to_vec()));
}

#[test]
fn t04_echo() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"hello world".to_vec()));
}

#[test]
fn t05_command_error_keeps_connection_alive() {
    // ECHO 缺参报错后，连接必须仍然可用（章程：命令级错误不断连）
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$4\r\nECHO\r\n");
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR wrong number of arguments for 'echo' command".into())
    );
    c.send(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t06_unknown_command() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$3\r\nFOO\r\n");
    assert_eq!(c.read_reply(), RespValue::Error("ERR unknown command 'FOO'".into()));
}

#[test]
fn t07_lowercase_command() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$4\r\nping\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t08_pipelined_commands_in_order() {
    // 单次 write 发送两条命令：服务端按序回复两条（验证 leftover 缓冲处理）
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nok\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"ok".to_vec()));
}

#[test]
fn t09_binary_payload_roundtrip() {
    // 载荷含 \r\n 与 \0：端到端二进制安全
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$4\r\nECHO\r\n$5\r\na\r\n\0b\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"a\r\n\0b".to_vec()));
}

#[test]
fn t10_protocol_error_reply_then_close() {
    // 协议损坏：先收到 Protocol error 回复，随后服务端主动断开（EOF）
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"$-2\r\n"); // 非法 bulk 长度
    match c.read_reply() {
        RespValue::Error(msg) => assert!(msg.contains("Protocol error"), "got: {msg}"),
        other => panic!("expected error reply, got {other:?}"),
    }
    c.expect_eof();
}

#[test]
fn t11_empty_inline_line_is_ignored() {
    // 空行不产生回复：收到的第一条回复就是 PONG
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"\r\nPING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t12_concurrent_connections() {
    // 两条并发连接各自独立服务（每连接一线程）
    let port = spawn_server();
    let mut a = TestClient::connect(port);
    let mut b = TestClient::connect(port);
    a.send(b"*1\r\n$4\r\nPING\r\n");
    b.send(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(a.read_reply(), RespValue::Simple("PONG".into()));
    assert_eq!(b.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t13_client_drop_then_server_still_serves() {
    // 客户端突然断开：服务端线程静默退出，服务器继续接受新连接
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"PING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
    drop(c);
    let mut c2 = TestClient::connect(port);
    c2.send(b"PING\r\n");
    assert_eq!(c2.read_reply(), RespValue::Simple("PONG".into()));
}

// —— 存储层 KV 命令全链路测试（阶段 4）——

#[test]
fn t14_set_get_roundtrip() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nhello\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"hello".to_vec()));
}

#[test]
fn t15_get_missing_is_nil() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n");
    assert_eq!(c.read_reply(), RespValue::Null);
}

#[test]
fn t16_del_counts_existing() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$4\r\nnope\r\n");
    assert_eq!(c.read_reply(), RespValue::Integer(1));
    c.send(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert_eq!(c.read_reply(), RespValue::Null);
}

#[test]
fn t17_del_missing_is_zero() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nDEL\r\n$5\r\nnope1\r\n$5\r\nnope2\r\n");
    assert_eq!(c.read_reply(), RespValue::Integer(0));
}

#[test]
fn t18_exists_counts_duplicates() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    // a 存在 ×2 + nope 不存在 → 2（重复 key 重复计数，与 Redis 一致）
    c.send(b"*4\r\n$6\r\nEXISTS\r\n$1\r\na\r\n$1\r\na\r\n$4\r\nnope\r\n");
    assert_eq!(c.read_reply(), RespValue::Integer(2));
}

#[test]
fn t19_keys_pattern_sorted() {
    // KEYS 返回顺序不保证：结果排序后与期望比较
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    for key in ["a1", "a2", "b1"] {
        c.send(format!("*3\r\n$3\r\nSET\r\n$2\r\n{key}\r\n$1\r\nv\r\n").as_bytes());
        assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    }
    c.send(b"*2\r\n$4\r\nKEYS\r\n$2\r\na*\r\n");
    match c.read_reply() {
        RespValue::Array(items) => {
            let mut got: Vec<Vec<u8>> = items
                .into_iter()
                .map(|v| match v {
                    RespValue::Bulk(data) => data,
                    other => panic!("expected bulk, got {other:?}"),
                })
                .collect();
            got.sort();
            assert_eq!(got, vec![b"a1".to_vec(), b"a2".to_vec()]);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn t20_type_string_and_none() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*2\r\n$4\r\nTYPE\r\n$1\r\na\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("string".into()));
    c.send(b"*2\r\n$4\r\nTYPE\r\n$4\r\nnope\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("none".into()));
}

#[test]
fn t21_flushall_clears_everything() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*1\r\n$8\r\nFLUSHALL\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert_eq!(c.read_reply(), RespValue::Null);
}

#[test]
fn t22_wrong_arity_keeps_connection_alive() {
    // 存储命令参数错误：报错后连接必须仍然可用
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$3\r\nSET\r\n$1\r\na\r\n");
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR wrong number of arguments for 'set' command".into())
    );
    c.send(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t23_non_utf8_key_error() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*2\r\n$3\r\nGET\r\n$2\r\n\xff\xfe\r\n");
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR key must be valid UTF-8".into())
    );
}

#[test]
fn t24_binary_value_roundtrip() {
    // value 含 \r\n 与 \0：TCP 全链路二进制安全
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\na\r\n\0b\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    c.send(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"a\r\n\0b".to_vec()));
}

#[test]
fn t25_pipelined_set_get() {
    // 单次 write 发送 SET+GET 两条命令，按序收到两条回复
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$2\r\nok\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("OK".into()));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"ok".to_vec()));
}

#[test]
fn t26_regression_ping_and_unknown() {
    // 存储命令接入后的回归冒烟：PING 与 unknown command 行为不变
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
    c.send(b"*1\r\n$3\r\nFOO\r\n");
    assert_eq!(c.read_reply(), RespValue::Error("ERR unknown command 'FOO'".into()));
}

// —— 向量命令全链路测试（阶段 5）——

/// 把参数构造为 RESP2 数组报文（测试辅助；参数须为 UTF-8 字符串）。
fn resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// 断言回复为扁平 [id, dist, ...] 数组，返回 (ids, dists)。
fn flat_search_reply(reply: RespValue) -> (Vec<String>, Vec<f64>) {
    match reply {
        RespValue::Array(items) => {
            let mut ids = Vec::new();
            let mut dists = Vec::new();
            for (i, item) in items.into_iter().enumerate() {
                let s = match item {
                    RespValue::Bulk(b) => String::from_utf8(b).expect("utf8"),
                    other => panic!("expected bulk, got {other:?}"),
                };
                if i % 2 == 0 {
                    ids.push(s);
                } else {
                    dists.push(s.parse::<f64>().expect("float dist"));
                }
            }
            (ids, dists)
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn t27_vadd_returns_auto_ids() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["VADD", "ix", "3", "0.0", "1.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"1".to_vec()));
}

#[test]
fn t28_vget_and_missing_id() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["VGET", "ix", "0"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Array(vec![
            RespValue::Bulk(b"1".to_vec()),
            RespValue::Bulk(b"0".to_vec()),
            RespValue::Bulk(b"0".to_vec()),
        ])
    );
    // id 不存在 → nil
    c.send(&resp(&["VGET", "ix", "99"]));
    assert_eq!(c.read_reply(), RespValue::Null);
}

#[test]
fn t29_vdim_states() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["VDIM", "ix"]));
    assert_eq!(c.read_reply(), RespValue::Integer(3));
    // 索引不存在 → nil
    c.send(&resp(&["VDIM", "nope"]));
    assert_eq!(c.read_reply(), RespValue::Null);
}

#[test]
fn t30_vsearch_default_cosine() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["VADD", "ix", "3", "0.0", "1.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"1".to_vec()));
    // k=1 → 扁平 [id, dist] 两个元素；同向距离 0（输出 "0"）
    c.send(&resp(&["VSEARCH", "ix", "1", "1.0", "0.0", "0.0"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0"]);
    assert!(dists[0].abs() < 1e-12);
}

#[test]
fn t31_vsearch_metric_l2_and_dot() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["VADD", "ix", "3", "0.0", "1.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"1".to_vec()));
    // l2：id0=0，id1=√2；METRIC 关键字大小写不敏感
    c.send(&resp(&["VSEARCH", "ix", "2", "1.0", "0.0", "0.0", "METRIC", "l2"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0", "1"]);
    assert!(dists[0].abs() < 1e-12);
    assert!((dists[1] - 2.0f64.sqrt()).abs() < 1e-12);
    // dot：id0=-1，id1=-0 → 升序 [0, 1]
    c.send(&resp(&["VSEARCH", "ix", "2", "1.0", "0.0", "0.0", "metric", "DOT"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0", "1"]);
    assert!((dists[0] + 1.0).abs() < 1e-12);
    assert!(dists[1].abs() < 1e-12);
}

#[test]
fn t32_vsearch_error_paths() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    // 索引缺失 → nil
    c.send(&resp(&["VSEARCH", "nope", "1", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Null);
    // 查询维度 ≠ 索引维度
    c.send(&resp(&["VSEARCH", "ix", "1", "1.0", "0.0"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR invalid vector dimension".into())
    );
    // 非法度量名
    c.send(&resp(&["VSEARCH", "ix", "1", "1.0", "0.0", "0.0", "METRIC", "euclid"]));
    assert_eq!(c.read_reply(), RespValue::Error("ERR invalid metric".into()));
    // k=0 → 越界
    c.send(&resp(&["VSEARCH", "ix", "0", "1.0", "0.0", "0.0"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR value is not an integer or out of range".into())
    );
    // 错误后连接保持可用
    c.send(&resp(&["PING"]));
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

#[test]
fn t33_vadd_dimension_lock() {
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    // 维度锁定后：dim=2 被拒绝
    c.send(&resp(&["VADD", "ix", "2", "1.0", "0.0"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR invalid vector dimension".into())
    );
    c.send(&resp(&["VDIM", "ix"]));
    assert_eq!(c.read_reply(), RespValue::Integer(3));
}

#[test]
fn t34_wrongtype_and_type_after_vadd() {
    // 阶段 4 推迟的端到端用例：VADD 后 GET → WRONGTYPE，TYPE → +vector
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(&resp(&["GET", "ix"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
    );
    c.send(&resp(&["TYPE", "ix"]));
    assert_eq!(c.read_reply(), RespValue::Simple("vector".into()));
}

#[test]
fn t35_inline_vector_commands() {
    // inline 形式的向量命令全链路
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(b"VADD ix 3 1.0 0.0 0.0\r\n");
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    c.send(b"VSEARCH ix 1 1.0 0.0 0.0\r\n");
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0"]);
    assert!(dists[0].abs() < 1e-12);
}

#[test]
fn t36_pipelined_vadd_vsearch() {
    // 单次 write 管道 VADD+VSEARCH：按序收到 id 与搜索结果
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    let mut raw = resp(&["VADD", "ix", "3", "1.0", "0.0", "0.0"]);
    raw.extend_from_slice(&resp(&["VSEARCH", "ix", "1", "1.0", "0.0", "0.0"]));
    c.send(&raw);
    assert_eq!(c.read_reply(), RespValue::Bulk(b"0".to_vec()));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0"]);
    assert!(dists[0].abs() < 1e-12);
}

// —— 持久化全链路测试（进阶 1 v0.4）——

#[test]
fn t37_restart_persistence_full_tcp_chain() {
    // 进程 A 模拟（纯文件层）：Db::open → SET / VADD → drop 释放 WAL 句柄
    let dir = temp_data_dir("t37");
    {
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open A");
        assert!(warnings.is_empty());
        db.set("a", Value::Str(b"hello".to_vec())).expect("set a");
        assert_eq!(db.vector_add("ix", vec![1.0, 0.0]).expect("add"), "0");
        assert_eq!(db.vector_add("ix", vec![0.0, 1.0]).expect("add"), "1");
    } // drop：释放 WAL 句柄，模拟进程退出
    // 进程 B：真实 TCP 服务器（Db::open_with 恢复 + accept_loop），通过连接验证数据
    let port = spawn_server_with_dir(dir, false);
    let mut c = TestClient::connect(port);
    // SET 的 key 拿到了
    c.send(&resp(&["GET", "a"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"hello".to_vec()));
    // VADD 的 id 拿到了
    c.send(&resp(&["VGET", "ix", "0"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Array(vec![
            RespValue::Bulk(b"1".to_vec()),
            RespValue::Bulk(b"0".to_vec()),
        ])
    );
    // next_id 续号正确：重启后新 VADD 拿 "2"
    c.send(&resp(&["VADD", "ix", "2", "1.0", "1.0"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"2".to_vec()));
}

#[test]
fn t38_bgsave_recovers_snapshot_plus_wal() {
    // 进程 A 模拟：SET x → BGSAVE（x 进快照、WAL 截断）→ SET y（y 只在 WAL）→ drop
    let dir = temp_data_dir("t38");
    {
        let (db, warnings) = Db::open(&PersistConfig { dir: dir.clone() }).expect("open A");
        assert!(warnings.is_empty());
        db.set("x", Value::Str(b"1".to_vec())).expect("set x");
        db.bgsave().expect("bgsave");
        db.set("y", Value::Str(b"2".to_vec())).expect("set y");
    }
    // 进程 B：真实 TCP 服务器（快照 + WAL 组合恢复）
    let port = spawn_server_with_dir(dir, false);
    let mut c = TestClient::connect(port);
    c.send(&resp(&["GET", "x"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"1".to_vec())); // 来自快照
    c.send(&resp(&["GET", "y"]));
    assert_eq!(c.read_reply(), RespValue::Bulk(b"2".to_vec())); // 来自 WAL 重放
}

#[test]
fn t39_bgsave_arity_error() {
    // BGSAVE 带参数：参数校验先于持久化，纯内存服务器即可验证；错误后连接保持
    let port = spawn_server();
    let mut c = TestClient::connect(port);
    c.send(&resp(&["BGSAVE", "extra"]));
    assert_eq!(
        c.read_reply(),
        RespValue::Error("ERR wrong number of arguments for 'bgsave' command".into())
    );
    c.send(&resp(&["PING"]));
    assert_eq!(c.read_reply(), RespValue::Simple("PONG".into()));
}

// —— HNSW 集成测试（进阶 3 第 4 步）——

#[test]
fn t40_vsearch_hnsw_path_correct() {
    // HNSW 启用 + metric 匹配（l2 索引 + METRIC l2）→ VSEARCH 走 HNSW 路径，
    // 结果与暴力一致（同 h26 数据：l2 top-2 = A, C）
    let dir = temp_data_dir("t40");
    {
        let (db, _) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("open A");
        assert_eq!(
            db.vector_add_with_metric("ix", vec![1.0, 0.0], Metric::L2).expect("add"),
            "0"
        );
        assert_eq!(
            db.vector_add_with_metric("ix", vec![10.0, 0.0], Metric::L2).expect("add"),
            "1"
        );
        assert_eq!(
            db.vector_add_with_metric("ix", vec![0.5, 0.5], Metric::L2).expect("add"),
            "2"
        );
    }
    let port = spawn_server_with_dir(dir, true);
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VSEARCH", "ix", "2", "1.0", "0.0", "METRIC", "l2"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0", "2"]);
    assert!(dists[0].abs() < 1e-12);
    assert!((dists[1] - 0.5f64.sqrt()).abs() < 1e-12);
}

#[test]
fn t41_vsearch_metric_mismatch_falls_back_to_brute() {
    // l2 索引 + 查询 METRIC cos → metric 不匹配 → 暴力路径（cos 语义 top-2 = A, B），
    // 绝不使用错 metric 的 HNSW 图（design.md 补充 3）。
    // 注：B 取 [1, 0.2] 而非与 A 同向——并列距离（同为 0）的堆弹出顺序未定义，
    // 测试数据必须避免并列，否则断言不稳定
    let dir = temp_data_dir("t41");
    {
        let (db, _) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("open A");
        assert_eq!(
            db.vector_add_with_metric("ix", vec![1.0, 0.0], Metric::L2).expect("add"),
            "0"
        );
        assert_eq!(
            db.vector_add_with_metric("ix", vec![1.0, 0.2], Metric::L2).expect("add"),
            "1"
        );
        assert_eq!(
            db.vector_add_with_metric("ix", vec![0.5, 0.5], Metric::L2).expect("add"),
            "2"
        );
    }
    let port = spawn_server_with_dir(dir, true);
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VSEARCH", "ix", "2", "1.0", "0.0", "METRIC", "cos"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0", "1"]);
    assert!(dists[0].abs() < 1e-12);
    assert!(dists[1] > 0.0 && dists[1] < 0.1); // B 的 cos 距离 ≈ 0.0194
}

#[test]
fn t42_hnsw_rebuilt_after_restart() {
    // 进程 A（HNSW 启用，文件层写入）→ drop → 进程 B（HNSW 启用，真实 TCP）：
    // 重启后索引重建，VSEARCH 走 HNSW 路径结果正确
    let dir = temp_data_dir("t42");
    {
        let (db, warnings) = Db::open_with(&PersistConfig { dir: dir.clone() }, true).expect("open A");
        assert!(warnings.is_empty());
        assert_eq!(
            db.vector_add_with_metric("ix", vec![1.0, 0.0], Metric::L2).expect("add"),
            "0"
        );
        assert_eq!(
            db.vector_add_with_metric("ix", vec![0.0, 1.0], Metric::L2).expect("add"),
            "1"
        );
    } // drop：模拟进程退出
    let port = spawn_server_with_dir(dir, true);
    let mut c = TestClient::connect(port);
    c.send(&resp(&["VSEARCH", "ix", "2", "1.0", "0.0", "METRIC", "l2"]));
    let (ids, dists) = flat_search_reply(c.read_reply());
    assert_eq!(ids, vec!["0", "1"]);
    assert!(dists[0].abs() < 1e-12);
    assert!((dists[1] - 2.0f64.sqrt()).abs() < 1e-12);
}
