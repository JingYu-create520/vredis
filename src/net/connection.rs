//! 单连接生命周期：读 → 解析 → 分发 → 编码 → 写。
//!
//! 退出条件（全部为正常收尾，不 panic）：
//! - 对端关闭（read 返回 0）或 IO 错误；
//! - 协议损坏：先回 `-ERR Protocol error: ...` 再断开（与 Redis 行为一致）；
//! - 输入缓冲超过 MAX_QUERY_BUF：先回错误再断开（OOM 防护）。
//!
//! 二进制安全关键点：**绝不使用 read_line / BufReader::read_line**——
//! bulk payload 内含 `\n`，按行读取会破坏协议；只做裸 read 追加字节，
//! 由 parser 按声明长度切分（这正是阶段 2 流式 parser 接口的设计目的）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use crate::command;
use crate::protocol::{encode, parse, ParseOutcome, RespValue};
use crate::storage::Db;

/// 单连接输入缓冲上限，对齐 Redis `client-query-buffer` 默认值（1GB）。
///
/// 这是 parser 防护在服务侧的必要补位：parser 对超长 bulk 必须等字节到齐才能拒绝，
/// 而 inline 行（如 10GB 的 `PING PING ...`）在协议层完全没有长度上限，
/// 无此检查时服务器内存会被恶意客户端撑爆。
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

/// 处理一条已建立连接，直到连接结束（阻塞）。
pub fn handle(mut stream: TcpStream, db: &Arc<Db>) {
    // 尽力关闭 Nagle 以降低小响应延迟；失败不影响服务
    let _ = stream.set_nodelay(true);

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        // ── 第 1 步：消化缓冲区内所有完整消息 ──
        loop {
            match parse(&buf) {
                ParseOutcome::Complete(value, consumed) => {
                    // 只移除已消费字节，剩余部分属于后续消息
                    buf.drain(..consumed);
                    // inline 空行 → 空数组：与 Redis 一致，不回复，继续读下一条
                    if matches!(&value, RespValue::Array(items) if items.is_empty()) {
                        continue;
                    }
                    let req = match value {
                        // 正常请求：RESP2 数组（inline 命令也被解析成数组）
                        RespValue::Array(items) => items,
                        // 坏客户端发来顶层标量（如 +OK\r\n）：按空请求处理，回未知命令错误
                        _ => Vec::new(),
                    };
                    let reply = command::execute(db, &req);
                    if stream.write_all(&encode(&reply)).is_err() {
                        return; // 写失败：对端异常，结束该连接
                    }
                }
                ParseOutcome::Incomplete => break, // 数据不足，去读 socket
                ParseOutcome::Error(reason) => {
                    // 协议损坏：回错误后断开连接（与 Redis 行为一致）
                    let reply = RespValue::Error(format!("ERR Protocol error: {reason}"));
                    let _ = stream.write_all(&encode(&reply));
                    return;
                }
            }
        }
        // ── 第 2 步：缓冲区无完整消息，阻塞读更多数据 ──
        match stream.read(&mut chunk) {
            Ok(0) => return, // 对端正常关闭
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            // 信号中断不是错误，重试；其他 IO 错误：结束该连接，不影响服务器
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return,
        }
        // ── 第 3 步：【MAX_QUERY_BUF 检查点】──
        // 位置约定（用户确认的第三阶段计划补充）：每次 stream.read 返回并追加
        // 缓冲之后、回到循环顶部调用 parse 之前。超限 → 回固定错误并断开连接，
        // 防止无长度上限的 inline 行把服务器内存撑爆。
        if buf.len() > MAX_QUERY_BUF {
            let reply =
                RespValue::Error("ERR Protocol error: query buffer limit exceeded".to_string());
            let _ = stream.write_all(&encode(&reply));
            return;
        }
    }
}
