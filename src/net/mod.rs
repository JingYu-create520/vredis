//! 网络层：监听、accept 循环、每连接一线程。
//!
//! 拆成 bind / accept_loop / serve 三个函数的动机：bind 在调用线程同步完成并立即
//! 返回 listener（内核 backlog 已开始排队），集成测试因此可以先拿到实际端口、
//! 再后台启动 accept，连接建立无竞态（docs/design.md §1）。

mod connection;

use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::config::ServerConfig;
use crate::storage::Db;

/// 绑定 `config.host:config.port`。失败（含端口占用）原样返回 io::Error，
/// 由调用方（main / 测试）决定提示文案与退出行为。
pub fn bind(config: &ServerConfig) -> io::Result<TcpListener> {
    TcpListener::bind((config.host.as_str(), config.port))
}

/// 在已有 listener 上运行 accept 循环（阻塞，永不返回）。
/// 每个连接派生一个线程；任何单连接/单次 accept 的失败都不影响服务器整体。
pub fn accept_loop(listener: TcpListener, db: Arc<Db>) {
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                // 每个连接线程持有一份 Arc 克隆，共享同一个 Db
                let db = Arc::clone(&db);
                // 用 Builder 而非 thread::spawn：后者在线程创建失败时会 panic，
                // Builder 返回 Result，符合章程“任何情况不 panic”的要求
                let spawned = thread::Builder::new()
                    .name("vredis-conn".to_string())
                    .spawn(move || connection::handle(stream, &db));
                if let Err(e) = spawned {
                    // 线程资源耗尽等极端情况：放弃该连接（客户端表现为连接被拒），服务器继续运行
                    eprintln!("[vredis] 创建连接线程失败: {e}");
                }
            }
            Err(e) => {
                // accept 偶发失败（如临时 fd 耗尽）：记录、稍作退避后继续，
                // 避免持续失败时热旋刷屏；与 Redis“记录并继续”的行为一致
                eprintln!("[vredis] accept 失败: {e}");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// 便捷入口：bind + accept_loop（main 使用；阻塞直至进程结束）。
pub fn serve(config: &ServerConfig, db: Arc<Db>) -> io::Result<()> {
    let listener = bind(config)?;
    // 打印真实监听地址：测试场景 port 0 时能显示系统实际分配的端口
    match listener.local_addr() {
        Ok(addr) => println!("[vredis] 监听 {addr}，等待连接…"),
        Err(_) => println!("[vredis] 监听 {}:{}", config.host, config.port),
    }
    accept_loop(listener, db);
    Ok(())
}
