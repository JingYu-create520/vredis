//! vredis 服务器入口。
//!
//! 端口约定（docs/design.md §7）：默认监听 127.0.0.1:6379；端口被占用时打印
//! 明确提示并以非零退出码退出，绝不自动更换端口。MVP 无命令行参数。

use std::sync::Arc;

use vredis::{config::ServerConfig, net, storage::Db};

fn main() {
    let config = ServerConfig::default();
    let db = Arc::new(Db::new());
    if let Err(e) = net::serve(&config, db) {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            eprintln!(
                "[vredis] 端口 {} 已被占用，可能已有 Redis 或另一个 vredis 实例在运行。",
                config.port
            );
            eprintln!("[vredis] 请释放该端口，或明确告知要使用的其他端口；程序不会自动更换端口。");
        } else {
            eprintln!("[vredis] 启动失败: {e}");
        }
        std::process::exit(1);
    }
}
