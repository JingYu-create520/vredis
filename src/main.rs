//! vredis 服务器入口。
//!
//! 端口约定（docs/design.md §7）：默认监听 127.0.0.1:6379；端口被占用时打印
//! 明确提示并以非零退出码退出，绝不自动更换端口。MVP 无命令行参数。
//!
//! 启动恢复（docs/design.md §3.1 v0.4）：加载快照 + 重放 WAL；
//! 快照或 WAL 真损坏时拒绝启动（不静默丢数据）。

use std::sync::Arc;

use vredis::{config::ServerConfig, net, persist::PersistConfig, storage::Db};

fn main() {
    let config = ServerConfig::default();
    let persist = PersistConfig::default();
    // 启动恢复：快照 + WAL 重放；撕裂尾警告打印到 stderr，真损坏 → 拒绝启动
    let db = match Db::open(&persist) {
        Ok((db, warnings)) => {
            for warning in warnings {
                eprintln!("[vredis] 警告: {warning}");
            }
            println!("[vredis] 数据目录: {}", persist.dir.display());
            Arc::new(db)
        }
        Err(e) => {
            eprintln!("[vredis] 数据恢复失败: {e}");
            eprintln!("[vredis] 为避免静默丢数据，拒绝启动。请检查数据目录后重试。");
            std::process::exit(1);
        }
    };
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
