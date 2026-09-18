//! vredis 服务器入口。
//!
//! 端口约定（docs/design.md §7）：默认监听 127.0.0.1:6379；端口被占用时打印
//! 明确提示并以非零退出码退出，绝不自动更换端口。MVP 无命令行参数。
//!
//! 启动恢复（docs/design.md §3.1 v0.4）：加载快照 + 重放 WAL；
//! 快照或 WAL 真损坏时拒绝启动（不静默丢数据）。

use std::sync::Arc;

use vredis::{config::ServerConfig, net, persist::PersistConfig, persist::PersistError, storage::Db};

fn main() {
    let config = ServerConfig::default();
    let persist = PersistConfig::default();
    // HNSW 开关（进阶 3 第 4 步）：仅 "1" 或 "true"（大小写不敏感）视为启用；
    // 其他一切值（"0"、"false"、"yes"、随机字符串、未设置）一律视为未启用，不报错。
    let hnsw_enabled = match std::env::var("VREDIS_HNSW") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    };
    // 启动恢复：快照 + WAL 重放；撕裂尾警告打印到 stderr，真损坏 → 拒绝启动
    let db = match Db::open_with(&persist, hnsw_enabled) {
        Ok((db, warnings)) => {
            for warning in warnings {
                eprintln!("[vredis] 警告: {warning}");
            }
            println!("[vredis] 数据目录: {}", persist.dir.display());
            println!("[vredis] HNSW: {}", if hnsw_enabled { "enabled" } else { "disabled" });
            Arc::new(db)
        }
        Err(e) => {
            eprintln!("[vredis] 数据恢复失败: {e}");
            // 版本不兼容（v0.3.0 起快照 v2 / WAL VADD 带 metric）与数据损坏
            // 都提示删除/迁移数据目录。已知取舍：旧 WAL 解析失败与位翻转损坏
            // 无法可靠区分，统一提示（后者同样以删除/恢复兜底）。
            match e {
                PersistError::VersionIncompatible(_) | PersistError::Corrupt(_) => {
                    eprintln!(
                        "[vredis] 提示：v0.3.0 起快照格式升级（v1→v2），旧数据目录不兼容。"
                    );
                    eprintln!(
                        "[vredis] 请删除 {}/ 后重启；或从备份中恢复数据。",
                        persist.dir.display()
                    );
                }
                PersistError::Io(_) => {}
            }
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
