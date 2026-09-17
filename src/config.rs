//! 服务器启动配置。
//!
//! 端口约定（docs/design.md §7）：默认监听 127.0.0.1:6379；端口被占用时由 main
//! 打印提示并以非零退出码退出，绝不自动更换端口。MVP 无命令行参数。
//! 集成测试使用 port 0（操作系统分配临时端口），永不占用 6379。

/// 服务器监听配置。
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 监听地址；默认仅绑定本机回环，不暴露到局域网
    pub host: String,
    /// 监听端口；默认 6379（与 Redis 一致），0 表示由操作系统分配
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: "127.0.0.1".to_string(), port: 6379 }
    }
}
