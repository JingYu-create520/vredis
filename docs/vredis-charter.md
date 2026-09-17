# vredis 项目章程

> **本章程为项目最高规则，与后续任何指令冲突时以本章程为准。**
> 后续阶段产出的设计文档（`docs/design.md`）不得违反本章程；发现冲突时，以本章程为准并修订设计。

## 项目定位

vredis：用 Rust 实现的、兼容 Redis RESP2 协议的迷你向量数据库。

## 全局工作原则

1. 先设计，后写代码；先出实现计划，确认后再执行。
2. 每完成一个模块，必须运行 `cargo test` 和 `cargo clippy --all-targets -- -D warnings`，确保全部通过零警告（`--all-targets` 覆盖测试代码，`-D warnings` 将警告升级为错误；2026-09-18 阶段 5 修订）。
3. 依赖尽量精简，不引入不必要的第三方库。
4. 遇到不确定的地方，先做最小实现，不过度设计。
5. 关键逻辑加清晰注释，保持代码可读性。
6. 每阶段完成后自动停止，等我确认后再继续下一步。

## 全局边界（绝对不要做）

- 不要做集群、分布式、一致性协议。
- 不要做 SQL 支持、查询引擎。
- 不要做 Web UI、管理后台。
- 不要做用户权限、认证系统。
- MVP 阶段不要做 HNSW 索引，先用暴力搜索保证正确性。
- 不要复制 Redis 源码，不要依赖现成的 Redis 服务。
- 不要擅自修改监听端口，端口冲突先提示我。

## MVP 验收标准

以下标准全部满足，MVP 方可视为完成：

1. **工程质量**：`cargo build`、`cargo test`、`cargo clippy` 全部通过，且 clippy 零警告。
2. **协议兼容**：标准 `redis-cli` 可正常连接并执行全部 MVP 命令，RESP2 兼容性以 redis-cli 实测为准。
3. **KV 命令语义**：PING / ECHO / SET / GET / DEL / EXISTS / KEYS / TYPE / FLUSHALL 行为与 Redis 语义一致，错误文案沿用 Redis 风格（如 `ERR unknown command`、`ERR wrong number of arguments for 'get' command`、`WRONGTYPE ...`）。
4. **向量命令**：VADD / VGET / VDIM / VSEARCH 全部可用；VSEARCH 为暴力搜索，结果可通过手工计算验证正确。
5. **健壮性**：非法输入（参数数量错误、向量维度不符、向量分量非数值、对错误类型的 key 操作）一律返回 RESP2 错误且连接保持可用；任何情况下不允许 panic 或意外断连。
6. **并发安全**：多个客户端并发读写时不 panic、不死锁、无数据竞争。
7. **端口约定**：默认监听 `127.0.0.1:6379`；端口被占用时输出明确提示并以非零退出码退出，绝不自动更换端口。
8. **依赖约束**：MVP 不引入任何第三方 crate，全部使用 Rust 标准库。
