# vredis MVP 验收清单

对照 [vredis-charter.md](./vredis-charter.md) 的 8 条 MVP 验收标准逐项复核。
复核日期：2026-09-18（阶段 6 收尾）。

| # | 验收标准 | 结论 | 证据 |
|---|---|---|---|
| 1 | **工程质量**：`cargo build` / `cargo test` / `cargo clippy` 全部通过，clippy 零警告 | ✅ 满足 | 最终验证（2026-09-18）：`cargo test` **145/145 全绿**（lib 109 + 集成 36）；`cargo clippy --all-targets -- -D warnings` 零警告（门禁自阶段 5 起统一为 `--all-targets`，覆盖测试代码） |
| 2 | **协议兼容**：标准 `redis-cli` 可连接并执行全部 MVP 命令 | ⚠️ 部分满足 | RESP2 兼容性由 36 个真实 TCP 集成测试（t01–t36）+ 真实 `vredis.exe` 二进制的 PowerShell TCP 会话实测记录（README「真实会话记录 / 协议实测」，含 PING/SET/GET/VADD/VSEARCH/VDIM/WRONGTYPE 逐字输出）佐证；**redis-cli 实测待有环境时补做**（开发机未安装） |
| 3 | **KV 命令语义**：9 条 KV 命令行为与 Redis 一致，错误文案沿用 Redis 风格 | ✅ 满足 | 分发/命令单元测试 d01–d05、k01–k06、e01–e19（错误文案逐字断言：`ERR unknown command 'FOO'`、`ERR wrong number of arguments for 'set' command`、`WRONGTYPE Operation against a key holding the wrong kind of value`）；集成 t05/t06/t22 验证错误后连接保持 |
| 4 | **向量命令**：VADD/VGET/VDIM/VSEARCH 可用，暴力搜索结果可手工复算 | ✅ 满足 | 距离单测 m01–m06（cos 正交=1、同向=0、零向量=1，l2/dot 已知值）；搜索单测 sr01–sr05（升序、k 截断、堆淘汰 20 取 3）；命令单测 w01–w09；集成 t27–t36；向量分量与距离均为 f64 最短表示，可手工复算（如 cos 正交距离 = 1） |
| 5 | **健壮性**：非法输入返回 RESP2 错误且连接保持；不 panic、不意外断连 | ✅ 满足 | 协议层 38 个测试覆盖 7 类消息**所有截断前缀**（Incomplete）、恶意输入（`$1000000000` 超长 bulk、`*2000000000` 恶意数组、65 层嵌套）；线程创建用 `Builder::spawn` 避免 panic 路径；锁毒化纵深防御（`PoisonError::into_inner`）；浮点分量拒绝 inf/nan |
| 6 | **并发安全**：多客户端并发读写不 panic、不死锁、无数据竞争 | ✅ 满足 | s09（8 线程 × 50 KV 并发冒烟）、v08（4 线程 × 100 并发 VADD，400 个 id 全局唯一）、t12（并发连接）、t13（客户端断连后服务器继续服务）；单把 Mutex 大锁，临界区均为内存操作 |
| 7 | **端口约定**：默认 127.0.0.1:6379；被占用时明确提示、非零退出码退出、绝不自动换端口 | ✅ 满足 | 阶段 3 真实二进制实测：第二实例打印提示（与 design.md §7 逐字一致）并以**退出码 1** 退出；提示文案位于 `src/main.rs`；无 `--port` 参数 |
| 8 | **依赖约束**：不引入任何第三方 crate | ✅ 满足 | `Cargo.toml` 的 `[dependencies]` 为空；全部使用标准库（`std::net` / `thread` / `sync::Mutex` / `collections::{HashMap, BinaryHeap}`） |

**总体结论**：8 条标准中 7 条满足，第 2 条部分满足（redis-cli 未实测，兼容性由真实 TCP 测试与二进制实测会话佐证）。

## 已知限制

- redis-cli 端到端实测待有环境时补做（当前由 36 个真实 TCP 用例保证 RESP2 兼容性）。
- BGSAVE 若快照保存成功但 WAL 截断失败，重启后 VADD 会重复插入（SET/DEL/FLUSHALL 幂等无影响）。不修原因：任何修复方案在另一失败模式下会导致数据丢失；修复需引入 WAL epoch，列入未来方向（design.md §8「已知限制 v0.4」）。
- 其他 MVP 范围外的简化项见 [design.md §8](./design.md)（TTL、事务、发布订阅、HNSW 等）。
