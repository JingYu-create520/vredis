# vredis 设计文档（MVP）

- 版本：v0.3（2026-09-18 修订：VADD 改为自动生成 id、VectorIndex 增加自增计数器、补充存储层搜索 API 与 VSEARCH 错误文案，应用阶段 5 用户决策）
- 日期：2026-09-17（v0.1） / 2026-09-18（v0.2 / v0.3）
- 上游约束：本设计严格遵守 [vredis-charter.md](./vredis-charter.md)（最高规则）

---

## 1. 总体架构

五层结构，依赖严格自上而下单向。一次请求的完整路径：

```
客户端（redis-cli / 任意 RESP2 客户端）
  │ TCP，默认 127.0.0.1:6379
  ▼
① 网络层（src/net）
  TcpListener accept 循环；每个连接派生一个线程；阻塞式读写
  │
  ▼
② 协议解析层（src/protocol）
  请求方向：字节流 → RespValue        响应方向：RespValue → 字节流
  │
  ▼
③ 命令分发层（src/command）
  命令表（命令名 → 处理函数）；参数校验；Redis 风格错误文案
  │
  ├────────────────────────────┐
  ▼                            ▼
④ 存储层（src/storage）         ⑤ 向量索引层（src/vector）
  Db：key → Value 内存哈希表     距离度量：cos / l2 / dot
  Mutex<HashMap> 单把大锁        索引内暴力扫描 top-k（MVP 无 HNSW）
```

| 层 | 模块 | 职责 | 使用的基础设施 |
|---|---|---|---|
| ① 网络层 | `src/net` | 监听、accept、每连接一线程、连接读写循环、连接级错误处理（断连） | `std::net`、`std::thread` |
| ② 协议解析层 | `src/protocol` | RESP2 解析（支持数组与简化 inline 两种请求形式）与编码 | 仅 `std` |
| ③ 命令分发层 | `src/command` | 命令名匹配、参数校验、错误文案、调用下层并组装 `RespValue` | `protocol`、`storage`、`vector` |
| ④ 存储层 | `src/storage` | key → Value 的内存表；所有读写的唯一入口；并发控制 | `std::sync::Mutex`、`std::collections::HashMap` |
| ⑤ 向量索引层 | `src/vector` | 向量分量校验、三种距离度量、暴力 top-k 搜索（只读遍历存储层） | 仅 `std` |

依赖方向约束（代码评审时检查）：

- `net → protocol → command → {storage, vector}`，严格单向，禁止反向依赖；
- 向量索引层对存储层**只读**（遍历向量做搜索），一切写操作都经由存储层完成；
- 各层均不感知 TCP，协议层不感知命令语义 —— 保证协议层和向量层可以独立单测。

---

## 2. 目录结构与职责

```
vredis/
├── Cargo.toml              # 包定义；MVP 零第三方依赖
├── docs/
│   ├── vredis-charter.md   # 项目章程（最高规则）
│   └── design.md           # 本设计文档
├── src/
│   ├── main.rs             # 入口：构建默认配置 → 绑定端口（冲突则提示并退出）→ 启动服务器
│   ├── lib.rs              # 库根：声明全部模块；集成测试 tests/ 通过库接口复用
│   ├── config.rs           # ServerConfig { host, port }；默认 127.0.0.1:6379
│   ├── error.rs            # VredisError：Io / Protocol 两类连接级错误
│   ├── net/
│   │   ├── mod.rs          # 网络层：accept 循环，每连接 spawn 线程
│   │   └── connection.rs   # 单连接生命周期：读 → 解析 → 分发 → 编码 → 写；协议损坏则断开
│   ├── protocol/
│   │   ├── mod.rs          # 模块根与再导出
│   │   ├── value.rs        # RespValue 枚举：RESP2 六种形态
│   │   ├── parser.rs       # 字节缓冲 → RespValue（流式：数据不足返回 Incomplete）
│   │   └── encoder.rs      # RespValue → RESP2 字节流
│   ├── command/
│   │   ├── mod.rs          # 分发层：execute(db, &[RespValue]) → RespValue；参数校验
│   │   ├── kv.rs           # KV 命令：PING ECHO SET GET DEL EXISTS KEYS TYPE FLUSHALL
│   │   └── vector.rs       # 向量命令：VADD VGET VDIM VSEARCH
│   ├── storage/
│   │   ├── mod.rs          # 存储层：Db 定义、读写原子操作
│   │   └── value.rs        # Value 枚举：Str(Vec<u8>) | VectorIndex { dim, vectors }
│   └── vector/
│       ├── mod.rs          # 向量索引层模块根
│       ├── distance.rs     # Metric 枚举与 cos / l2 / dot 距离实现
│       └── search.rs       # 暴力搜索：索引内全量扫描 + 大小为 k 的二叉堆
└── tests/
    └── e2e.rs              # M6 阶段：真实 TCP 端到端集成测试（使用临时端口，不占 6379）
```

---

## 3. 核心数据结构

```rust
// ── protocol/value.rs：RESP2 值的统一表示 ────────────────────────────
pub enum RespValue {
    Simple(String),        // +OK\r\n
    Error(String),         // -ERR unknown command 'foo'\r\n
    Integer(i64),          // :42\r\n
    Bulk(Vec<u8>),         // $5\r\nhello\r\n（Vec<u8> 保证二进制安全）
    Null,                  // $-1\r\n（RESP2 的 null bulk reply）
    Array(Vec<RespValue>), // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
}

// ── protocol/parser.rs：流式解析，单连接循环内的唯一入口 ─────────────
pub enum ParseOutcome {
    Complete(RespValue, usize), // 解析成功 + 消费的字节数
    Incomplete,                 // 缓冲区数据不足，等待下一次 socket 读取
    Error(String),              // 协议损坏 → 记日志并断开连接（与 Redis 行为一致）
}
pub fn parse(buf: &[u8]) -> ParseOutcome;

// ── storage/value.rs：key 背后存的值 ────────────────────────────────
pub enum Value {
    Str(Vec<u8>),           // SET / GET 管理
    VectorIndex {           // VADD 管理：一个 key 即一个向量索引（集合）
        dim: usize,                              // 索引维度，首次 VADD 时确定并永久锁定
        vectors: HashMap<String, Vec<f32>>,      // 向量 id → 分量
        next_id: u64,                            // 自增 id 计数器（v0.3：VADD 自动生成 id）
    },
}

// ── storage/mod.rs：数据库本体，进程内唯一，Arc 跨线程共享 ───────────
pub struct Db {
    // 单把大锁：MVP 以正确性与简单为先，临界区都是内存操作，足够快；
    // 后续如成瓶颈再演进为分片锁（设计上已隔离在 Db 内部，外部无感）
    map: Mutex<HashMap<String, Value>>,
}
// KV：set / get / del / exists_any / keys / flush
// 向量（v0.3，全部单次持锁，禁止把索引克隆出锁外）：
//   vector_add(key, data) -> Result<String, AddVectorError>  // 原子：查维度锁+生成id+插入+计数
//   get_vector(key, id)   -> Result<Option<Vec<f32>>, _>     // 仅克隆单条向量
//   probe_index(key)      -> IndexProbe::{Dim, Missing, NotAnIndex}
//   for_each_vector(key, f: FnMut(&str, &[f32])) -> VisitOutcome
//                                                    // ★ 遍历与回调都在 Mutex 临界区内执行

// ── vector/distance.rs ──────────────────────────────────────────────
pub enum Metric { Cosine, L2, Dot }          // 由 "cos" | "l2" | "dot" 解析，大小写不敏感
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f64;

// ── vector/search.rs ────────────────────────────────────────────────
pub struct Hit { pub id: String, pub dist: f64 }   // id 为索引内的向量 id
pub enum SearchError { IndexMissing, WrongType, DimensionMismatch(usize) }
pub fn search(db: &Db, index: &str, query: &[f32], k: usize, metric: Metric) -> Result<Vec<Hit>, SearchError>;
// 实现：probe 前置校验（短临界区）后，遍历 + top-k 收集在 Db::for_each_vector
// 的单次持锁内完成（std 二叉堆维护 top-k）；绝不把索引克隆出锁外

// ── command/mod.rs：分发层统一入口 ──────────────────────────────────
pub fn execute(db: &Arc<Db>, req: &[RespValue]) -> RespValue;
// req[0] 是命令名（Bulk），后续为参数；用 match 做静态命令表，MVP 不搞注册机制
```

### 关键设计决策

| # | 决策 | 理由 |
|---|---|---|
| D1 | key 使用 `String` 而非 `Vec<u8>` | RESP2 的 bulk 理论上允许任意字节作 key，但 MVP 做**最小实现**：key 必须是合法 UTF-8，否则返回 `-ERR ...`。此简化已记录，如需完全二进制安全再改为 `Vec<u8>`（改动被隔离在 storage 层内部） |
| D2 | 单把 `Mutex` 大锁 | 最小实现原则；临界区均为内存操作。并发的正确性由锁保证，性能不是 MVP 目标 |
| D3 | 向量分量存 `f32`，距离计算用 `f64` 累加 | 与主流向量库一致，省一半内存；f64 累加减少求和误差，搜索结果可手工复算 |
| D4 | `RespValue::Bulk(Vec<u8>)` | GET 返回的值必须二进制安全 |
| D5 | 命令级错误直接以 `RespValue::Error` 返回，不走 `error.rs` | 两类错误分离：命令级错误（参数错、类型错）原路回给客户端且**连接保持**；连接级错误（IO、协议损坏）才走 `VredisError` 并断连 |
| D6 | 静态 `match` 命令表 | 最小实现；命令总数 < 15，不需要注册/反射机制 |
| D7 | key 即向量索引名：一个索引锁定一个维度，内含多条带 id 的向量 | 2026-09-18 用户审核修订：搜索范围限定在索引内，并消除跨维度跳过的歧义 |
| D8 | VADD 自动生成 id：索引内自增十进制串（从 "0" 起），Bulk 返回 | 2026-09-18 阶段 5 用户决策，取代 v0.2 的显式 id 语法；id 永不碰撞，覆盖语义随之取消 |

---

## 4. MVP 命令列表与返回格式

命令名大小写不敏感。KV 部分与 Redis 语义对齐；向量命令为本库扩展，统一 `V` 前缀避免与 Redis 未来语义冲突。**数据模型（2026-09-18 修订）：VADD/VSEARCH 的第一个参数是索引名，一个索引包含多条带 id 的向量；向量命令只在指定索引内操作，绝不全库扫描。**

### 4.1 KV 命令（兼容 Redis）

| 命令 | 语法 | 成功返回 | 说明 |
|---|---|---|---|
| PING | `PING [msg]` | `+PONG` 或 bulk(msg) | |
| ECHO | `ECHO msg` | bulk(msg) | |
| SET | `SET key value` | `+OK` | 直接覆盖同 key 的任何类型（与 Redis 一致）；MVP 不支持 EX/NX 等选项 |
| GET | `GET key` | bulk 或 `$-1`(nil) | key 存的是向量 → `WRONGTYPE` |
| DEL | `DEL key [key ...]` | `:N`（实际删除数） | |
| EXISTS | `EXISTS key [key ...]` | `:N`（存在数，重复 key 重复计数，与 Redis 一致） | |
| KEYS | `KEYS pattern` | bulk 数组 | MVP 通配符仅支持 `*`（任意序列）与 `?`（单字符），不含 `[...]` |
| TYPE | `TYPE key` | `+string` / `+vector` / `+none` | `vector` 为本库扩展类型（向量索引） |
| FLUSHALL | `FLUSHALL` | `+OK` | 清空全部数据 |

请求/响应示例（RESP2 报文）：

```
客户端 → *3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nhello\r\n
服务端 → +OK\r\n

客户端 → *2\r\n$3\r\nGET\r\n$1\r\na\r\n
服务端 → $5\r\nhello\r\n
```

### 4.2 向量命令（本库扩展）

| 命令 | 语法 | 成功返回 | 说明 |
|---|---|---|---|
| VADD | `VADD key dim v1 ... vdim` | **bulk(id)** | 首参为**索引名**；索引不存在时自动创建并锁定维度；id 由索引内自增生成（"0" 起，十进制串）；之后 `dim` ≠ 索引维度 → 报错；分量必须是合法有限浮点数 |
| VGET | `VGET key id` | 浮点 bulk 数组，或 `$-1`(nil) | 索引或 id 不存在 → nil；key 是字符串 → `WRONGTYPE` |
| VDIM | `VDIM key` | `:维度` 或 `$-1`(nil) | 返回索引维度 |
| VSEARCH | `VSEARCH key k v1 ... vdim [METRIC cos\|l2\|dot]` | 扁平数组 `id1, dist1, id2, dist2, ...` | **仅在指定索引内**暴力搜索 top-k，绝不全库扫描；`k` 超过索引内向量数时返回全部；默认 `cos` |

VSEARCH 语义细则（必须在文档与注释中写明）：

- **搜索范围仅限第一个参数指定的索引**（2026-09-18 修订）：绝不全库扫描。
- 返回的是**距离**，升序排列（越小越相似）：
  - `cos`：余弦距离 = 1 − 余弦相似度 ∈ [0, 2]；任一向量模长为 0 时距离定义为 1.0（相似度 0）；
  - `l2`：欧氏距离；
  - `dot`：以 **−内积** 作为距离（内积越大距离越小，值可为负）。
- 维度校验（索引内维度统一，不做跨维度跳过，2026-09-18 阶段 5 用户确认维持报错语义）：`VADD` 的 `dim` 与索引既有维度不符 → `-ERR invalid vector dimension`；`VSEARCH` 查询向量维度 ≠ 索引维度 → 同样报错。
- 结果中的标识是**索引内的向量 id**（自增十进制串），不是全局 key。
- 索引不存在 → `$-1`(nil)；key 是字符串 → `WRONGTYPE`；`k` 非整数或为 0 → `-ERR value is not an integer or out of range`；非法 METRIC 名 → `-ERR invalid metric`。METRIC 尾缀解析顺序（防止 dim=1 时计数歧义）：**先剥离 METRIC 尾缀 → 再校验分量数 == 维度 → 再逐分量解析浮点**。
- 浮点输出统一用 Rust `{}` 最短表示（`1.0` 输出为 `"1"`；向量分量与距离分数同规则）。

```
客户端 → VADD faces 3 1.0 0.0 0.0          → $1\r\n0\r\n     （自动 id "0"）
客户端 → VADD faces 3 0.0 1.0 0.0          → $1\r\n1\r\n     （自动 id "1"）
客户端 → VSEARCH faces 1 1.0 0.0 0.0
服务端 → *2\r\n$1\r\n0\r\n$1\r\n0\r\n      （id "0"，cos 距离 0.0 → 输出 "0"）
```

### 4.3 错误文案约定（沿用 Redis 风格）

| 场景 | 返回 |
|---|---|
| 未知命令 | `-ERR unknown command 'FOO'`（MVP 简化，不带参数提示） |
| 参数数量错误 | `-ERR wrong number of arguments for 'get' command`（命令名小写） |
| 类型错误 | `-WRONGTYPE Operation against a key holding the wrong kind of value` |
| 浮点解析失败 | `-ERR value is not a valid float` |
| 维度不匹配 | `-ERR invalid vector dimension` |
| 整数解析失败/越界 | `-ERR value is not an integer or out of range`（VADD 的 dim、VSEARCH 的 k 等） |
| 度量名非法 | `-ERR invalid metric`（VSEARCH 的 METRIC 参数） |
| key 非 UTF-8 | `-ERR key must be valid UTF-8` |
| 协议损坏 | `-ERR Protocol error ...` 后**断开连接**（与 Redis 一致） |

请求格式：完整支持 RESP2 数组形式；同时支持**简化 inline 命令**（空格分隔、无引号处理），便于 `nc` 手测。redis-cli 始终使用数组形式。

---

## 5. 依赖选型

**结论：MVP 零第三方依赖**，`Cargo.toml` 的 `[dependencies]` 为空。

| 需求 | 选型 | 说明 |
|---|---|---|
| TCP 服务 | `std::net::{TcpListener, TcpStream}` | 阻塞式 + 每连接一线程；MVP 数据量和并发规模下完全够用 |
| 并发 | `std::sync::{Arc, Mutex}`、`std::thread` | |
| 序列化 | 手写 RESP2 编解码 | 协议仅 5 种类型 + 数组，预计 ~250 行，可控且有完整单测 |
| 错误处理 | 手写 `VredisError` 枚举 | 错误种类少，不需要 thiserror/anyhow |
| 日志 | `eprintln!` | MVP 不引入 log/tracing |
| 测试 | 内置 `#[test]`；集成测试用 std `TcpStream` 写一个 ~30 行的最小 RESP2 测试客户端 | 不需要测试专用依赖 |

被否决的候选（避免后续重复讨论）：

| 候选 | 否决理由 |
|---|---|
| tokio / async-std | 异步运行时对 MVP 是过度设计；阻塞 + 线程模型代码更简单、更易验证正确性 |
| serde | 没有外部序列化需求，RESP2 手写即可 |
| thiserror / anyhow | 错误类型少且集中，手写 `Display` 几十行 |
| rayon | 暴力搜索在持锁单线程内完成，MVP 数据量下无并行需求 |

---

## 6. MVP 开发排期与里程碑

| 里程碑 | 对应阶段 | 内容 | 完成标志 |
|---|---|---|---|
| M1 | 阶段 1（本次） | 项目初始化、章程、设计文档 | `cargo check` 通过，设计经用户确认 |
| M2 | 阶段 2 | RESP2 协议层：RespValue / parser / encoder + 完整单元测试 | `cargo test` + `cargo clippy` 零警告 |
| M3 | 阶段 3 | 网络层 + 命令分发骨架（PING/ECHO 打通全链路） | 同上 + redis-cli 冒烟 |
| M4 | 阶段 4 | 存储层 + 全部 KV 命令 | 同上 |
| M5 | 阶段 5 | 向量索引层 + VADD/VGET/VDIM/VSEARCH | 同上 + 搜索结果手工验算 |
| M6 | 阶段 6 | TCP 端到端集成测试、README、对照验收标准逐项复核 | MVP 验收标准 8 项全过 |

每阶段固定流程：**出计划 → 用户确认 → 编码 → `cargo test` + `cargo clippy`（零警告）→ 汇报 → 停止等待确认**。预计总代码量约 1000–1500 行（含测试与注释）。

---

## 7. 端口与运行约定

- 默认监听 `127.0.0.1:6379`，常量写死在 `src/config.rs`（仅绑定本机回环，不暴露到局域网）。
- 绑定失败且错误为 `AddrInUse` 时输出：

  ```
  [vredis] 端口 6379 已被占用，可能已有 Redis 或另一个 vredis 实例在运行。
  [vredis] 请释放该端口，或明确告知要使用的其他端口；程序不会自动更换端口。
  ```

  然后以退出码 1 退出。其他绑定错误同样打印明确原因后退出。
- MVP 不提供任何命令行参数（不设 `--port`）；确需换端口时，由用户明确指示后修改 `config.rs`。
- 集成测试使用端口 0（由操作系统分配临时端口），永不占用 6379。

---

## 8. 明确不做（MVP 范围外）

对应章程边界 + MVP 简化项，均列入未来清单、当前一律不实现：

TTL/过期删除、持久化（RDB/AOF）、事务（MULTI/EXEC）、发布订阅、Lua 脚本、SELECT 多数据库、AUTH 认证、HNSW/IVF 索引、集群、连接空闲超时管理、`SET` 的 EX/NX 等选项、完整 glob 通配符、删除索引内单条向量（VDEL）。
