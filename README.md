# vredis

> 用 Rust 实现的、兼容 Redis RESP2 协议的迷你向量数据库。

Rust 1.98+ · 零第三方依赖 · MVP 采用暴力搜索保证正确性

## 5 分钟跑起来

```bash
cargo build
cargo run          # 监听 127.0.0.1:6379，等待 RESP2 客户端连接
```

- 默认监听 `127.0.0.1:6379`（仅本机回环）；端口被占用时打印明确提示并以退出码 1 退出，**绝不自动更换端口**。
- 跑测试（169 个，含真实 TCP 端到端）：`cargo test`

## Docker 部署

```bash
docker build -t vredis .
docker run -d --name vredis -p 6379:6379 -v vredis-data:/app/data vredis
```

或使用 docker compose：

```bash
docker compose up -d
docker compose logs -f
```

数据持久化：vredis 的 WAL 与快照写在**可执行文件所在目录下的 `data/`**（design.md §3.1），
镜像内二进制位于 `/app/vredis`，因此挂载点为 **`/app/data`**——compose 使用具名 volume
`vredis-data` 挂载，容器重建/重启数据不丢；BGSAVE 快照与启动恢复机制在容器内照常工作。
容器以非 root 用户 `vredis` 运行（镜像内已预建 `/app/data` 并赋属主）。

## 支持的命令

### KV 命令（兼容 Redis 语义与错误文案）

| 命令 | 说明 |
|---|---|
| `PING [message]` | 连接测试；无参返回 `+PONG`，带参原样返回 |
| `ECHO message` | 原样返回消息 |
| `SET key value` | 写入字符串；覆盖任意已存在类型；value 二进制安全 |
| `GET key` | 读取字符串；缺失返回 nil；对向量索引返回 `WRONGTYPE` |
| `DEL key [key ...]` | 删除，返回实际删除数量 |
| `EXISTS key [key ...]` | 返回存在的数量；重复 key 重复计数（与 Redis 一致） |
| `KEYS pattern` | 通配符匹配（仅支持 `*` 与 `?`），返回顺序不保证 |
| `TYPE key` | 返回 `string` / `vector` / `none`（`vector` 为本库扩展类型） |
| `FLUSHALL` | 清空全部数据 |
| `BGSAVE` | 触发一次快照并截断 WAL（v0.2.0 起；同步实现，无后台线程） |

### 向量命令（本库扩展）

| 命令 | 说明 |
|---|---|
| `VADD key dim v1 ... vdim` | 向索引 `key` 添加向量；索引不存在则创建并**锁定维度**；返回自动生成的 id（索引内自增十进制串，从 `"0"` 起） |
| `VGET key id` | 读取指定 id 的向量分量（浮点数组）；不存在返回 nil |
| `VDIM key` | 返回索引维度；索引不存在返回 nil |
| `VSEARCH key k v1 ... vdim [METRIC cos\|l2\|dot]` | 在索引内暴力搜索 top-k，返回扁平 `[id, 距离, ...]` 升序；默认 `cos`；距离越小越相似 |

## 真实会话记录 / 协议实测

> 本机无 redis-cli。以下为 **PowerShell TCP 客户端驱动真实 `vredis.exe`（127.0.0.1:6379）的实测记录**，
> `|` 是 RESP2 报文中 CRLF 的展示分隔符，内容逐字未改。RESP2 兼容性另由 36 个真实 TCP
> 集成测试保证（见 `tests/integration.rs`）。

```text
=== PING
C> *1 | $4 | PING |
S> +PONG |

=== SET greet hello
C> *3 | $3 | SET | $5 | greet | $5 | hello |
S> +OK |

=== GET greet
C> *2 | $3 | GET | $5 | greet |
S> $5 | hello |

=== VADD faces 3 1.0 0.0 0.0
C> *6 | $4 | VADD | $5 | faces | $1 | 3 | $3 | 1.0 | $3 | 0.0 | $3 | 0.0 |
S> $1 | 0 |                                  ← 自动生成 id "0"

=== VADD faces 3 0.0 1.0 0.0
C> *6 | $4 | VADD | $5 | faces | $1 | 3 | $3 | 0.0 | $3 | 1.0 | $3 | 0.0 |
S> $1 | 1 |                                  ← 自动生成 id "1"

=== VSEARCH faces 1 1.0 0.0 0.0
C> *6 | $7 | VSEARCH | $5 | faces | $1 | 1 | $3 | 1.0 | $3 | 0.0 | $3 | 0.0 |
S> *2 | $1 | 0 | $1 | 0 |                    ← 扁平 [id="0", cos 距离="0"]

=== VDIM faces
C> *2 | $4 | VDIM | $5 | faces |
S> :3 |

=== GET faces（预期 WRONGTYPE）
C> *2 | $3 | GET | $5 | faces |
S> -WRONGTYPE Operation against a key holding the wrong kind of value |
```

## 架构

五层结构，依赖严格自上而下单向（详见 [docs/design.md](docs/design.md) §1）：

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

## 设计亮点

- **协议二进制安全**：解析器绝不按行读取（bulk payload 可含 `\r\n`、`\0`），按声明长度切分；
  含特殊字节的值从 SET 到 GET 端到端往返有专项测试。
- **OOM 三重防护**：bulk 长度上限 512MB（对齐 Redis `proto-max-bulk-len`）；数组元素上限 100 万
  且**绝不按声明数预分配**；单连接输入缓冲 1GB 检查点（超限回错并断连）。
- **零第三方依赖**：全部使用 Rust 标准库（`std::net` / `thread` / `Mutex` / `BinaryHeap`），
  `Cargo.toml` 的 `[dependencies]` 为空。
- **锁纪律**：VSEARCH 的距离计算与 top-k 收集在存储层单次持锁回调内完成，
  绝不把整个索引克隆出锁外（对 10 万级向量索引是数量级差异）。
- **测试文化**：169 个测试——协议层对 7 类消息做**所有截断前缀**的 Incomplete 断言、
  恶意输入（`$1000000000`、`*2000000000`、65 层嵌套）防护测试、8 线程并发 KV 冒烟、
  4 线程并发 VADD 的 id 唯一性验证。

## 运行环境

- Rust 1.98+（edition 2021；在 1.98.1 stable GNU 上开发验证）
- 无其他依赖，Windows / Linux / macOS 均可构建

## Roadmap / Known Limitations

以下为记录在案的未来方向，当前均未实现：

- HNSW 近似最近邻索引
- Benchmark（10 万 / 100 万向量 QPS 与召回率）
- 分片锁替代单把 Mutex
- VDEL（删除索引内单条向量）
- Graceful shutdown
- 流水线缓冲用 `read_pos` 指针替代 `buf.drain`（消除 O(n²)）

## 文档

- [docs/design.md](docs/design.md) — 架构设计（v0.3）：分层、数据结构、命令语义、依赖选型
- [docs/vredis-charter.md](docs/vredis-charter.md) — 项目章程（最高规则）与 MVP 验收标准
- [docs/acceptance.md](docs/acceptance.md) — MVP 验收清单（8 条标准逐项复核）
