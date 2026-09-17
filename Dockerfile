# vredis 多阶段构建
# 阶段 1：rust:1.98-slim-bookworm 编译 release 二进制（与本地开发/CI 同为 1.98，GNU）。
# 显式锁定底层 Debian 为 bookworm：slim 标签的底层版本会随官方升级漂移（bookworm→trixie），
# 若 build 阶段升而 runtime 仍 bookworm，glibc 不兼容会导致运行报错。
# 阶段 2：debian:bookworm-slim 仅携带二进制运行
FROM rust:1.98-slim-bookworm AS build
WORKDIR /app

# —— 依赖缓存层：先只拷贝清单文件，用占位源码预构建依赖 ——
# 之后源码变更时本层命中缓存，仅重编 vredis crate 本体。
# （当前零第三方依赖，此层暂无外部依赖可缓存；一旦引入依赖立即生效）
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "pub fn placeholder() {}" > src/lib.rs \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked

# —— 真实源码构建：依赖层已缓存，只重编 crate 本体 ——
COPY src ./src
RUN cargo build --release --locked

# 阶段 2：运行镜像
FROM debian:bookworm-slim AS runtime
# vredis 的持久化目录是「可执行文件所在目录下的 ./data/」（design.md §3.1）：
# 二进制放在 /app/vredis，因此容器内挂载点为 /app/data
#（compose 的具名 volume 与 docker run -v 都挂这里）
COPY --from=build /app/target/release/vredis /app/vredis
# 非 root 运行 + 预建数据目录并赋属主：
# volume 首次挂载时从镜像目录继承权限，若 /app/data 是 root:root，
# 非 root 的 vredis 无法写 WAL/快照 → 启动后引擎直接进入 Broken 只读态
RUN useradd -r -s /sbin/nologin vredis \
    && mkdir -p /app/data \
    && chown -R vredis:vredis /app
USER vredis
WORKDIR /app
EXPOSE 6379
VOLUME ["/app/data"]
ENTRYPOINT ["/app/vredis"]
