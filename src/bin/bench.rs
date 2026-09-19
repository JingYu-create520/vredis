//! vredis search benchmark（进阶 4）：HNSW vs 暴力搜索实测对比。
//!
//! 独立 binary（Cargo 自动识别 src/bin/），零第三方依赖，自包含运行：
//! - 只复用库公共 API：`HnswIndex`（建图/搜索）与 `distance/Metric`（距离计算）；
//! - 自带 SplitMix64 副本（与 src/vector/hnsw.rs 同源算法；bench 独立 binary
//!   自包含副本，避免为可见性改动现有代码）。
//!
//! ★ CLI 解析健壮性（进阶 4 补充 2）：参数缺失 / 负数 / 非数字 / 未知参数
//!   一律 `eprintln + exit(2)`，绝不 panic。
//!
//! 用法：`cargo run --release --bin bench -- --n 10000 --dim 128 --k 10 --queries 100 [--json]`
//! ★ 必须用 `--release`：debug 构建慢 10–30×，数字无意义。

use std::time::Instant;

use vredis::vector::distance::{distance, Metric};
use vredis::vector::hnsw::HnswIndex;

// ── 固定基准参数（与 docs/design.md §4.4 / h11-h12 召回验证口径一致）──
/// 距离度量固定 l2（输出中注明）
const METRIC: Metric = Metric::L2;
/// 每层最大邻居数 m（论文默认）
const HNSW_M: usize = 16;
/// 建图搜索宽度 ef_construction
const HNSW_EF_CONSTRUCTION: usize = 200;
/// 基准数据 seed：固定值 ⇒ 任何机器、任何次运行的数据集与图结构一致
const BENCH_SEED: u64 = 0xBE4C_41CC;

/// SplitMix64：与 src/vector/hnsw.rs 同源（bench 自包含副本）。非密码学安全。
#[derive(Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// 命令行参数（解析失败的缺省值见 [`parse_args`]）。
#[derive(Debug)]
struct Args {
    n: usize,
    dim: usize,
    k: usize,
    queries: usize,
    /// HNSW 搜索宽度；默认 100（与 h11/h12 口径一致）。ef < k 时被 HNSW 内部
    /// 提升为 max(ef, k)（hnswlib 兼容语义，见 HnswIndex::search 文档）
    ef: usize,
    json: bool,
}

/// 解析当前 flag 的 usize 值：缺失 / 非数字 / 负数（usize::parse 拒绝 "-1"）
/// 统一转为友好错误信息 → main 以 exit(2) 退出（进阶 4 补充 2）。
fn parse_usize(argv: &mut std::vec::IntoIter<String>, flag: &str) -> Result<usize, String> {
    let value = argv.next().ok_or_else(|| format!("{flag} 缺少值"))?;
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} 的值 \"{value}\" 不是合法正整数"))
}

/// 解析命令行；任何问题 → Err（main 打印 usage 后 exit 2）。
fn parse_args() -> Result<Args, String> {
    let mut args = Args { n: 10_000, dim: 128, k: 10, queries: 100, ef: 100, json: false };
    let mut argv: std::vec::IntoIter<String> = std::env::args().skip(1).collect::<Vec<_>>().into_iter();
    while let Some(flag) = argv.next() {
        match flag.as_str() {
            "--n" => args.n = parse_usize(&mut argv, &flag)?,
            "--dim" => args.dim = parse_usize(&mut argv, &flag)?,
            "--k" => args.k = parse_usize(&mut argv, &flag)?,
            "--queries" => args.queries = parse_usize(&mut argv, &flag)?,
            "--ef" => args.ef = parse_usize(&mut argv, &flag)?,
            "--json" => args.json = true,
            other => {
                return Err(format!(
                    "未知参数 \"{other}\"（支持 --n/--dim/--k/--queries/--ef/--json）"
                ));
            }
        }
    }
    if args.n == 0 {
        return Err("--n 必须 ≥ 1".to_string());
    }
    if args.dim == 0 {
        return Err("--dim 必须 ≥ 1".to_string());
    }
    if args.k == 0 {
        return Err("--k 必须 ≥ 1".to_string());
    }
    if args.queries == 0 {
        return Err("--queries 必须 ≥ 1".to_string());
    }
    if args.ef == 0 {
        return Err("--ef 必须 ≥ 1".to_string());
    }
    if args.k > args.n {
        return Err(format!("--k {} 不能大于 --n {}", args.k, args.n));
    }
    Ok(args)
}

/// 均匀随机 f32 向量（固定 seed ⇒ 数据集可复现）。
fn rand_vec(rng: &mut SplitMix64, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|_| ((rng.next() >> 11) as f64 / (1u64 << 53) as f64) as f32)
        .collect()
}

/// 打印用法（参数错误时伴随 exit(2) 输出）。
fn print_usage() {
    eprintln!("usage: cargo run --release --bin bench -- [--n N] [--dim D] [--k K] [--queries Q] [--json]");
}

/// 除法保护：耗时趋近 0 时钳到极小值，避免 QPS/加速比出现 inf/NaN（JSON 非法）。
fn safe_div(a: f64, b: f64) -> f64 {
    a / b.max(1e-9)
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("bench: {e}");
            print_usage();
            std::process::exit(2);
        }
    };

    // ── 阶段 0：生成数据集与查询集（固定 seed，计时仅展示、不计入 QPS）──
    let mut rng = SplitMix64::new(BENCH_SEED);
    let t0 = Instant::now();
    let points: Vec<Vec<f32>> = (0..args.n).map(|_| rand_vec(&mut rng, args.dim)).collect();
    let queries: Vec<Vec<f32>> =
        (0..args.queries).map(|_| rand_vec(&mut rng, args.dim)).collect();
    let gen_secs = t0.elapsed().as_secs_f64();

    // ── 阶段 1：暴力搜索（计时；同时保存暴力 top-k id 作召回对照基准）──
    let t1 = Instant::now();
    let mut brute_top: Vec<Vec<String>> = Vec::with_capacity(args.queries);
    for q in &queries {
        let mut scored: Vec<(f64, usize)> = points
            .iter()
            .enumerate()
            .map(|(i, v)| (distance(METRIC, q, v), i))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        brute_top.push(scored.into_iter().take(args.k).map(|(_, i)| i.to_string()).collect());
    }
    let brute_secs = t1.elapsed().as_secs_f64();
    let brute_qps = safe_div(args.queries as f64, brute_secs);

    // ── 阶段 2：HNSW 建图（计时）──
    let t2 = Instant::now();
    let mut ix = match HnswIndex::new(METRIC, HNSW_M, HNSW_EF_CONSTRUCTION, BENCH_SEED) {
        Ok(ix) => ix,
        Err(e) => {
            // 常量参数已验证合法，此分支理论不可达；防御性处理
            eprintln!("bench: HNSW 参数非法: {e}");
            std::process::exit(2);
        }
    };
    for (i, v) in points.iter().enumerate() {
        if let Err(e) = ix.insert(&i.to_string(), v) {
            eprintln!("bench: HNSW 插入失败: {e}");
            std::process::exit(2);
        }
    }
    let build_secs = t2.elapsed().as_secs_f64();

    // ── 阶段 3：HNSW 搜索（计时 + 召回率对照暴力基准）──
    let t3 = Instant::now();
    let mut overlap = 0usize;
    for (qi, q) in queries.iter().enumerate() {
        let hits = match ix.search(q, args.k, args.ef) {
            Ok(hits) => hits,
            Err(e) => {
                eprintln!("bench: HNSW 搜索失败: {e}");
                std::process::exit(2);
            }
        };
        let truth = &brute_top[qi];
        overlap += hits.iter().filter(|(id, _)| truth.contains(id)).count();
    }
    let hnsw_secs = t3.elapsed().as_secs_f64();
    let hnsw_qps = safe_div(args.queries as f64, hnsw_secs);
    let recall = overlap as f64 / (args.queries as f64 * args.k as f64);
    // 加速比 = 暴力总耗时 / HNSW 总耗时（同查询集）
    let speedup = safe_div(brute_secs, hnsw_secs);

    // ── 输出 ──
    if args.json {
        // 单行 JSON（零依赖手写；字段全部为数字，无转义风险）
        println!(
            "{{\"n\":{},\"dim\":{},\"k\":{},\"queries\":{},\"ef_search\":{},\"brute_qps\":{:.2},\"hnsw_qps\":{:.2},\"speedup\":{:.2},\"recall\":{:.4},\"hnsw_build_secs\":{:.2}}}",
            args.n,
            args.dim,
            args.k,
            args.queries,
            args.ef,
            brute_qps,
            hnsw_qps,
            speedup,
            recall,
            build_secs
        );
        return;
    }
    println!("vredis search benchmark");
    println!("-----------------------");
    println!("dataset        : {} points × {} dims, metric = l2", args.n, args.dim);
    println!(
        "hnsw params    : m = {}, ef_construction = {}, ef_search = {}",
        HNSW_M, HNSW_EF_CONSTRUCTION, args.ef
    );
    println!("generate       : {gen_secs:.2}s");
    println!(
        "brute force    : {} queries in {brute_secs:.2}s  ->  {brute_qps:.1} QPS",
        args.queries
    );
    println!("hnsw build     : {build_secs:.2}s");
    println!(
        "hnsw search    : {} queries in {hnsw_secs:.2}s  ->  {hnsw_qps:.1} QPS",
        args.queries
    );
    println!("speedup        : {speedup:.1}x");
    println!("recall@{}     : {recall:.4}", args.k);
}
