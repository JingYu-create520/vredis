//! 距离度量：cos / l2 / dot（语义细则见 docs/design.md §4.2）。

/// 距离度量类型；VSEARCH 的 METRIC 参数由 [`Metric::parse`] 解析。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Cosine,
    L2,
    Dot,
}

impl Metric {
    /// 从参数字节解析度量名（大小写不敏感）；非法 → None（命令层回 `ERR invalid metric`）。
    pub(crate) fn parse(bytes: &[u8]) -> Option<Metric> {
        match bytes.to_ascii_uppercase().as_slice() {
            b"COS" => Some(Metric::Cosine),
            b"L2" => Some(Metric::L2),
            b"DOT" => Some(Metric::Dot),
            _ => None,
        }
    }

    /// 线格式编码（WAL / 快照）：0 = Cosine, 1 = L2, 2 = Dot。
    /// 未来新增度量**追加到末尾**（3, 4, ...），绝不改变已有编码。
    pub fn as_u8(self) -> u8 {
        match self {
            Metric::Cosine => 0,
            Metric::L2 => 1,
            Metric::Dot => 2,
        }
    }

    /// 线格式解码；未知字节 → None（上层转 Corrupt，响亮拒绝）。
    pub fn from_u8(byte: u8) -> Option<Metric> {
        match byte {
            0 => Some(Metric::Cosine),
            1 => Some(Metric::L2),
            2 => Some(Metric::Dot),
            _ => None,
        }
    }
}

/// 计算两向量的距离（**越小越相似**）。
///
/// - `cos`：余弦距离 = 1 − 余弦相似度 ∈ [0, 2]；任一向量模长为 0 → 距离定义为 1.0；
/// - `l2`：欧氏距离；
/// - `dot`：以 −内积 作为距离（内积越大距离越小，值可为负）。
///
/// 全程 f64 累加以减少求和误差（design.md D3），保证结果可手工复算。
/// 两侧维度必须相等（上游已校验；debug 构建下断言，release 不 panic）。
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f64 {
    debug_assert_eq!(a.len(), b.len(), "dimensions must match before distance()");
    match metric {
        Metric::Cosine => {
            let (mut dot, mut norm_a, mut norm_b) = (0.0f64, 0.0f64, 0.0f64);
            for i in 0..a.len() {
                let (x, y) = (f64::from(a[i]), f64::from(b[i]));
                dot += x * y;
                norm_a += x * x;
                norm_b += y * y;
            }
            if norm_a == 0.0 || norm_b == 0.0 {
                return 1.0; // 零向量：相似度定义为 0（design.md §4.2）
            }
            1.0 - dot / (norm_a.sqrt() * norm_b.sqrt())
        }
        Metric::L2 => {
            let mut sum = 0.0f64;
            for i in 0..a.len() {
                let d = f64::from(a[i]) - f64::from(b[i]);
                sum += d * d;
            }
            sum.sqrt()
        }
        Metric::Dot => {
            let mut dot = 0.0f64;
            for i in 0..a.len() {
                dot += f64::from(a[i]) * f64::from(b[i]);
            }
            -dot
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static EPS: f64 = 1e-12;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < EPS.max(expected.abs() * 1e-12),
            "expected {expected}, got {actual}"
        );
    }

    // —— 距离度量（m01–m06）——

    #[test]
    fn m01_cosine_orthogonal_is_one() {
        // cos 90° = 0 → 距离 1
        assert_close(distance(Metric::Cosine, &[1.0, 0.0], &[0.0, 1.0]), 1.0);
    }

    #[test]
    fn m02_cosine_identical_is_zero() {
        assert_close(distance(Metric::Cosine, &[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn m03_cosine_zero_vector_is_one() {
        // 任一模长为 0 → 距离定义为 1.0（相似度 0）
        assert_close(distance(Metric::Cosine, &[0.0, 0.0], &[1.0, 1.0]), 1.0);
        assert_close(distance(Metric::Cosine, &[1.0, 1.0], &[0.0, 0.0]), 1.0);
    }

    #[test]
    fn m04_l2_known_values() {
        assert_close(distance(Metric::L2, &[1.0, 0.0], &[0.0, 1.0]), 2.0f64.sqrt());
        assert_close(distance(Metric::L2, &[3.0], &[3.0]), 0.0);
    }

    #[test]
    fn m05_dot_is_negative_inner_product() {
        // 内积越大距离越小；距离可为负
        assert_close(distance(Metric::Dot, &[1.0, 0.0], &[2.0, 0.0]), -2.0);
        assert_close(distance(Metric::Dot, &[1.0, 1.0], &[1.0, 1.0]), -2.0);
    }

    #[test]
    fn m06_metric_parse_case_insensitive() {
        assert_eq!(Metric::parse(b"cos"), Some(Metric::Cosine));
        assert_eq!(Metric::parse(b"METRIC"), None);
        assert_eq!(Metric::parse(b"Dot"), Some(Metric::Dot));
        assert_eq!(Metric::parse(b"L2"), Some(Metric::L2));
        assert_eq!(Metric::parse(b"euclid"), None);
        assert_eq!(Metric::parse(b""), None);
    }
}
