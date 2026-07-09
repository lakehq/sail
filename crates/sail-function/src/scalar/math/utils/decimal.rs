use std::cmp::{max, min};

use datafusion::arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};

// https://github.com/apache/spark/blob/50a328ba98577ea12bbae50f2cbf406438b01a2f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1491-L1508
#[inline]
pub fn round_decimal_base(
    precision: i32,
    scale: i32,
    target_scale: i32,
    decimal_128: bool,
) -> (u8, i8) {
    let integral_least_num_digits = precision - scale + 1;
    if target_scale < 0 {
        let new_precision = max(integral_least_num_digits, -target_scale + 1) as u8;
        if decimal_128 {
            (min(new_precision, DECIMAL128_MAX_PRECISION), 0)
        } else {
            (min(new_precision, DECIMAL256_MAX_PRECISION), 0)
        }
    } else {
        let new_scale = min(scale, target_scale);
        let max_precision = if decimal_128 {
            DECIMAL128_MAX_PRECISION
        } else {
            DECIMAL256_MAX_PRECISION
        } as i32;
        (
            min(integral_least_num_digits + new_scale, max_precision) as u8,
            new_scale as i8,
        )
    }
}

/// Spark's `DecimalType.MINIMUM_ADJUSTED_SCALE`: the minimum number of fractional
/// digits preserved when a wide decimal result is capped at precision 38.
pub const SPARK_MINIMUM_ADJUSTED_SCALE: i32 = 6;

/// Spark's `adjustPrecisionScale`: when a computed decimal precision exceeds 38,
/// cap it at 38 and reduce the scale, keeping at least
/// `min(scale, SPARK_MINIMUM_ADJUSTED_SCALE)` fractional digits. DataFusion's
/// coercion instead caps the scale at 38, which diverges from Spark for wide results.
pub fn adjust_precision_scale(precision: i32, scale: i32) -> (u8, i8) {
    let max_precision = DECIMAL128_MAX_PRECISION as i32;
    if precision <= max_precision {
        (precision as u8, scale as i8)
    } else {
        let int_digits = precision - scale;
        let min_scale = scale.min(SPARK_MINIMUM_ADJUSTED_SCALE);
        let adjusted_scale = (max_precision - int_digits).max(min_scale);
        (DECIMAL128_MAX_PRECISION, adjusted_scale as i8)
    }
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) * DECIMAL(p2,s2)`:
/// precision `p1 + p2 + 1` and scale `s1 + s2`, then [`adjust_precision_scale`]
/// (which reduces the scale when the precision exceeds 38). DataFusion caps the
/// precision at 38 but keeps the full scale, diverging from Spark for wide products.
pub fn spark_decimal_multiply_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    let precision = p1 as i32 + p2 as i32 + 1;
    let scale = s1 as i32 + s2 as i32;
    adjust_precision_scale(precision, scale)
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) / DECIMAL(p2,s2)`:
/// scale `max(6, s1 + p2 + 1)` (Spark's `MINIMUM_ADJUSTED_SCALE` floor of 6) and
/// precision `(p1 - s1) + s2 + scale`, then [`adjust_precision_scale`]. Arrow's
/// `div` kernel uses a different (smaller) scale, so division must apply this rule
/// to match Spark.
pub fn spark_decimal_divide_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    let scale = max(6, s1 as i32 + p2 as i32 + 1);
    let precision = (p1 as i32 - s1 as i32) + s2 as i32 + scale;
    adjust_precision_scale(precision, scale)
}
