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
    let max_precision = if decimal_128 {
        DECIMAL128_MAX_PRECISION
    } else {
        DECIMAL256_MAX_PRECISION
    } as i32;
    if target_scale < 0 {
        // Cap in i32 BEFORE narrowing: `-target_scale + 1` can overflow i32 for an extreme
        // literal scale, and casting to u8 before the `min` would wrap (e.g. 256 -> 0).
        let new_precision = max(
            integral_least_num_digits,
            target_scale.saturating_neg().saturating_add(1),
        );
        (min(new_precision, max_precision) as u8, 0)
    } else {
        let new_scale = min(scale, target_scale);
        (
            min(integral_least_num_digits + new_scale, max_precision) as u8,
            new_scale as i8,
        )
    }
}

/// Spark's `DecimalType.MINIMUM_ADJUSTED_SCALE`: the minimum number of fractional
/// digits preserved when a wide decimal result is capped at precision 38.
pub const SPARK_MINIMUM_ADJUSTED_SCALE: i32 = 6;

/// Spark's `DecimalType.MAX_SCALE`.
const SPARK_MAX_SCALE: i32 = 38;

/// Spark's `adjustPrecisionScale`: when a computed decimal precision exceeds 38,
/// cap it at 38 and reduce the scale, keeping at least
/// `min(scale, SPARK_MINIMUM_ADJUSTED_SCALE)` fractional digits. DataFusion's
/// coercion instead caps the scale at 38, which diverges from Spark for wide results.
///
/// Spark calls this only when `spark.sql.decimalOperations.allowPrecisionLoss` is
/// true; otherwise it uses [`bounded`].
/// <https://github.com/apache/spark/blob/v4.1.1/sql/api/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L166-L201>
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

/// Spark's `DecimalType.bounded`: clamp both precision and scale to their maxima
/// *without* reducing the scale to protect the integer part. This is what Spark uses
/// when `spark.sql.decimalOperations.allowPrecisionLoss` is false, where an
/// unrepresentable result yields NULL at runtime rather than a rounded value.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/api/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L144-L146>
pub fn bounded(precision: i32, scale: i32) -> (u8, i8) {
    (
        min(precision, DECIMAL128_MAX_PRECISION as i32) as u8,
        min(scale, SPARK_MAX_SCALE) as i8,
    )
}

/// Applies Spark's precision/scale capping for the operators that share the
/// `adjustPrecisionScale`-vs-`bounded` split (`+ - * %`, but not `/`).
fn cap(precision: i32, scale: i32, allow_precision_loss: bool) -> (u8, i8) {
    if allow_precision_loss {
        adjust_precision_scale(precision, scale)
    } else {
        bounded(precision, scale)
    }
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) * DECIMAL(p2,s2)`:
/// precision `p1 + p2 + 1` and scale `s1 + s2`, capped per `allow_precision_loss`.
/// DataFusion caps the precision at 38 but keeps the full scale, diverging from Spark
/// for wide products.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L603-L611>
pub fn spark_decimal_multiply_type(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> (u8, i8) {
    let precision = p1 as i32 + p2 as i32 + 1;
    let scale = s1 as i32 + s2 as i32;
    cap(precision, scale, allow_precision_loss)
}

/// The unclamped `(precision, scale)` of Spark's `+`/`-` rule, shared by the type and the
/// divergence check so the formula lives in one place: scale `max(s1,s2)` and precision
/// `max(p1-s1, p2-s2) + scale + 1`.
fn spark_add_precision_scale(p1: u8, s1: i8, p2: u8, s2: i8) -> (i32, i32) {
    let scale = max(s1 as i32, s2 as i32);
    let precision = max(p1 as i32 - s1 as i32, p2 as i32 - s2 as i32) + scale + 1;
    (precision, scale)
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) + DECIMAL(p2,s2)` — also the rule
/// for `-`, which Spark defines with the identical formula, capped per `allow_precision_loss`.
///
/// Arrow's own add/sub rule computes the same precision but caps it with a plain
/// `min(_, 38)` that keeps the scale — i.e. Spark's `bounded`, never `adjustPrecisionScale`.
/// So the two only diverge once the exact precision exceeds 38, and only under the default
/// `allowPrecisionLoss = true`.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L430-L438>
pub fn spark_decimal_add_type(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> (u8, i8) {
    let (precision, scale) = spark_add_precision_scale(p1, s1, p2, s2);
    cap(precision, scale, allow_precision_loss)
}

/// Whether Spark's `+`/`-` result type actually differs from Arrow's for these decimals.
///
/// Arrow caps the same precision with a plain `min(_, 38)` that keeps the scale — exactly
/// Spark's [`bounded`]. So the two agree whenever the precision fits in 38, AND whenever
/// `allow_precision_loss` is false (both use `bounded`), AND when `adjustPrecisionScale`
/// happens to leave the scale untouched (e.g. `decimal(38,2) + decimal(38,2)`). Re-typing
/// only when they genuinely differ avoids a needless per-row `round`+`cast` on the common
/// max-precision shapes.
pub fn spark_decimal_add_diverges(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> bool {
    let (precision, scale) = spark_add_precision_scale(p1, s1, p2, s2);
    cap(precision, scale, allow_precision_loss) != bounded(precision, scale)
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) % DECIMAL(p2,s2)` — also the
/// rule for `pmod`, which Spark documents as "This follows Remainder rule":
/// scale `max(s1,s2)` and precision `min(p1-s1, p2-s2) + scale`.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L983-L991>
pub fn spark_decimal_remainder_type(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> (u8, i8) {
    let scale = max(s1 as i32, s2 as i32);
    let precision = min(p1 as i32 - s1 as i32, p2 as i32 - s2 as i32) + scale;
    cap(precision, scale, allow_precision_loss)
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) / DECIMAL(p2,s2)`.
///
/// With `allow_precision_loss`, scale is `max(6, s1 + p2 + 1)` (Spark's
/// `MINIMUM_ADJUSTED_SCALE` floor) and precision `(p1 - s1) + s2 + scale`, then
/// `adjustPrecisionScale`. Arrow's `div` kernel uses a different (smaller) scale, so
/// division must apply this rule to match Spark.
///
/// Without it, division does **not** simply swap in `bounded` like the other
/// operators: Spark keeps Hive's older sizing, which trims the fractional digits by
/// `diff / 2 + 1` and gives the rest to the integer part.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L824-L840>
pub fn spark_decimal_divide_type(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> (u8, i8) {
    if allow_precision_loss {
        let scale = max(SPARK_MINIMUM_ADJUSTED_SCALE, s1 as i32 + p2 as i32 + 1);
        let precision = (p1 as i32 - s1 as i32) + s2 as i32 + scale;
        adjust_precision_scale(precision, scale)
    } else {
        let mut int_digits = min(SPARK_MAX_SCALE, p1 as i32 - s1 as i32 + s2 as i32);
        let mut dec_digits =
            (s1 as i32 + p2 as i32 + 1).clamp(SPARK_MINIMUM_ADJUSTED_SCALE, SPARK_MAX_SCALE);
        let diff = (int_digits + dec_digits) - SPARK_MAX_SCALE;
        if diff > 0 {
            dec_digits -= diff / 2 + 1;
            int_digits = SPARK_MAX_SCALE - dec_digits;
        }
        bounded(int_digits + dec_digits, dec_digits)
    }
}
