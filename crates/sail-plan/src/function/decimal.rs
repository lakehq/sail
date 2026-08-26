//! Spark's decimal result-type rules for the arithmetic operators.
//!
//! These are plan-time type-derivation rules, not kernels: they mirror
//! `resultDecimalType` on Spark's `Add`/`Multiply`/`Divide`/`Remainder` and the
//! `DecimalType.adjustPrecisionScale` / `DecimalType.bounded` pair those delegate to.
//! DataFusion's `BinaryTypeCoercer` derives different types for the capped cases, so the
//! arithmetic plan builders in `function::scalar::math` re-type the result against these.

use std::cmp::{max, min};

use datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION;

/// Spark's `DecimalType.MINIMUM_ADJUSTED_SCALE`: the minimum number of fractional
/// digits preserved when a wide decimal result is capped at precision 38.
const SPARK_MINIMUM_ADJUSTED_SCALE: i32 = 6;

/// Spark's `DecimalType.MAX_SCALE`.
const SPARK_MAX_SCALE: i32 = 38;

/// Spark's `adjustPrecisionScale`: when a computed decimal precision exceeds 38,
/// cap it at 38 and reduce the scale, keeping at least
/// `min(scale, SPARK_MINIMUM_ADJUSTED_SCALE)` fractional digits. DataFusion's
/// coercion instead caps the scale at 38, which diverges from Spark for wide results.
///
/// Spark calls this only when `spark.sql.decimalOperations.allowPrecisionLoss` is
/// true; otherwise it uses [`bounded`].
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L166-L201>
fn adjust_precision_scale(precision: i32, scale: i32) -> (u8, i8) {
    let max_precision = DECIMAL128_MAX_PRECISION as i32;
    if precision <= max_precision {
        (precision as u8, scale as i8)
    } else if scale < 0 {
        // Spark keeps a negative scale unchanged here (DecimalType.scala:182), reachable only
        // with `spark.sql.legacy.allowNegativeScaleOfDecimal` (negative-scale decimals). Mirror it
        // exactly for faithfulness; with non-negative operand scales the `+ - * %` result scale
        // stays `>= 0` (`*` uses `s1 + s2`), so this branch is not hit in practice.
        (DECIMAL128_MAX_PRECISION, scale as i8)
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
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L144-L146>
fn bounded(precision: i32, scale: i32) -> (u8, i8) {
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
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L603-L611>
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
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L430-L438>
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

/// Whether Spark's `+`/`-` needs the wide Decimal256 retype path instead of Arrow's native
/// i128 add/sub kernel.
///
/// The gate is "does the exact sum need more than 38 digits", mirroring the capped multiply
/// gate — NOT the narrower "does Spark's result TYPE differ from Arrow's". Once the precision
/// exceeds 38, Arrow caps it and the native i128 kernel RAISES on a sum that no longer fits,
/// while Spark yields NULL under ANSI off (`CheckOverflow`) and represents the value when the
/// capped type fits. This holds even when the two result TYPES agree — a scale-unchanged
/// overflow like `decimal(38,0) + decimal(38,0)` — and regardless of `allowPrecisionLoss`
/// (Spark's `bounded` still overflows to NULL), so the gate cannot be the old `cap != bounded`.
/// Below precision 38 nothing is capped, the sum is exact, and it stays on the native kernel.
pub fn spark_decimal_add_diverges(p1: u8, s1: i8, p2: u8, s2: i8) -> bool {
    spark_add_precision_scale(p1, s1, p2, s2).0 > DECIMAL128_MAX_PRECISION as i32
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) % DECIMAL(p2,s2)` — also the
/// rule for `pmod`, which Spark documents as "This follows Remainder rule":
/// scale `max(s1,s2)` and precision `min(p1-s1, p2-s2) + scale`.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L983-L991>
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
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L824-L840>
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
