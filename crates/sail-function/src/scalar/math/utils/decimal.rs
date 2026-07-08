use std::cmp::{max, min};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, Decimal128Array, Decimal128Builder, PrimitiveArray,
};
use datafusion::arrow::compute::kernels::arithmetic::multiply_fixed_point_checked;
use datafusion::arrow::compute::kernels::cast::{CastOptions, cast, cast_with_options};
use datafusion::arrow::compute::kernels::numeric::{add, sub};
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, Decimal128Type, DecimalType, i256,
};
use datafusion_common::{Result, exec_datafusion_err};

/// Spark's `adjustPrecisionScale`: when a computed decimal precision exceeds 38,
/// cap it at 38 and reduce the scale, keeping at least `min(scale, 6)` fractional
/// digits (`MINIMUM_ADJUSTED_SCALE`). DataFusion's coercion instead caps the
/// scale at 38, which diverges from Spark for wide results.
pub fn adjust_precision_scale(precision: i32, scale: i32) -> (u8, i8) {
    let max_precision = DECIMAL128_MAX_PRECISION as i32;
    if precision <= max_precision {
        (precision as u8, scale as i8)
    } else {
        let int_digits = precision - scale;
        let min_scale = scale.min(6);
        let adjusted_scale = (max_precision - int_digits).max(min_scale);
        (DECIMAL128_MAX_PRECISION, adjusted_scale as i8)
    }
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) * DECIMAL(p2,s2)`:
/// precision `p1 + p2 + 1` and scale `s1 + s2`, then [`adjust_precision_scale`].
pub fn spark_decimal_multiply_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    let precision = p1 as i32 + p2 as i32 + 1;
    let scale = s1 as i32 + s2 as i32;
    adjust_precision_scale(precision, scale)
}

/// Result `(precision, scale)` of Spark `DECIMAL(p1,s1) / DECIMAL(p2,s2)`:
/// scale `max(6, s1 + p2 + 1)` (Spark's `MINIMUM_ADJUSTED_SCALE` floor of 6) and
/// precision `(p1 - s1) + s2 + scale`, then [`adjust_precision_scale`]. Arrow's
/// `div` kernel uses a different (smaller) scale, so division must use this rule
/// + [`decimal_divide`] to match Spark.
pub fn spark_decimal_divide_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    let scale = max(6, s1 as i32 + p2 as i32 + 1);
    let precision = (p1 as i32 - s1 as i32) + s2 as i32 + scale;
    adjust_precision_scale(precision, scale)
}

/// A Spark additive arithmetic operator over decimals (multiplication has its
/// own precision-loss algorithm, see [`decimal_multiply`]).
#[derive(Clone, Copy)]
pub enum DecimalBinaryOp {
    Add,
    Subtract,
}

/// Evaluate `left <op> right` for `Decimal128` add/sub with Spark overflow
/// semantics. `error_on_overflow` (`= ansi_mode && !safe`) selects the
/// disposition of a result that exceeds the target precision: `true` raises
/// `ARITHMETIC_OVERFLOW`, `false` returns NULL per element (Spark's ANSI-off and
/// `try_*` behavior).
///
/// The result needs at most one extra integral digit, so the only overflow risk
/// is the raw `i128` range (e.g. the sum of two `DECIMAL(38,0)` maxima ≈
/// 2·10^38); we widen both operands to `Decimal256` (every such result fits
/// `i256`), apply the op there, then cast back — the cast's `safe` flag turns a
/// precision overflow into NULL or an error.
pub fn decimal_binary_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: DecimalBinaryOp,
    result_type: &DataType,
    error_on_overflow: bool,
) -> Result<ArrayRef> {
    let widen = |array: &ArrayRef| -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Decimal128(precision, scale) => {
                Ok(cast(array, &DataType::Decimal256(*precision, *scale))?)
            }
            _ => Ok(Arc::clone(array)),
        }
    };
    let (left, right) = (widen(left)?, widen(right)?);
    let widened = match op {
        DecimalBinaryOp::Add => add(&left, &right)?,
        DecimalBinaryOp::Subtract => sub(&left, &right)?,
    };
    let options = CastOptions {
        safe: !error_on_overflow,
        ..Default::default()
    };
    Ok(cast_with_options(&widened, result_type, &options)?)
}

/// Multiply two `Decimal128` operands with Spark precision-loss semantics.
///
/// Unlike add/sub, the product scale is `s1 + s2` and precision `p1 + p2 + 1`,
/// which Spark caps at 38 by *reducing the scale* (`adjustPrecisionScale`). The
/// raw product also routinely overflows `i128` even for tiny values once the
/// scale is high (`1.5 * 1.5` at `DECIMAL(38,19)` has raw ≈ 2·10^38). So we
/// compute in `i256`, round to the result scale (HALF_UP, like Spark), and check
/// the result fits the target precision: error under ANSI on, NULL under off.
///
/// `result_type` carries Spark's already-adjusted `(precision, scale)`; arrow's
/// `multiply_fixed_point_checked` performs exactly this for the ANSI-on (error)
/// path, and [`decimal_multiply_nullable`] mirrors it for the NULL path.
pub fn decimal_multiply(
    left: &ArrayRef,
    right: &ArrayRef,
    result_type: &DataType,
    error_on_overflow: bool,
) -> Result<ArrayRef> {
    let DataType::Decimal128(precision, scale) = *result_type else {
        return Err(exec_datafusion_err!(
            "decimal_multiply expects a Decimal128 result type, got {result_type}"
        ));
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let product = if error_on_overflow {
        multiply_fixed_point_checked(left, right, scale)?
            .with_precision_and_scale(precision, scale)?
    } else {
        decimal_multiply_nullable(left, right, precision, scale)?
    };
    Ok(Arc::new(product))
}

/// Divide two `Decimal128` operands with Spark precision/scale semantics.
///
/// `result_type` carries Spark's already-adjusted `(precision, scale)` (from
/// [`spark_decimal_divide_type`], NOT Arrow's `div` scale). The quotient at that
/// scale is `round(a * 10^(scale + s2 - s1) / b)` with HALF_UP rounding (via
/// [`divide_and_round`]), computed in `i256` to avoid the intermediate overflow.
/// Division by zero and a result that exceeds the target precision raise under
/// ANSI on (`error_on_overflow`) and become per-element NULL under ANSI off /
/// `try_divide`.
pub fn decimal_divide(
    left: &ArrayRef,
    right: &ArrayRef,
    result_type: &DataType,
    error_on_overflow: bool,
) -> Result<ArrayRef> {
    let DataType::Decimal128(precision, scale) = *result_type else {
        return Err(exec_datafusion_err!(
            "decimal_divide expects a Decimal128 result type, got {result_type}"
        ));
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    // Spark's result scale always exceeds s1 (scale >= s1 + p2 + 1), so the shift
    // is positive: scale up the dividend before the integer division + rounding.
    let shift = (scale as i32 + right.scale() as i32 - left.scale() as i32).max(0);
    let factor = i256::from_i128(10).pow_wrapping(shift as u32);
    let fits = |v: i128| Decimal128Type::validate_decimal_precision(v, precision, scale).is_ok();
    let quotient = |a: i128, b: i128| {
        divide_half_up(i256::from_i128(a).wrapping_mul(factor), i256::from_i128(b)).to_i128()
    };

    if error_on_overflow {
        let mut builder = Decimal128Builder::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                builder.append_null();
                continue;
            }
            let (a, b) = (left.value(i), right.value(i));
            if b == 0 {
                return Err(crate::scalar::math::spark_divide::divide_by_zero_err());
            }
            match quotient(a, b) {
                Some(v) if fits(v) => builder.append_value(v),
                _ => {
                    return Err(exec_datafusion_err!(
                        "[ARITHMETIC_OVERFLOW] Decimal division overflow. Use 'try_divide' to \
                         tolerate overflow and return NULL instead."
                    ));
                }
            }
        }
        Ok(Arc::new(
            builder
                .finish()
                .with_precision_and_scale(precision, scale)?,
        ))
    } else {
        let result: Decimal128Array = (0..left.len())
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    return None;
                }
                let (a, b) = (left.value(i), right.value(i));
                if b == 0 {
                    return None;
                }
                quotient(a, b).filter(|v| fits(*v))
            })
            .collect();
        Ok(Arc::new(result.with_precision_and_scale(precision, scale)?))
    }
}

/// NULL-on-overflow counterpart of [`multiply_fixed_point_checked`]: each element
/// whose product does not fit the target precision becomes NULL instead of
/// failing the whole batch (Spark's ANSI-off behavior).
fn decimal_multiply_nullable(
    left: &PrimitiveArray<Decimal128Type>,
    right: &PrimitiveArray<Decimal128Type>,
    precision: u8,
    required_scale: i8,
) -> Result<Decimal128Array> {
    let product_scale = left.scale() + right.scale();
    let same_scale = required_scale == product_scale;
    let divisor = i256::from_i128(10).pow_wrapping((product_scale - required_scale) as u32);
    let result: Decimal128Array = left
        .iter()
        .zip(right.iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => {
                let raw = i256::from_i128(a).wrapping_mul(i256::from_i128(b));
                let scaled = if same_scale {
                    raw
                } else {
                    divide_and_round(raw, divisor)
                };
                scaled.to_i128().filter(|v| {
                    Decimal128Type::validate_decimal_precision(*v, precision, required_scale)
                        .is_ok()
                })
            }
            _ => None,
        })
        .collect();
    Ok(result.with_precision_and_scale(precision, required_scale)?)
}

/// Integer division with Spark's HALF_UP (round half away from zero) for an
/// **arbitrary** divisor. [`divide_and_round`] only rounds correctly when the
/// divisor is a power of ten (its `div / 2` truncates for odd divisors — e.g.
/// `3 / 2 = 1` would round `x/3` up too eagerly), so decimal *division* by an
/// arbitrary denominator needs this general rule: round away from zero exactly
/// when `2·|remainder| >= |divisor|`.
fn divide_half_up(num: i256, den: i256) -> i256 {
    let quotient = num.wrapping_div(den);
    let remainder = num.wrapping_rem(den);
    let abs = |x: i256| {
        if x >= i256::ZERO { x } else { x.wrapping_neg() }
    };
    if abs(remainder).wrapping_mul(i256::from_i128(2)) >= abs(den) {
        if (num >= i256::ZERO) == (den >= i256::ZERO) {
            quotient.wrapping_add(i256::ONE)
        } else {
            quotient.wrapping_sub(i256::ONE)
        }
    } else {
        quotient
    }
}

/// Divide `input` by `div` rounding half away from zero (Spark's HALF_UP),
/// mirroring arrow's private `divide_and_round` for `i256`.
fn divide_and_round(input: i256, div: i256) -> i256 {
    let quotient = input.wrapping_div(div);
    let remainder = input.wrapping_rem(div);
    let half = div.wrapping_div(i256::from_i128(2));
    let half_neg = half.wrapping_neg();
    if input >= i256::ZERO {
        if remainder >= half {
            quotient.wrapping_add(i256::ONE)
        } else {
            quotient
        }
    } else if remainder <= half_neg {
        quotient.wrapping_sub(i256::ONE)
    } else {
        quotient
    }
}

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
