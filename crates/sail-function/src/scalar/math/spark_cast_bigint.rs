//! The casts to BIGINT that `hex` and `bin` apply to their argument (`ImplicitCastInputTypes`).

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Decimal128Array, Int64Array};
use datafusion::arrow::datatypes::{Decimal128Type, DecimalType};
use datafusion_common::{Result, exec_datafusion_err};
use sail_common_datafusion::display::{spark_f32_to_string, spark_f64_to_string};

/// `QueryExecutionErrors.castingCauseOverflowError`: `shown` is the value as Spark prints it for its
/// source type (`1.0E30D`, `1.0E30`, `9223372036854775808.9BD`) and `source_type` the SQL type name.
pub(crate) fn cast_overflow_err(
    shown: &str,
    source_type: &str,
) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "[CAST_OVERFLOW] The value {shown} of the type \"{source_type}\" cannot be cast to \"BIGINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead."
    )
}

/// Spark's DOUBLE -> BIGINT cast: Rust's `as` matches the JVM's `(long) double`
/// (NaN -> 0, +/-Infinity -> i64::MAX/MIN, truncate toward zero, finite
/// out-of-range saturates). ANSI mode instead raises CAST_OVERFLOW for any value
/// outside the BIGINT range — NaN, +/-Infinity, and finite overflow alike.
pub(crate) fn double_to_i64(value: f64, ansi: bool) -> Result<i64> {
    if ansi && overflows_i64(value) {
        // `toSQLValue(Double)` prints NaN and the infinities bare; only a finite value gets `D`.
        let shown = match value.is_finite() {
            true => format!("{}D", spark_f64_to_string(value)),
            false => spark_f64_to_string(value),
        };
        return Err(cast_overflow_err(&shown, "DOUBLE"));
    }
    Ok(value as i64)
}

/// Spark's FLOAT -> BIGINT cast. A float widens to a double exactly, so the range check is the
/// same as for a DOUBLE; only the error names the FLOAT and prints it as `Float.toString` does.
pub(crate) fn float_to_i64(value: f32, ansi: bool) -> Result<i64> {
    if ansi && overflows_i64(f64::from(value)) {
        return Err(cast_overflow_err(&spark_f32_to_string(value), "FLOAT"));
    }
    Ok(value as i64)
}

fn overflows_i64(value: f64) -> bool {
    // Spark checks `Math.floor(x) <= Long.MaxValue && Math.ceil(x) >= Long.MinValue` with both
    // bounds promoted to double (`Cast.scala:2031`). `Long.MaxValue` rounds up to `2^63`, so
    // `2^63` itself is accepted and saturates to `i64::MAX`; the next f64 above it overflows.
    // The comparisons also cover +/-Infinity; NaN needs an explicit check since all NaN
    // comparisons are false.
    const I64_UPPER_BOUND: f64 = 9223372036854775808.0;
    value.is_nan() || value.floor() > I64_UPPER_BOUND || value.ceil() < i64::MIN as f64
}

/// Spark's DECIMAL -> BIGINT cast: truncate the fraction towards zero, then keep the low 64
/// bits when ANSI is off (`Decimal.toLong` wraps) or raise `CAST_OVERFLOW` when it is on.
pub(crate) fn decimal_array_to_i64(
    values: &Decimal128Array,
    precision: u8,
    scale: i8,
    ansi_mode: bool,
) -> Result<ArrayRef> {
    let divisor = 10_i128.checked_pow(u32::try_from(scale.max(0)).unwrap_or(0));
    let out = values
        .iter()
        .map(|v| {
            let Some(unscaled) = v else {
                return Ok(None);
            };
            let whole = match divisor {
                Some(divisor) if divisor != 0 => unscaled / divisor,
                _ => unscaled,
            };
            match i64::try_from(whole) {
                Ok(v) => Ok(Some(v)),
                Err(_) if ansi_mode => Err(cast_overflow_err(
                    &format!(
                        "{}BD",
                        Decimal128Type::format_decimal(unscaled, precision, scale)
                    ),
                    &format!("DECIMAL({precision},{scale})"),
                )),
                Err(_) => Ok(Some(whole as i64)),
            }
        })
        .collect::<Result<Int64Array>>()?;
    Ok(Arc::new(out))
}
