// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result;

use super::xorshift::XorShiftRandom;
use crate::error::{generic_exec_err, unsupported_data_type_exec_err};

/// Generates the uniform values for one batch, as Spark replaces `uniform` with
/// `CAST(CAST(min AS DOUBLE) + (CAST(max AS DOUBLE) - CAST(min AS DOUBLE)) * rand(seed) AS <type>)`.
/// The bounds keep their own types, since each of them is cast to `DOUBLE` directly.
pub(crate) fn uniform_with_rng(
    min_array: &ArrayRef,
    max_array: &ArrayRef,
    number_rows: usize,
    output_type: &DataType,
    rng: &mut XorShiftRandom,
) -> Result<ArrayRef> {
    // Fast path: if either bound is fully null, the result is all-null and the RNG
    // does not advance, as Spark does not evaluate `rand` for a null bound.
    if min_array.null_count() == number_rows || max_array.null_count() == number_rows {
        return Ok(new_null_array(output_type, number_rows));
    }

    let min_values = bound_values(min_array)?;
    let max_values = bound_values(max_array)?;
    let values = (0..number_rows).map(|i| match (min_values[i], max_values[i]) {
        (Some(min), Some(max)) => Some(min + (max - min) * rng.next_double()),
        _ => None,
    });
    // The casts from `f64` to integers truncate toward zero and saturate, as in Spark.
    let array: ArrayRef = match *output_type {
        DataType::Int8 => Arc::new(values.map(|v| v.map(|v| v as i8)).collect::<Int8Array>()),
        DataType::Int16 => Arc::new(values.map(|v| v.map(|v| v as i16)).collect::<Int16Array>()),
        DataType::Int32 => Arc::new(values.map(|v| v.map(|v| v as i32)).collect::<Int32Array>()),
        DataType::Int64 => Arc::new(values.map(|v| v.map(|v| v as i64)).collect::<Int64Array>()),
        DataType::Float32 => Arc::new(
            values
                .map(|v| v.map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        DataType::Float64 => Arc::new(values.collect::<Float64Array>()),
        DataType::Decimal128(precision, scale) => Arc::new(
            values
                .map(|v| v.map(|v| decimal_from_f64(v, precision, scale)).transpose())
                .collect::<Result<Decimal128Array>>()?
                .with_precision_and_scale(precision, scale)?,
        ),
        _ => {
            return Err(unsupported_data_type_exec_err(
                "uniform",
                "Integer, Float, or Decimal",
                output_type,
            ));
        }
    };
    Ok(array)
}

/// Returns the bound values as `f64`, as Spark casts them to `DOUBLE`.
fn bound_values(array: &ArrayRef) -> Result<Vec<Option<f64>>> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{Decimal128Type, Float64Type};

    match array.data_type() {
        DataType::Null => Ok(vec![None; array.len()]),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let (precision, scale) = (*precision, *scale);
            let values = cast(array, &DataType::Decimal128(precision, scale))?;
            values
                .as_primitive::<Decimal128Type>()
                .iter()
                .map(|value| value.map(|value| decimal_to_f64(value, scale)).transpose())
                .collect()
        }
        _ => {
            let values = cast(array, &DataType::Float64)?;
            Ok(values.as_primitive::<Float64Type>().iter().collect())
        }
    }
}

/// Converts a decimal to `f64` as Spark's `Decimal.toDouble`, which rounds the exact
/// value to the nearest `f64`.
fn decimal_to_f64(unscaled: i128, scale: i8) -> Result<f64> {
    if (0..=18).contains(&scale) && unscaled.unsigned_abs() < 1 << f64::MANTISSA_DIGITS {
        // Both operands are exact, so the division is correctly rounded.
        Ok(unscaled as f64 / 10_i64.pow(scale as u32) as f64)
    } else {
        format!("{unscaled}e{}", -scale)
            .parse::<f64>()
            .map_err(|e| generic_exec_err("uniform", &e.to_string()))
    }
}

/// Converts a value to a decimal as Spark's `CAST(<double> AS DECIMAL)`, which rounds the
/// decimal representation of `Double.toString` half up.
fn decimal_from_f64(value: f64, precision: u8, scale: i8) -> Result<i128> {
    let overflow = || {
        generic_exec_err(
            "uniform",
            &format!("{value} cannot be represented as Decimal({precision}, {scale})."),
        )
    };
    if !value.is_finite() {
        return Err(overflow());
    }
    let (digits, length, exponent) = java_double_digits(value.abs());
    let digits = &digits[..length];
    // The value is `0.d1d2...dn * 10^(exponent + 1)`, and the first `kept` digits have a
    // weight of at least `10^(-scale)`.
    let kept = exponent + 1 + scale as i32;
    let mut unscaled: i128 = 0;
    for digit in digits.iter().take(kept.max(0) as usize) {
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|v| v.checked_add(*digit as i128))
            .ok_or_else(overflow)?;
    }
    if let Some(zeros) = kept.checked_sub(digits.len() as i32).filter(|n| *n > 0) {
        unscaled = 10_i128
            .checked_pow(zeros as u32)
            .and_then(|v| unscaled.checked_mul(v))
            .ok_or_else(overflow)?;
    }
    if kept >= 0 && digits.get(kept as usize).is_some_and(|digit| *digit >= 5) {
        unscaled += 1;
    }
    if unscaled >= 10_i128.pow(precision as u32) {
        return Err(overflow());
    }
    Ok(if value < 0.0 { -unscaled } else { unscaled })
}

/// Returns the decimal digits and the exponent of a non-negative finite value as printed by
/// `Double.toString` since Java 19: the decimal closest to the value (with ties to an even
/// last digit) among the shortest ones with at least two digits that round to the value.
/// Java 17 prints all the digits of some integers of at least 2^53 in magnitude instead
/// (JDK-4511638).
fn java_double_digits(value: f64) -> ([u8; 17], usize, i32) {
    use std::fmt::Write;

    // The shortest representation from `Display` is not always the closest one.
    let mut shortest = FormatBuffer::new();
    let _ = write!(shortest, "{value:e}");
    let length = shortest
        .as_str()
        .split('e')
        .next()
        .unwrap_or_default()
        .bytes()
        .filter(u8::is_ascii_digit)
        .count()
        .clamp(2, 17);
    // Exact formatting rounds the value to the nearest, with ties to even.
    let mut text = FormatBuffer::new();
    let _ = write!(text, "{value:.*e}", length - 1);
    let text = text.as_str();
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    let mut digits = [0; 17];
    let mut count = 0;
    for digit in mantissa.bytes().filter(u8::is_ascii_digit) {
        digits[count] = digit - b'0';
        count += 1;
    }
    (digits, count, exponent.parse().unwrap_or_default())
}

/// A stack buffer for formatting a finite `f64` without allocating. Scientific notation
/// with at most 17 significant digits fits in 32 bytes.
struct FormatBuffer {
    bytes: [u8; 32],
    len: usize,
}

impl FormatBuffer {
    fn new() -> Self {
        Self {
            bytes: [0; 32],
            len: 0,
        }
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).unwrap_or_default()
    }
}

impl std::fmt::Write for FormatBuffer {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        let dest = self.bytes.get_mut(self.len..end).ok_or(std::fmt::Error)?;
        dest.copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_integer_truncates_toward_zero() -> Result<()> {
        // `SELECT uniform(-5000000000L, 5000000000L, 42) FROM range(0, 23, 1, 1)` in Spark 4.2.
        let expected: [i64; 23] = [
            1191893702,
            96018842,
            3325259388,
            -2367719095,
            1702867696,
            173283545,
            4991441647,
            -4300676627,
            4696695610,
            2959575617,
            -515749415,
            1793959570,
            -1275886137,
            3326094725,
            2479557402,
            2216183163,
            -4839487789,
            1307120027,
            -4246291762,
            3389305582,
            -186663888,
            2310762213,
            413617395,
        ];
        let min: ArrayRef = Arc::new(Int64Array::from_value(-5_000_000_000, expected.len()));
        let max: ArrayRef = Arc::new(Int64Array::from_value(5_000_000_000, expected.len()));
        let mut rng = XorShiftRandom::new(42);
        let actual = uniform_with_rng(&min, &max, expected.len(), &DataType::Int64, &mut rng)?;
        assert_eq!(
            actual.as_ref(),
            &Int64Array::from(expected.to_vec()) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn test_uniform_converts_each_bound_to_double() -> Result<()> {
        // `SELECT uniform(16777217, CAST(16777219 AS FLOAT), 0) FROM range(0, 8, 1, 1)` in
        // Spark 4.2, where the integer bound is not converted to `FLOAT`.
        let min: ArrayRef = Arc::new(Int32Array::from_value(16777217, 8));
        // `CAST(16777219 AS FLOAT)` rounds to 16777220.
        let max: ArrayRef = Arc::new(Float32Array::from_value(16777220.0, 8));
        let mut rng = XorShiftRandom::new(0);
        let actual = uniform_with_rng(&min, &max, 8, &DataType::Float32, &mut rng)?;
        let expected = Float32Array::from(vec![
            16777220.0, 16777218.0, 16777218.0, 16777218.0, 16777220.0, 16777218.0, 16777218.0,
            16777218.0,
        ]);
        assert_eq!(actual.as_ref(), &expected as &dyn Array);
        Ok(())
    }

    #[test]
    fn test_decimal_from_f64_rounds_shortest_representation_half_up() -> Result<()> {
        // `CAST(<double> AS DECIMAL(38, 18))` in Spark 4.2 uses the shortest representation.
        assert_eq!(
            decimal_from_f64(1.7145670158242736, 38, 18)?,
            1_714_567_015_824_273_600
        );
        assert_eq!(decimal_from_f64(2.5, 2, 0)?, 3);
        assert_eq!(decimal_from_f64(-2.5, 2, 0)?, -3);
        assert_eq!(decimal_from_f64(-4.404, 4, 2)?, -440);
        assert!(decimal_from_f64(240.6453672351514, 2, 1).is_err());
        // Spark on Java 19 or later (Java 17 gives 99208681397455808).
        assert_eq!(
            decimal_from_f64(9.92086813974558e16, 38, 18)?,
            99_208_681_397_455_800 * 10_i128.pow(18)
        );
        // The closest shortest representation, not 1131761090849672.3 from `Display`.
        assert_eq!(
            decimal_from_f64(1131761090849672.2, 38, 18)?,
            11_317_610_908_496_722 * 10_i128.pow(17)
        );
        assert_eq!(decimal_from_f64(0.0005, 4, 3)?, 1);
        assert_eq!(decimal_from_f64(0.0004, 4, 3)?, 0);
        assert_eq!(decimal_from_f64(1234.5, 4, -1)?, 123);
        Ok(())
    }

    #[test]
    fn test_decimal_to_f64_rounds_exact_value() -> Result<()> {
        // `CAST(CAST(61822791393531.8852 AS DECIMAL(18, 4)) AS DOUBLE)` in Spark 4.2.
        assert_eq!(
            decimal_to_f64(618_227_913_935_318_852, 4)?,
            61822791393531.88
        );
        assert_eq!(decimal_to_f64(-55, 1)?, -5.5);
        assert_eq!(decimal_to_f64(10_i128.pow(37), 18)?, 1e19);
        Ok(())
    }
}
