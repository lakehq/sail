use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, new_null_array,
};
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, FieldRef,
};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{RngExt, rng};

use super::xorshift::SparkXorShiftRandom;
use crate::error::{
    generic_exec_err, invalid_arg_count_exec_err, unsupported_data_type_exec_err,
    unsupported_data_types_exec_err,
};

/// Generates random numbers from a uniform distribution between `min`
/// (inclusive) and `max` (exclusive), optionally seeded for reproducibility.
///
/// # Syntax
/// `uniform(min, max[, seed])`
///
/// # Implementation
///
/// Uses [`SparkXorShiftRandom`], a bit-for-bit port of Apache Spark's
/// `org.apache.spark.util.random.XORShiftRandom` (MurmurHash3-hashed seed +
/// XORShift with shifts 21/35/4 and a `Random.nextDouble()`-compatible output).
///
/// Scaling mirrors Spark's `Uniform` expression, which is replaced with
/// `CAST(CAST(min AS DOUBLE) + (CAST(max AS DOUBLE) - CAST(min AS DOUBLE)) * rand(seed) AS <type>)`:
/// each bound is converted to `f64` from its own type, the value is computed in
/// `f64`, and then cast to the output type.
/// - **Integer**: the cast truncates toward zero and saturates.
/// - **Float / Double**: the cast rounds to the nearest value.
/// - **Decimal**: the cast rounds the shortest decimal representation of the
///   value half up, which is what makes `uniform(5.5, 10.5, 123)` round to `6.3`
///   rather than truncating to `6.2`, matching Spark.
///
/// With a fixed seed the generated values match Spark JVM exactly.
///
/// # References
/// - Spark: <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/randomExpressions.scala>
/// - XORShiftRandom: <https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUniform {
    signature: Signature,
}

impl Default for SparkUniform {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUniform {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }

    /// Helper to extract precision and scale from any decimal type
    fn extract_decimal_info(dt: &DataType) -> Option<(u8, i8)> {
        match dt {
            DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
                Some((*p, *s))
            }
            DataType::Decimal256(p, s) => {
                if *p <= DECIMAL128_MAX_PRECISION && *s <= DECIMAL128_MAX_SCALE {
                    Some((*p, *s))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Helper to calculate the output decimal precision and scale
    /// Follows Spark's behavior: the type of `min` if it is wider than the type of `max`
    /// (more integral digits and at least the same scale), and the type of `max` otherwise.
    fn calculate_decimal_output(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
        if p1 as i16 - s1 as i16 >= p2 as i16 - s2 as i16 && s1 >= s2 {
            (p1, s1)
        } else {
            (p2, s2)
        }
    }

    fn calculate_output_type(t_min: &DataType, t_max: &DataType) -> DataType {
        use DataType::*;

        // Spark's literal NULL produces a DOUBLE fallback regardless of the other side.
        if matches!(t_min, Null) || matches!(t_max, Null) {
            return Float64;
        }

        // Float precedence: DOUBLE wins over FLOAT, which wins over DECIMAL/INT.
        if matches!(t_min, Float64) || matches!(t_max, Float64) {
            return Float64;
        }
        if matches!(t_min, Float32) || matches!(t_max, Float32) {
            return Float32;
        }

        if t_min.is_integer() && t_max.is_integer() {
            // Integer promotion hierarchy: Int8 < Int16 < Int32 < Int64.
            // UInt32/UInt64 require Int64 to safely represent all values.
            return match (t_min, t_max) {
                (Int64, _) | (_, Int64) | (UInt32, _) | (_, UInt32) | (UInt64, _) | (_, UInt64) => {
                    Int64
                }
                (Int32, _) | (_, Int32) | (UInt16, _) | (_, UInt16) => Int32,
                (Int16, _) | (_, Int16) | (UInt8, _) | (_, UInt8) => Int16,
                (Int8, Int8) => Int8,
                _ => Int32,
            };
        }

        let decimal_min = Self::extract_decimal_info(t_min);
        let decimal_max = Self::extract_decimal_info(t_max);

        match (decimal_min, decimal_max) {
            (Some((p1, s1)), Some((p2, s2))) => {
                // Spark uses one of the two types completely (both precision AND scale): e.g.
                // Decimal(2,1) + Decimal(20,0) → Decimal(20,0), not Decimal(20,1).
                let (precision, scale) = Self::calculate_decimal_output(p1, s1, p2, s2);
                Decimal128(precision, scale)
            }
            (Some((p, s)), None) | (None, Some((p, s))) => Decimal128(p, s),
            (None, None) => Float64,
        }
    }
}

impl ScalarUDFImpl for SparkUniform {
    fn name(&self) -> &str {
        "uniform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if !matches!(args.arg_fields.len(), 2 | 3) {
            return Err(invalid_arg_count_exec_err(
                "uniform",
                (2, 3),
                args.arg_fields.len(),
            ));
        }

        let t_min = args.arg_fields[0].data_type();
        let t_max = args.arg_fields[1].data_type();
        let return_type = Self::calculate_output_type(t_min, t_max);

        // The result is NULL whenever a bound is NULL, so the field must be
        // nullable if either bound is a NULL literal or a nullable expression.
        let nullable = matches!(t_min, DataType::Null)
            || matches!(t_max, DataType::Null)
            || args.arg_fields[0].is_nullable()
            || args.arg_fields[1].is_nullable();

        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Spark requires all arguments to be foldable (literal-like). A column
        // reference arrives here as `ColumnarValue::Array`, so we reject that
        // case to match Spark's `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`.
        let arg_names = ["min", "max", "seed"];
        for (i, arg) in args.args.iter().enumerate() {
            if matches!(arg, ColumnarValue::Array(_)) {
                return Err(generic_exec_err(
                    "uniform",
                    &format!(
                        "the input `{}` must be a foldable integer, floating-point, or decimal expression",
                        arg_names[i.min(arg_names.len() - 1)]
                    ),
                ));
            }
        }
        // Expand the foldable scalars to length-`number_rows` arrays ourselves so
        // we keep ownership of the RNG across all rows. Using
        // `make_scalar_function` would rebuild the RNG per invocation, which with
        // a fixed seed produces the same value on every row.
        let number_rows = args.number_rows;
        let output_type = args.return_field.data_type().clone();
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(number_rows))
            .collect::<Result<_>>()?;
        let array = uniform(&arrays, number_rows, &output_type)?;
        Ok(ColumnarValue::Array(array))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(invalid_arg_count_exec_err(
                "uniform",
                (2, 3),
                arg_types.len(),
            ));
        }

        let t_min = &arg_types[0];
        let t_max = &arg_types[1];

        if !is_valid_bound_type(t_min) {
            return Err(unsupported_data_type_exec_err(
                "uniform",
                "NUMERIC type for min",
                t_min,
            ));
        }
        if !is_valid_bound_type(t_max) {
            return Err(unsupported_data_type_exec_err(
                "uniform",
                "NUMERIC type for max",
                t_max,
            ));
        }

        // Each bound keeps its own type, since Spark casts it to `DOUBLE` directly.
        let mut coerced_types = vec![t_min.clone(), t_max.clone()];

        if arg_types.len() == 3 {
            // Spark accepts only INT or BIGINT for the seed. Both are coerced to
            // Int64 internally so the RNG always receives a 64-bit seed.
            let seed_type = match &arg_types[2] {
                DataType::Int32 | DataType::Int64 => DataType::Int64,
                DataType::Null => DataType::Null,
                _ => {
                    return Err(unsupported_data_types_exec_err(
                        "uniform",
                        "INT or BIGINT type for seed",
                        &arg_types[2..],
                    ));
                }
            };
            coerced_types.push(seed_type);
        }

        Ok(coerced_types)
    }
}

/// Returns true if `dt` is an acceptable min/max bound type for `uniform`:
/// any numeric (integer, float, decimal) plus the literal NULL type.
///
/// `Decimal256` is only accepted when its precision and scale fit in
/// `Decimal128`, since `calculate_output_type` collapses all decimal paths to
/// `Decimal128`. Rejecting oversized `Decimal256` here — instead of silently
/// falling back to `Float64` downstream — keeps `uniform` consistent with
/// other math UDFs (e.g. ceil/floor) and surfaces precision loss early.
fn is_valid_bound_type(dt: &DataType) -> bool {
    dt.is_integer()
        || matches!(dt, DataType::Float32 | DataType::Float64)
        || matches!(
            dt,
            DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _)
        )
        || matches!(
            dt,
            DataType::Decimal256(precision, scale)
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE
        )
        || matches!(dt, DataType::Null)
}

/// Build a single Spark-compatible RNG for the whole invocation.
///
/// Spark's `Uniform` expression instantiates `XORShiftRandom(seed)` once and
/// advances it across rows, so a multi-row batch with a fixed seed produces
/// a sequence of distinct, bit-for-bit reproducible values rather than the
/// same value repeated. When the caller did not supply a seed we draw a
/// random `i64` from the thread RNG so different invocations still differ.
///
/// A call with a seed is usually evaluated by a per-partition generator instead
/// (see `partition_random`), which adds the partition index to the seed and
/// continues across batches as Spark does.
fn build_rng(seed: Option<u64>) -> SparkXorShiftRandom {
    let s: i64 = match seed {
        Some(v) => v as i64,
        None => rng().random(),
    };
    SparkXorShiftRandom::new(s)
}

#[inline]
fn extract_seed(seed_array: Option<&ArrayRef>, i: usize) -> Option<u64> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Int64Type, UInt64Type};

    seed_array.and_then(|arr| {
        if arr.is_null(i) {
            // A null seed is zero, as in Spark.
            Some(0)
        } else {
            match arr.data_type() {
                DataType::Int64 => Some(arr.as_primitive::<Int64Type>().value(i) as u64),
                DataType::UInt64 => Some(arr.as_primitive::<UInt64Type>().value(i)),
                _ => None,
            }
        }
    })
}

fn uniform(args: &[ArrayRef], number_rows: usize, output_type: &DataType) -> Result<ArrayRef> {
    // Empty batch: return an empty typed array. We also skip seed extraction,
    // which would otherwise index row 0 of a zero-length array and panic.
    if number_rows == 0 {
        return Ok(new_null_array(output_type, 0));
    }

    // The seed is required to be foldable, so it is invariant across rows; we
    // build a single RNG here and advance it as we generate each row.
    let seed_val = extract_seed(args.get(2), 0);
    let mut rng = build_rng(seed_val);

    uniform_with_rng(&args[0], &args[1], number_rows, output_type, &mut rng)
}

/// Generates the uniform values for one batch, as Spark replaces `uniform` with
/// `CAST(CAST(min AS DOUBLE) + (CAST(max AS DOUBLE) - CAST(min AS DOUBLE)) * rand(seed) AS <type>)`.
/// The bounds keep their own types, since each of them is cast to `DOUBLE` directly.
pub(crate) fn uniform_with_rng(
    min_array: &ArrayRef,
    max_array: &ArrayRef,
    number_rows: usize,
    output_type: &DataType,
    rng: &mut SparkXorShiftRandom,
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
        let mut rng = SparkXorShiftRandom::new(42);
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
        let mut rng = SparkXorShiftRandom::new(0);
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

    #[test]
    fn test_output_decimal_type_follows_spark() {
        // `typeof(uniform(CAST(1.1 AS DECIMAL(5,4)), CAST(9.9 AS DECIMAL(4,1)), 7))` in Spark 4.2.
        assert_eq!(SparkUniform::calculate_decimal_output(5, 4, 4, 1), (4, 1));
        assert_eq!(SparkUniform::calculate_decimal_output(4, 1, 5, 4), (5, 4));
        assert_eq!(SparkUniform::calculate_decimal_output(3, 2, 5, 1), (5, 1));
    }

    #[test]
    fn test_return_field_rejects_invalid_arity() {
        let udf = SparkUniform::new();
        let int32 = Arc::new(Field::new("x", DataType::Int32, false));

        for arity in [0usize, 1, 4, 5] {
            let arg_fields: Vec<_> = (0..arity).map(|_| int32.clone()).collect();
            let scalar_arguments: Vec<Option<&datafusion_common::ScalarValue>> =
                (0..arity).map(|_| None).collect();
            let args = ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            };
            let result = udf.return_field_from_args(args);
            assert!(result.is_err(), "expected error for arity {arity}, got Ok");
            let msg = match result {
                Err(e) => e.to_string(),
                Ok(_) => continue,
            };
            assert!(msg.contains("uniform"), "got: {msg}");
            assert!(msg.contains(&arity.to_string()), "got: {msg}");
        }
    }
}
