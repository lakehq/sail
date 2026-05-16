use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{rng, RngExt};

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
/// Scaling mirrors Spark's `Uniform` expression:
/// - **Integer**: `min + floor(nextDouble() * (max - min))` in `i64` space.
/// - **Float / Double**: `min + nextDouble() * (max - min)` in `f64`, cast
///   to the target type (so `FLOAT + FLOAT` yields an `f32` whose bits match
///   Spark's `Float.intBitsToFloat` output).
/// - **Decimal**: double-space interpolation — the scaled `i128` bounds are
///   converted to `f64` via `value / 10^scale`, interpolated with
///   `nextDouble`, then `(result * 10^scale).round() as i128`. The rounding
///   step is what makes `uniform(5.5, 10.5, 123)` round to `6.3` rather than
///   truncating to `6.2`, matching Spark.
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
    /// Follows Spark's behavior: use the complete type (precision AND scale) of the larger precision
    fn calculate_decimal_output(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
        if p1 > p2 {
            (p1, s1)
        } else if p2 > p1 {
            (p2, s2)
        } else {
            // Same precision → use max scale
            (p1, s1.max(s2))
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
                // When one decimal has significantly larger precision, use that decimal's
                // type completely (both precision AND scale): e.g. Decimal(2,1) + Decimal(20,0)
                // → Decimal(20,0), not Decimal(20,1).
                let (precision, scale) = Self::calculate_decimal_output(p1, s1, p2, s2);
                Decimal128(precision, scale)
            }
            (Some((p, s)), None) | (None, Some((p, s))) => Decimal128(p, s),
            (None, None) => Float64,
        }
    }
}

impl ScalarUDFImpl for SparkUniform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uniform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
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

        let output_type: DataType = Self::calculate_output_type(t_min, t_max);

        let mut coerced_types = vec![output_type.clone(), output_type];

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
                    ))
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
/// NOTE: Spark also adds `partitionIndex` to the seed to decorrelate
/// partitions at runtime. DataFusion does not expose a per-partition index
/// to scalar UDFs today, so we use the user-supplied seed verbatim. Within
/// a single batch (one `invoke_with_args` call) the sequence is bit-exact
/// vs Spark JVM; across batches of the same logical partition the sequence
/// restarts (tracked separately in #1266).
fn build_rng(seed: Option<u64>) -> SparkXorShiftRandom {
    let s: i64 = match seed {
        Some(v) => v as i64,
        None => rng().random(),
    };
    SparkXorShiftRandom::new(s)
}

/// Compute Spark's uniform integer scaling: `min + floor(nextDouble * (max - min))`.
///
/// The formula is applied verbatim — Spark does NOT normalize bounds. For
/// `uniform(20, 10, 0)` the span `max - min = -10` is negative, so
/// `floor(nextDouble * -10)` is negative and the result stays in `(max, min]`.
/// When `min == max` we short-circuit to skip the RNG advance; the scaled
/// offset would be zero anyway.
macro_rules! generate_uniform_int_fn {
    ($fn_name:ident, $type:ty) => {
        #[inline]
        fn $fn_name(min: $type, max: $type, rng: &mut SparkXorShiftRandom) -> $type {
            if min == max {
                return min;
            }
            let span = (max as f64) - (min as f64);
            let scaled = (rng.next_double() * span).floor() as i64;
            ((min as i64) + scaled) as $type
        }
    };
}

generate_uniform_int_fn!(generate_uniform_int8_single, i8);
generate_uniform_int_fn!(generate_uniform_int16_single, i16);
generate_uniform_int_fn!(generate_uniform_int32_single, i32);
generate_uniform_int_fn!(generate_uniform_int64_single, i64);

/// Spark's uniform float scaling computes in `f64` and casts to the target
/// float type at the end, so `FLOAT + FLOAT` produces `f32` values whose exact
/// bits match Spark's `Float.intBitsToFloat(...)` output. Bounds are NOT
/// normalized — swapped bounds yield a negative span, matching Spark.
#[inline]
fn generate_uniform_float_single(min: f64, max: f64, rng: &mut SparkXorShiftRandom) -> f64 {
    if min == max {
        return min;
    }
    min + rng.next_double() * (max - min)
}

#[inline]
fn generate_uniform_float32_single(min: f32, max: f32, rng: &mut SparkXorShiftRandom) -> f32 {
    generate_uniform_float_single(min as f64, max as f64, rng) as f32
}

#[inline]
fn extract_seed(seed_array: Option<&ArrayRef>, i: usize) -> Option<u64> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Int64Type, UInt64Type};

    seed_array.and_then(|arr| {
        if arr.is_null(i) {
            None
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
    use datafusion::arrow::array::AsArray;

    let min_array = &args[0];
    let max_array = &args[1];
    let seed_array = args.get(2);

    // Empty batch: return an empty typed array. We also skip seed extraction,
    // which would otherwise index row 0 of a zero-length array and panic.
    if number_rows == 0 {
        return Ok(new_null_array(output_type, 0));
    }

    // Fast path: if either bound is fully null, the result is all-null — every
    // per-row branch would fall through to `append_null()` anyway. Skip the
    // type dispatch entirely and allocate the null array directly.
    if min_array.null_count() == min_array.len() || max_array.null_count() == max_array.len() {
        return Ok(new_null_array(output_type, number_rows));
    }

    // The seed is required to be foldable, so it is invariant across rows; we
    // build a single RNG here and advance it as we generate each row.
    let seed_val = extract_seed(seed_array, 0);
    let mut rng = build_rng(seed_val);

    match *output_type {
        DataType::Int8 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();

            let mut builder = Int8Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder.append_value(generate_uniform_int8_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();

            let mut builder = Int16Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder.append_value(generate_uniform_int16_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Int32Type>();

            let mut builder = Int32Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder.append_value(generate_uniform_int32_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();

            let mut builder = Int64Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder.append_value(generate_uniform_int64_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();

            let mut builder = Float32Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder
                        .append_value(generate_uniform_float32_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();

            let mut builder = Float64Array::builder(number_rows);
            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    builder.append_value(generate_uniform_float_single(min_val, max_val, &mut rng));
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let min_arr = min_array.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
            let max_arr = max_array.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();

            let mut builder =
                Decimal128Array::builder(number_rows).with_precision_and_scale(precision, scale)?;

            for i in 0..number_rows {
                if min_arr.is_null(i) || max_arr.is_null(i) {
                    builder.append_null();
                    continue;
                }

                let min_val = min_arr.value(i);
                let max_val = max_arr.value(i);

                // Spark routes decimal through double-space: convert bounds to
                // `double`, interpolate with `nextDouble`, cast back to scaled
                // `i128`. The `.round()` (not `.floor()`) step is what makes
                // `uniform(5.5, 10.5, 123) → 6.3` match Spark rather than
                // truncating to `6.2`. Bounds are NOT normalized — swapped
                // bounds yield a negative span, matching Spark.
                let decimal_val = if min_val == max_val {
                    min_val
                } else {
                    let scale_factor = 10f64.powi(scale as i32);
                    let min_f = (min_val as f64) / scale_factor;
                    let max_f = (max_val as f64) / scale_factor;
                    let result_f = min_f + rng.next_double() * (max_f - min_f);
                    (result_f * scale_factor).round() as i128
                };

                builder.append_value(decimal_val);
            }

            Ok(Arc::new(builder.finish()))
        }
        _ => Err(unsupported_data_type_exec_err(
            "uniform",
            "Integer, Float, or Decimal array",
            output_type,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
