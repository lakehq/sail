use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{RngExt, rng};

use super::xorshift::SparkXorShiftRandom;
use crate::error::{generic_exec_err, unsupported_data_type_exec_err};

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
}

impl ScalarUDFImpl for SparkUniform {
    fn name(&self) -> &str {
        "uniform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_math_spark_uniform::return_type(_arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_math_spark_uniform::return_field_from_args(args)
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
        sail_type_system::policies::scalar_math_spark_uniform::coerce_types(arg_types)
    }
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
    use arrow_schema::Field;

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
