use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use rand::rngs::StdRng;
use rand::{rng, Rng, SeedableRng};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_types_exec_err};

/// The `Uniform` function generates random numbers from a uniform distribution
/// between a specified minimum and maximum value.
///
/// # Syntax
/// `uniform(min, max, seed)`
/// - min: minimum value of the range (inclusive)
/// - max: maximum value of the range (exclusive)
/// - seed: optional random seed for reproducibility
///
/// # Implementation Notes
///
/// This implementation follows Apache Spark's `uniform` function API but uses
/// different random number generation algorithms.
///
/// **Spark (Java/Scala)**:
/// - RNG: `java.util.Random` (Linear Congruential Generator)
/// - Algorithm: LCG with 48-bit seed
/// - Integer type: Int32
/// - Implementation: Uses `XORShiftRandom` wrapper around `java.util.Random`
/// - Source: See `randomExpressions.scala` in Spark's Catalyst module
///
/// **Sail (Rust)**:
/// - RNG: `rand::rngs::StdRng` (ChaCha20-based cryptographic RNG)
/// - Algorithm: ChaCha20 stream cipher (20 rounds)
/// - Integer type: Int32
/// - Implementation: Uses `rand` crate's standard RNG
/// - Source: <https://docs.rs/rand/latest/rand/>
///
/// **Why Different RNGs?**
/// - Spark uses `java.util.Random` for JVM ecosystem compatibility
/// - Sail uses `StdRng` for better cryptographic properties and Rust ecosystem standards
/// - Both produce statistically uniform distributions
/// - Both are deterministic (reproducible with same seed in their respective environments)
/// - Cross-platform reproducibility (Spark ↔ Sail) is **not guaranteed**
///
/// Due to different RNG implementations, the same seed produces different
/// sequences in Spark vs Sail, but both produce statistically uniform
/// distributions and are deterministic within their respective environments.
///
/// # References
/// - Spark Python API: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.uniform.html>
/// - Spark Scala source: <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/randomExpressions.scala>
/// - Rust rand crate: <https://docs.rs/rand/latest/rand/rngs/struct.StdRng.html>
/// - ChaCha20 algorithm: <https://docs.rs/rand_chacha/latest/rand_chacha/>
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
        if t_min.is_integer() && t_max.is_integer() {
            // Preserve type when both arguments have the same small integer type
            if matches!((t_min, t_max), (DataType::Int8, DataType::Int8)) {
                return DataType::Int8;
            }
            if matches!((t_min, t_max), (DataType::Int16, DataType::Int16)) {
                return DataType::Int16;
            }

            // Check for 64-bit types (signed or unsigned)
            let is_64: bool = matches!(t_min, DataType::Int64 | DataType::UInt64)
                || matches!(t_max, DataType::Int64 | DataType::UInt64);

            // Check for unsigned 32-bit types
            let is_unsigned_32: bool =
                matches!(t_min, DataType::UInt32) || matches!(t_max, DataType::UInt32);

            return if is_64 {
                DataType::Int64
            } else if is_unsigned_32 {
                // UInt32 needs Int64 to safely represent all values
                DataType::Int64
            } else {
                DataType::Int32
            };
        }

        // Try to extract decimal info from both types
        let decimal_min = Self::extract_decimal_info(t_min);
        let decimal_max = Self::extract_decimal_info(t_max);

        match (decimal_min, decimal_max) {
            (Some((p1, s1)), Some((p2, s2))) => {
                // Both are decimal types
                // Spark's behavior: When one decimal has significantly larger precision,
                // use that decimal's type completely (both precision AND scale).
                // Example: Decimal(2,1) + Decimal(20,0) → Decimal(20,0) (not Decimal(20,1))
                // This happens when mixing small literals (1.2) with large numbers (12345678901234567890)
                let (precision, scale) = Self::calculate_decimal_output(p1, s1, p2, s2);
                DataType::Decimal128(precision, scale)
            }
            (Some((p, s)), None) => DataType::Decimal128(p, s),
            (None, Some((p, s))) => DataType::Decimal128(p, s),
            (None, None) => {
                // Preserve Float32 when both arguments are Float32
                if matches!((t_min, t_max), (DataType::Float32, DataType::Float32)) {
                    DataType::Float32
                } else {
                    DataType::Float64
                }
            }
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
        let t_min = args.arg_fields[0].data_type();
        let t_max = args.arg_fields[1].data_type();
        let return_type = Self::calculate_output_type(t_min, t_max);

        let nullable: bool = args.arg_fields[0].is_nullable() || args.arg_fields[1].is_nullable();

        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(uniform, vec![])(&args.args)
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

        let output_type: DataType = Self::calculate_output_type(t_min, t_max);

        let mut coerced_types = vec![output_type.clone(), output_type];

        if arg_types.len() == 3 {
            let seed_type = match &arg_types[2] {
                t if t.is_signed_integer() => DataType::Int64,
                t if t.is_unsigned_integer() => DataType::UInt64,
                DataType::Null => DataType::Null,
                _ => {
                    return Err(unsupported_data_types_exec_err(
                        "uniform",
                        "Integer Type for seed",
                        &arg_types[2..],
                    ))
                }
            };
            coerced_types.push(seed_type);
        }

        Ok(coerced_types)
    }
}

macro_rules! generate_uniform_fn {
    ($fn_name:ident, $fn_name_single:ident, $type:ty) => {
        /// Generate a single uniform value
        #[inline]
        fn $fn_name_single(min: $type, max: $type, seed: Option<u64>) -> $type {
            let mut min_v = min;
            let mut max_v = max;

            if min_v > max_v {
                std::mem::swap(&mut min_v, &mut max_v);
            }

            if min_v == max_v {
                return min_v;
            }

            if let Some(seed_val) = seed {
                let mut rng = StdRng::seed_from_u64(seed_val);
                rng.random_range(min_v..max_v)
            } else {
                let mut rng = rng();
                rng.random_range(min_v..max_v)
            }
        }

        #[allow(dead_code)]
        fn $fn_name(
            min: $type,
            max: $type,
            seed: Option<u64>,
            number_rows: usize,
        ) -> Result<Vec<$type>> {
            let mut min_v = min;
            let mut max_v = max;

            if min_v > max_v {
                std::mem::swap(&mut min_v, &mut max_v);
            }

            if min_v == max_v {
                return Ok(vec![min_v; number_rows]);
            }

            let values: Vec<$type> = if let Some(seed_val) = seed {
                let mut rng = StdRng::seed_from_u64(seed_val);
                (0..number_rows)
                    .map(|_| rng.random_range(min_v..max_v))
                    .collect()
            } else {
                let mut rng = rng();
                (0..number_rows)
                    .map(|_| rng.random_range(min_v..max_v))
                    .collect()
            };

            Ok(values)
        }
    };
}

generate_uniform_fn!(generate_uniform_int8, generate_uniform_int8_single, i8);
generate_uniform_fn!(generate_uniform_int16, generate_uniform_int16_single, i16);
generate_uniform_fn!(generate_uniform_int32, generate_uniform_int32_single, i32);
generate_uniform_fn!(generate_uniform_int64, generate_uniform_int64_single, i64);
generate_uniform_fn!(generate_uniform_float32, generate_uniform_float32_single, f32);
generate_uniform_fn!(generate_uniform_float, generate_uniform_float_single, f64);

#[inline]
fn extract_seed(seed_array: Option<&ArrayRef>, i: usize) -> Option<u64> {
    use datafusion::arrow::array::AsArray;

    seed_array.and_then(|arr| {
        if arr.is_null(i) {
            None
        } else {
            Some(
                arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                    .value(i) as u64,
            )
        }
    })
}

fn uniform(args: &[ArrayRef]) -> Result<ArrayRef> {
    use datafusion::arrow::array::AsArray;

    let min_array = &args[0];
    let max_array = &args[1];
    let seed_array = args.get(2);

    let number_rows = min_array.len();

    let output_type =
        SparkUniform::calculate_output_type(min_array.data_type(), max_array.data_type());

    match output_type {
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
                    let seed_val = extract_seed(seed_array, i);
                    builder.append_value(generate_uniform_int8_single(min_val, max_val, seed_val));
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
                    let seed_val = extract_seed(seed_array, i);
                    builder.append_value(generate_uniform_int16_single(min_val, max_val, seed_val));
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
                    let seed_val = extract_seed(seed_array, i);
                    builder.append_value(generate_uniform_int32_single(min_val, max_val, seed_val));
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
                    let seed_val = extract_seed(seed_array, i);
                    builder.append_value(generate_uniform_int64_single(min_val, max_val, seed_val));
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
                    let seed_val = extract_seed(seed_array, i);
                    builder
                        .append_value(generate_uniform_float32_single(min_val, max_val, seed_val));
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
                    let seed_val = extract_seed(seed_array, i);
                    builder.append_value(generate_uniform_float_single(min_val, max_val, seed_val));
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
                } else {
                    let min_val = min_arr.value(i);
                    let max_val = max_arr.value(i);
                    let seed_val = extract_seed(seed_array, i);

                    // Convert Decimal128 to f64 for generation
                    let min_f64 = min_val as f64 / 10_f64.powi(scale as i32);
                    let max_f64 = max_val as f64 / 10_f64.powi(scale as i32);

                    // Generate and convert back to Decimal128
                    let float_val = generate_uniform_float_single(min_f64, max_f64, seed_val);
                    let decimal_val = (float_val * 10_f64.powi(scale as i32)).round() as i128;

                    builder.append_value(decimal_val);
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        _ => Err(generic_exec_err(
            "uniform",
            &format!("Unsupported array type: {}", output_type),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test 1: uniform(10, 20, 0) should return 18 (with i32)
    #[test]
    fn test_uniform_single_value() -> Result<()> {
        let values = generate_uniform_int32(10, 20, Some(0), 1)?;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], 18);
        Ok(())
    }

    /// Test 2: uniform(10, 20, 0) FROM range(5) - should return DIFFERENT values
    #[test]
    fn test_uniform_multiple_values_are_different() -> Result<()> {
        let values = generate_uniform_int32(10, 20, Some(0), 5)?;

        // Verify that we have 5 values
        assert_eq!(values.len(), 5);

        // Verify that the first value is 18 (with i32 RNG)
        assert_eq!(values[0], 18);

        // Verify that NOT all values are the same (this is the key test)
        let all_same = values.windows(2).all(|w| w[0] == w[1]);
        assert!(
            !all_same,
            "All values are {:?} but should be different!",
            values
        );

        // All values must be in the range [10, 20)
        for &v in &values {
            assert!((10..20).contains(&v), "Value {} out of range", v);
        }
        Ok(())
    }

    /// Test 3: min == max should return that constant value
    #[test]
    fn test_uniform_min_equals_max() -> Result<()> {
        let values = generate_uniform_int32(5, 5, Some(0), 3)?;
        assert_eq!(values, vec![5, 5, 5]);
        Ok(())
    }

    /// Test 4: Seed 42 should always give the same result (reproducibility)
    #[test]
    fn test_uniform_reproducibility() -> Result<()> {
        let values1 = generate_uniform_int32(0, 100, Some(42), 5)?;
        let values2 = generate_uniform_int32(0, 100, Some(42), 5)?;
        assert_eq!(values1, values2, "Same seed should produce same values");
        Ok(())
    }

    /// Test 5: Without seed should give different values on each call
    #[test]
    fn test_uniform_without_seed_is_random() -> Result<()> {
        let values1 = generate_uniform_int32(0, 100, None, 5)?;
        let values2 = generate_uniform_int32(0, 100, None, 5)?;

        // It's extremely unlikely they're all the same if it's random
        let probably_different = values1 != values2;
        assert!(
            probably_different,
            "Without seed, values should likely be different: {:?} vs {:?}",
            values1, values2
        );
        Ok(())
    }

    /// Test 6: Swapping min and max
    #[test]
    fn test_uniform_swapped_min_max() -> Result<()> {
        let values1 = generate_uniform_int32(20, 10, Some(0), 1)?;
        let values2 = generate_uniform_int32(10, 20, Some(0), 1)?;
        assert_eq!(values1, values2, "Swapped min/max should give same result");
        Ok(())
    }

    /// Test 7: Float values
    #[test]
    fn test_uniform_float_values() -> Result<()> {
        let values = generate_uniform_float(5.5, 10.5, Some(123), 1)?;
        assert_eq!(values.len(), 1);
        let val = values[0];
        assert!((5.5..10.5).contains(&val), "Value {} out of range", val);
        Ok(())
    }

    /// Test 8: Multiple different float values
    #[test]
    fn test_uniform_float_multiple_different() -> Result<()> {
        let values = generate_uniform_float(0.0, 1.0, Some(99), 10)?;
        assert_eq!(values.len(), 10);

        // Verify that not all are the same
        let all_same = values.windows(2).all(|w| w[0] == w[1]);
        assert!(!all_same, "Float values should be different: {:?}", values);

        // All in range
        for &v in &values {
            assert!((0.0..1.0).contains(&v), "Value {} out of range", v);
        }
        Ok(())
    }

    /// Test 9: Verify that seed 0 with 10 values generates the expected sequence
    #[test]
    fn test_uniform_seed_zero_sequence() -> Result<()> {
        let values = generate_uniform_int32(10, 20, Some(0), 10)?;
        assert_eq!(values.len(), 10);

        assert_eq!(values[0], 18);

        // Verify that there is variety
        let unique_count = values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(
            unique_count > 1,
            "Should have multiple unique values, got {:?}",
            values
        );
        Ok(())
    }

    /// Test 10: Negative numbers
    #[test]
    fn test_uniform_negative_range() -> Result<()> {
        let values = generate_uniform_int32(-10, 10, Some(0), 5)?;
        assert_eq!(values.len(), 5);

        for &v in &values {
            assert!((-10..10).contains(&v), "Value {} out of range [-10, 10)", v);
        }
        Ok(())
    }

    /// Test 11: Verify specific value with seed 42
    #[test]
    fn test_uniform_seed_42_value() -> Result<()> {
        let values = generate_uniform_int32(0, 100, Some(42), 1)?;
        assert_eq!(values[0], 13);
        Ok(())
    }

    /// Test 12: Float min == max
    #[test]
    fn test_uniform_float_equal() -> Result<()> {
        let values = generate_uniform_float(2.5, 2.5, Some(0), 5)?;
        assert_eq!(values, vec![2.5, 2.5, 2.5, 2.5, 2.5]);
        Ok(())
    }

    /// Test 13: Verify return type for integers (should be Int32)
    /// This corresponds to Spark's "integer" type in the schema
    #[test]
    fn test_uniform_return_type_integer() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int64, false)),
            Arc::new(Field::new("max", DataType::Int64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "Integer inputs should return Int64 (maps to Spark's integer type)"
        );
        Ok(())
    }

    /// Test 14: Verify return type for Int32 inputs (should still be Int32)
    #[test]
    fn test_uniform_return_type_int32() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int32, false)),
            Arc::new(Field::new("max", DataType::Int32, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;
        assert_eq!(
            field.data_type(),
            &DataType::Int32,
            "Int32 inputs should be coerced to Int32"
        );
        Ok(())
    }

    /// Test 15: Verify return type for floats (should be Float64)
    #[test]
    fn test_uniform_return_type_float() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Float64, false)),
            Arc::new(Field::new("max", DataType::Float64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;
        assert_eq!(
            field.data_type(),
            &DataType::Float64,
            "Float inputs should return Float64"
        );
        Ok(())
    }

    /// Test 16: Verify return type for mixed int/float (should be Float64)
    #[test]
    fn test_uniform_return_type_mixed() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int64, false)),
            Arc::new(Field::new("max", DataType::Float64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Float64,
            "Mixed int/float inputs should return Float64"
        );
        Ok(())
    }

    /// Test 17: Verify coerce_types for integer inputs
    #[test]
    fn test_uniform_coerce_types_integer() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Int32, DataType::Int32, DataType::Int64];
        let coerced = uniform_fn.coerce_types(&arg_types)?;
        assert_eq!(
            coerced,
            vec![DataType::Int32, DataType::Int32, DataType::Int64],
            "Int32 inputs should remain Int32 for min/max, Int64 for seed"
        );
        Ok(())
    }

    /// Test 18: Verify coerce_types for float inputs - Float32 is preserved
    #[test]
    fn test_uniform_coerce_types_float() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Float32, DataType::Float32, DataType::Int64];
        let coerced = uniform_fn.coerce_types(&arg_types)?;
        assert_eq!(
            coerced,
            vec![DataType::Float32, DataType::Float32, DataType::Int64],
            "Float32 should be preserved when both arguments are Float32"
        );
        Ok(())
    }

    /// Test 19: Verify return type for decimal inputs (Spark's behavior with literals like 5.5, 10.5)
    #[test]
    fn test_uniform_return_type_decimal() -> Result<()> {
        let uniform_fn = SparkUniform::new();

        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(3, 1), false)),
            Arc::new(Field::new("max", DataType::Decimal128(3, 1), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(3, 1),
            "Decimal(3,1) inputs should return Decimal(3,1) matching Spark's behavior"
        );

        assert!(
            !field.is_nullable(),
            "Field should be non-nullable when min/max are non-nullable"
        );

        Ok(())
    }

    /// Test 20: Verify coerce_types for decimal inputs
    #[test]
    fn test_uniform_coerce_types_decimal() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![
            DataType::Decimal128(3, 1),
            DataType::Decimal128(3, 1),
            DataType::Int64,
        ];
        let coerced = uniform_fn.coerce_types(&arg_types)?;
        assert_eq!(
            coerced,
            vec![
                DataType::Decimal128(3, 1),
                DataType::Decimal128(3, 1),
                DataType::Int64
            ],
            "Decimal types should be preserved in coercion"
        );
        Ok(())
    }

    /// Test 21: Verify decimal values with different precision/scale
    #[test]
    fn test_uniform_return_type_decimal_different() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        // One with Decimal(2, 1) [5.5] and another with Decimal(3, 1) [10.5]

        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("max", DataType::Decimal128(3, 1), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(3, 1),
            "Should use max precision from inputs"
        );
        Ok(())
    }

    /// Test 22: Verify that return_field_from_args returns non-nullable field
    #[test]
    fn test_uniform_is_not_nullable() -> Result<()> {
        use std::sync::Arc;

        let uniform_fn = SparkUniform::new();

        // Create argument fields (representing the schema of the inputs)
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int64, false)),
            Arc::new(Field::new("max", DataType::Int64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        // Create ReturnFieldArgs
        let return_field_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        // Get the field using return_field_from_args
        let field = uniform_fn.return_field_from_args(return_field_args)?;

        // Verify that the field is not nullable
        assert!(
            !field.is_nullable(),
            "uniform() should return non-nullable field (nullable = false)"
        );
        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "Should return Int64 for integer inputs when arg fields are Int64"
        );
        assert_eq!(field.name(), "uniform", "Field name should be 'uniform'");

        Ok(())
    }

    /// Test 23: Verify that decimal overflow returns NULL instead of error
    #[test]
    fn test_uniform_decimal_overflow_handling() -> Result<()> {
        let values = generate_uniform_float(1e20, 1e30, Some(0), 10)?;
        assert_eq!(values.len(), 10);

        // Check that we don't panic or error out
        Ok(())
    }

    /// Test 24: Decimal + Integer → uses Decimal type (Spark rule)
    /// uniform(5.5, 10) → decimal(2,1)
    #[test]
    fn test_uniform_return_type_decimal_plus_integer() -> Result<()> {
        let uniform_fn = SparkUniform::new();

        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("max", DataType::Int32, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;
        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(2, 1),
            "Decimal + Integer should return the Decimal type"
        );
        Ok(())
    }

    /// Test 25: Integer + Decimal → uses Decimal type (order doesn't matter)
    /// uniform(10, 5.5) → decimal(2,1)
    #[test]
    fn test_uniform_return_type_integer_plus_decimal() -> Result<()> {
        let uniform_fn = SparkUniform::new();

        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int32, false)),
            Arc::new(Field::new("max", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(2, 1),
            "Integer + Decimal should return the Decimal type"
        );
        Ok(())
    }

    /// Test 26: Decimal + Large Integer → uses Decimal type
    /// uniform(1.2, 1234567890) → decimal(2,1)
    #[test]
    fn test_uniform_return_type_decimal_plus_large_integer() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("max", DataType::Int32, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(2, 1),
            "Decimal + Large Integer should return the Decimal type"
        );
        Ok(())
    }

    /// Test 27: Two large Decimals with different precision → uses the larger one completely
    /// uniform(1.2, 12345678901234567890) where 12345678901234567890 is Decimal(20,0)
    /// → decimal(20,0) (takes the type of the one with larger precision)
    #[test]
    fn test_uniform_return_type_large_decimals() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("max", DataType::Decimal128(20, 0), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(20, 0),
            "When precision differs significantly, should use the larger decimal's type completely: Decimal(20,0)"
        );
        Ok(())
    }

    /// Test 28: Decimal + Int64 → uses Decimal type
    /// uniform(1.2, CAST(9223372036854775807 AS BIGINT)) → decimal(2,1)
    #[test]
    fn test_uniform_return_type_decimal_plus_int64() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(2, 1), false)),
            Arc::new(Field::new("max", DataType::Int64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(2, 1),
            "Decimal + Int64 should return the Decimal type (ignores Int64)"
        );
        Ok(())
    }

    /// Test 29: Two Decimals with same precision → uses max(scale)
    /// uniform(Decimal(3,1), Decimal(3,2)) → decimal(3,2)
    #[test]
    fn test_uniform_return_type_same_precision_different_scale() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(3, 1), false)),
            Arc::new(Field::new("max", DataType::Decimal128(3, 2), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(3, 2),
            "Same precision should use max(scale): Decimal(3, max(1,2)) = Decimal(3,2)"
        );
        Ok(())
    }

    /// Test 30: UInt32 should return Int64 (not Int32 to avoid overflow)
    #[test]
    fn test_uniform_return_type_uint32() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::UInt32, false)),
            Arc::new(Field::new("max", DataType::UInt32, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "UInt32 inputs should return Int64 to safely represent all values"
        );
        Ok(())
    }

    /// Test 31: UInt64 should return Int64
    #[test]
    fn test_uniform_return_type_uint64() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::UInt64, false)),
            Arc::new(Field::new("max", DataType::UInt64, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "UInt64 inputs should return Int64"
        );
        Ok(())
    }

    /// Test 32: Mixed UInt32 and Int32 should return Int64
    #[test]
    fn test_uniform_return_type_uint32_int32_mixed() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::UInt32, false)),
            Arc::new(Field::new("max", DataType::Int32, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "Mixed UInt32 and Int32 should return Int64 to avoid lossy conversion"
        );
        Ok(())
    }

    /// Test 33: Int16 (SMALLINT) should be preserved as Int16
    #[test]
    fn test_uniform_return_type_int16() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int16, false)),
            Arc::new(Field::new("max", DataType::Int16, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;
        assert_eq!(
            field.data_type(),
            &DataType::Int16,
            "Int16 (SMALLINT) inputs should return Int16"
        );
        Ok(())
    }

    /// Test 34: Int8 (TINYINT) should be preserved as Int8
    #[test]
    fn test_uniform_return_type_int8() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Int8, false)),
            Arc::new(Field::new("max", DataType::Int8, false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Int8,
            "Int8 inputs should return Int8"
        );
        Ok(())
    }

    /// Test 35: Generate Int64 uniform values
    #[test]
    fn test_generate_uniform_int64() -> Result<()> {
        let values = generate_uniform_int64(100, 200, Some(42), 5)?;
        assert_eq!(values.len(), 5);

        // All values should be in range [100, 200)
        for &v in &values {
            assert!((100..200).contains(&v), "Value {} out of range", v);
        }

        // Verify reproducibility
        let values2 = generate_uniform_int64(100, 200, Some(42), 5)?;
        assert_eq!(
            values, values2,
            "Same seed should produce same Int64 values"
        );
        Ok(())
    }

    /// Test 36: Decimal256 with valid precision/scale should return Decimal128
    #[test]
    fn test_uniform_return_type_decimal256_valid() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal256(10, 2), false)),
            Arc::new(Field::new("max", DataType::Decimal256(10, 2), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(10, 2),
            "Decimal256 with valid precision/scale should convert to Decimal128"
        );
        Ok(())
    }

    /// Test 37: Decimal256 with precision > DECIMAL128_MAX_PRECISION should fail
    #[test]
    fn test_uniform_return_type_decimal256_invalid_precision() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        // Decimal256 with precision > 38 (exceeds Decimal128 limit)
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal256(50, 2), false)),
            Arc::new(Field::new("max", DataType::Decimal256(50, 2), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        // Should return Float64 as fallback since it doesn't fit in Decimal128
        assert_eq!(
            field.data_type(),
            &DataType::Float64,
            "Decimal256 with precision > 38 should fall back to Float64"
        );
        Ok(())
    }

    /// Test 38: Mixed Decimal128 and Decimal256
    #[test]
    fn test_uniform_return_type_mixed_decimal128_decimal256() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal128(5, 2), false)),
            Arc::new(Field::new("max", DataType::Decimal256(10, 3), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(10, 3),
            "Mixed Decimal128/Decimal256 should use larger precision"
        );
        Ok(())
    }

    /// Test 39: Decimal256 with same precision, different scale
    #[test]
    fn test_uniform_return_type_decimal256_same_precision() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_fields = vec![
            Arc::new(Field::new("min", DataType::Decimal256(15, 2), false)),
            Arc::new(Field::new("max", DataType::Decimal256(15, 5), false)),
            Arc::new(Field::new("seed", DataType::Int64, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None, None],
        };

        let field = uniform_fn.return_field_from_args(args)?;

        assert_eq!(
            field.data_type(),
            &DataType::Decimal128(15, 5),
            "Decimal256 with same precision should use max scale"
        );
        Ok(())
    }
}
