use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Decimal128Array, Float64Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
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

    fn calculate_output_type(t_min: &DataType, t_max: &DataType) -> DataType {
        if t_min.is_integer() && t_max.is_integer() {
            return DataType::Int32;
        }

        match (t_min, t_max) {
            (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                let precision = (*p1).max(*p2);
                let scale = (*s1).max(*s2);
                DataType::Decimal128(precision, scale)
            }
            (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => {
                DataType::Decimal128(*p, *s)
            }
            _ => DataType::Float64,
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let t_min = &arg_types[0];
        let t_max = &arg_types[1];
        Ok(Self::calculate_output_type(t_min, t_max))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Get the data types from the argument fields
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();

        let return_type = self.return_type(&arg_types)?;

        // Create field with nullable = false (uniform never returns NULL)
        Ok(Arc::new(Field::new(self.name(), return_type, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        // Extract min and max (must be scalars)
        let min = match &args[0] {
            ColumnarValue::Scalar(s) => s.clone(),
            ColumnarValue::Array(_) => {
                return Err(generic_exec_err(
                    "uniform",
                    "min must be a scalar value, not an array",
                ))
            }
        };

        let max = match &args[1] {
            ColumnarValue::Scalar(s) => s.clone(),
            ColumnarValue::Array(_) => {
                return Err(generic_exec_err(
                    "uniform",
                    "max must be a scalar value, not an array",
                ))
            }
        };

        // Extract seed if present
        let seed: Option<u64> = if args.len() == 3 {
            match &args[2] {
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::Int64(Some(value)) => Some(*value as u64),
                    ScalarValue::UInt64(Some(value)) => Some(*value),
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        None
                    }
                    _ => {
                        return Err(generic_exec_err(
                            "uniform",
                            &format!("seed must be an integer, got {}", scalar.data_type()),
                        ))
                    }
                },
                ColumnarValue::Array(_) => {
                    return Err(generic_exec_err(
                        "uniform",
                        "seed must be a scalar value, not an array",
                    ))
                }
            }
        } else {
            None
        };

        // Generate values based on type
        match (&min, &max) {
            (ScalarValue::Int32(Some(min_val)), ScalarValue::Int32(Some(max_val))) => {
                let values = generate_uniform_int(*min_val, *max_val, seed, number_rows)?;
                let array = Int32Array::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ScalarValue::Float64(Some(min_val)), ScalarValue::Float64(Some(max_val))) => {
                let values = generate_uniform_float(*min_val, *max_val, seed, number_rows)?;
                let array = Float64Array::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (
                ScalarValue::Decimal128(Some(min_val), p1, s1),
                ScalarValue::Decimal128(Some(max_val), p2, s2),
            ) => {
                // Convert Decimal128 to f64 for generation
                let min_f64 = *min_val as f64 / 10_f64.powi(*s1 as i32);
                let max_f64 = *max_val as f64 / 10_f64.powi(*s2 as i32);

                // Generate float values
                let float_values = generate_uniform_float(min_f64, max_f64, seed, number_rows)?;

                // Convert back to Decimal128
                let precision = (*p1).max(*p2);
                let scale = (*s1).max(*s2);
                let decimal_values: Vec<i128> = float_values
                    .iter()
                    .map(|&v| (v * 10_f64.powi(scale as i32)).round() as i128)
                    .collect();

                let array = Decimal128Array::from(decimal_values)
                    .with_precision_and_scale(precision, scale)?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => Err(generic_exec_err(
                "uniform",
                &format!(
                    "unsupported types for min and max: {} and {}",
                    min.data_type(),
                    max.data_type()
                ),
            )),
        }
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
            let seed_t = &arg_types[2];

            if seed_t.is_signed_integer() {
                coerced_types.push(DataType::Int64);
            } else if seed_t.is_unsigned_integer() {
                coerced_types.push(DataType::UInt64);
            } else if seed_t.is_null() {
                coerced_types.push(DataType::Null);
            } else {
                return Err(unsupported_data_types_exec_err(
                    "uniform",
                    "Integer Type for seed",
                    &arg_types[2..],
                ));
            }
        }

        Ok(coerced_types)
    }
}

macro_rules! generate_uniform_fn {
    ($fn_name:ident, $type:ty) => {
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

generate_uniform_fn!(generate_uniform_int, i32);
generate_uniform_fn!(generate_uniform_float, f64);

#[cfg(test)]
mod tests {
    use super::*;

    /// Test 1: uniform(10, 20, 0) should return 18 (with i32)
    #[test]
    fn test_uniform_single_value() -> Result<()> {
        let values = generate_uniform_int(10, 20, Some(0), 1)?;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], 18);
        Ok(())
    }

    /// Test 2: uniform(10, 20, 0) FROM range(5) - should return DIFFERENT values
    #[test]
    fn test_uniform_multiple_values_are_different() -> Result<()> {
        let values = generate_uniform_int(10, 20, Some(0), 5)?;

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
        let values = generate_uniform_int(5, 5, Some(0), 3)?;
        assert_eq!(values, vec![5, 5, 5]);
        Ok(())
    }

    /// Test 4: Seed 42 should always give the same result (reproducibility)
    #[test]
    fn test_uniform_reproducibility() -> Result<()> {
        let values1 = generate_uniform_int(0, 100, Some(42), 5)?;
        let values2 = generate_uniform_int(0, 100, Some(42), 5)?;
        assert_eq!(values1, values2, "Same seed should produce same values");
        Ok(())
    }

    /// Test 5: Without seed should give different values on each call
    #[test]
    fn test_uniform_without_seed_is_random() -> Result<()> {
        let values1 = generate_uniform_int(0, 100, None, 5)?;
        let values2 = generate_uniform_int(0, 100, None, 5)?;

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
        let values1 = generate_uniform_int(20, 10, Some(0), 1)?;
        let values2 = generate_uniform_int(10, 20, Some(0), 1)?;
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
        let values = generate_uniform_int(10, 20, Some(0), 10)?;
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
        let values = generate_uniform_int(-10, 10, Some(0), 5)?;
        assert_eq!(values.len(), 5);

        for &v in &values {
            assert!((-10..10).contains(&v), "Value {} out of range [-10, 10)", v);
        }
        Ok(())
    }

    /// Test 11: Verify specific value with seed 42
    #[test]
    fn test_uniform_seed_42_value() -> Result<()> {
        let values = generate_uniform_int(0, 100, Some(42), 1)?;
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
        let arg_types = vec![DataType::Int64, DataType::Int64, DataType::Int64];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Int32,
            "Integer inputs should return Int32 (maps to Spark's integer type)"
        );
        Ok(())
    }

    /// Test 14: Verify return type for Int32 inputs (should still be Int32)
    #[test]
    fn test_uniform_return_type_int32() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Int32, DataType::Int32, DataType::Int64];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Int32,
            "Int32 inputs should be coerced to Int32"
        );
        Ok(())
    }

    /// Test 15: Verify return type for floats (should be Float64)
    #[test]
    fn test_uniform_return_type_float() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Float64, DataType::Float64, DataType::Int64];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Float64,
            "Float inputs should return Float64"
        );
        Ok(())
    }

    /// Test 16: Verify return type for mixed int/float (should be Float64)
    #[test]
    fn test_uniform_return_type_mixed() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Int64, DataType::Float64, DataType::Int64];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Float64,
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

    /// Test 18: Verify coerce_types for float inputs
    #[test]
    fn test_uniform_coerce_types_float() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        let arg_types = vec![DataType::Float32, DataType::Float32, DataType::Int64];
        let coerced = uniform_fn.coerce_types(&arg_types)?;
        assert_eq!(
            coerced,
            vec![DataType::Float64, DataType::Float64, DataType::Int64],
            "Float32 should be coerced to Float64"
        );
        Ok(())
    }

    /// Test 19: Verify return type for decimal inputs (Spark's behavior with literals like 5.5, 10.5)
    #[test]
    fn test_uniform_return_type_decimal() -> Result<()> {
        let uniform_fn = SparkUniform::new();
        // Decimal(3, 1) represents numbers like 5.5, 10.5 (3 total digits, 1 after decimal)
        let arg_types = vec![
            DataType::Decimal128(3, 1),
            DataType::Decimal128(3, 1),
            DataType::Int64,
        ];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Decimal128(3, 1),
            "Decimal(3,1) inputs should return Decimal(3,1) matching Spark's behavior"
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
        let arg_types = vec![
            DataType::Decimal128(2, 1),
            DataType::Decimal128(3, 1),
            DataType::Int64,
        ];
        let return_type = uniform_fn.return_type(&arg_types)?;
        assert_eq!(
            return_type,
            DataType::Decimal128(3, 1),
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
            &DataType::Int32,
            "Should return Int32 for integer inputs"
        );
        assert_eq!(field.name(), "uniform", "Field name should be 'uniform'");

        Ok(())
    }
}
