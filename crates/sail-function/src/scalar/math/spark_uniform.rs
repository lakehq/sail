use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rngs::StdRng;
use rand::{rng, Rng, SeedableRng};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_types_exec_err};

/// The `Uniform` function generates random numbers from a uniform distribution
/// between a specified minimum and maximum value.
///
/// Syntax: `uniform(min, max, seed)`
/// - min: minimum value of the range (inclusive)
/// - max: maximum value of the range (exclusive)
/// - seed: optional random seed for reproducibility
///
/// This function is similar to Spark's `uniform` function.
/// <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.uniform.html>
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

        if t_min.is_integer() && t_max.is_integer() {
            Ok(DataType::Int64)
        } else {
            Ok(DataType::Float64)
        }
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
            (ScalarValue::Int64(Some(min_val)), ScalarValue::Int64(Some(max_val))) => {
                let values = generate_uniform_int(*min_val, *max_val, seed, number_rows)?;
                let array = Int64Array::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ScalarValue::Float64(Some(min_val)), ScalarValue::Float64(Some(max_val))) => {
                let values = generate_uniform_float(*min_val, *max_val, seed, number_rows)?;
                let array = Float64Array::from(values);
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

        let output_type = match (t_min, t_max) {
            (t1, t2) if t1.is_integer() && t2.is_integer() => DataType::Int64,
            _ => DataType::Float64,
        };

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

generate_uniform_fn!(generate_uniform_int, i64);
generate_uniform_fn!(generate_uniform_float, f64);

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Test 1: uniform(10, 20, 0) should return 17
    #[test]
    fn test_uniform_single_value() {
        let values = generate_uniform_int(10, 20, Some(0), 1).expect("generation failed");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], 17);
    }

    /// Test 2: uniform(10, 20, 0) FROM range(5) - should return DIFFERENT values
    #[test]
    fn test_uniform_multiple_values_are_different() {
        let values = generate_uniform_int(10, 20, Some(0), 5).expect("generation failed");

        // Verify that we have 5 values
        assert_eq!(values.len(), 5);

        // Verify that the first value is 17
        assert_eq!(values[0], 17);

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
    }

    /// Test 3: min == max should return that constant value
    #[test]
    fn test_uniform_min_equals_max() {
        let values = generate_uniform_int(5, 5, Some(0), 3).expect("generation failed");
        assert_eq!(values, vec![5, 5, 5]);
    }

    /// Test 4: Seed 42 should always give the same result (reproducibility)
    #[test]
    fn test_uniform_reproducibility() {
        let values1 = generate_uniform_int(0, 100, Some(42), 5).expect("generation failed");
        let values2 = generate_uniform_int(0, 100, Some(42), 5).expect("generation failed");
        assert_eq!(values1, values2, "Same seed should produce same values");
    }

    /// Test 5: Without seed should give different values on each call
    #[test]
    fn test_uniform_without_seed_is_random() {
        let values1 = generate_uniform_int(0, 100, None, 5).expect("generation failed");
        let values2 = generate_uniform_int(0, 100, None, 5).expect("generation failed");

        // It's extremely unlikely they're all the same if it's random
        let probably_different = values1 != values2;
        assert!(
            probably_different,
            "Without seed, values should likely be different: {:?} vs {:?}",
            values1, values2
        );
    }

    /// Test 6: Swapping min and max
    #[test]
    fn test_uniform_swapped_min_max() {
        let values1 = generate_uniform_int(20, 10, Some(0), 1).expect("generation failed");
        let values2 = generate_uniform_int(10, 20, Some(0), 1).expect("generation failed");
        assert_eq!(values1, values2, "Swapped min/max should give same result");
    }

    /// Test 7: Float values
    #[test]
    fn test_uniform_float_values() {
        let values = generate_uniform_float(5.5, 10.5, Some(123), 1).expect("generation failed");
        assert_eq!(values.len(), 1);
        let val = values[0];
        assert!((5.5..10.5).contains(&val), "Value {} out of range", val);
    }

    /// Test 8: Multiple different float values
    #[test]
    fn test_uniform_float_multiple_different() {
        let values = generate_uniform_float(0.0, 1.0, Some(99), 10).expect("generation failed");
        assert_eq!(values.len(), 10);

        // Verify that not all are the same
        let all_same = values.windows(2).all(|w| w[0] == w[1]);
        assert!(!all_same, "Float values should be different: {:?}", values);

        // All in range
        for &v in &values {
            assert!((0.0..1.0).contains(&v), "Value {} out of range", v);
        }
    }

    /// Test 9: Verify that seed 0 with 10 values generates the expected sequence
    #[test]
    fn test_uniform_seed_zero_sequence() {
        let values = generate_uniform_int(10, 20, Some(0), 10).expect("generation failed");
        assert_eq!(values.len(), 10);

        // The first value should be 17
        assert_eq!(values[0], 17);

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
    }

    /// Test 10: Negative numbers
    #[test]
    fn test_uniform_negative_range() {
        let values = generate_uniform_int(-10, 10, Some(0), 5).expect("generation failed");
        assert_eq!(values.len(), 5);

        for &v in &values {
            assert!((-10..10).contains(&v), "Value {} out of range [-10, 10)", v);
        }
    }

    /// Test 11: Verify specific value with seed 42
    #[test]
    fn test_uniform_seed_42_value() {
        let values = generate_uniform_int(0, 100, Some(42), 1).expect("generation failed");
        // With Rust StdRng, seed 42 should give 52
        assert_eq!(values[0], 52);
    }

    /// Test 12: Float min == max
    #[test]
    fn test_uniform_float_equal() {
        let values = generate_uniform_float(2.5, 2.5, Some(0), 5).expect("generation failed");
        assert_eq!(values, vec![2.5, 2.5, 2.5, 2.5, 2.5]);
    }
}
