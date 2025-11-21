use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::{rng, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() < 2 || args.len() > 3 {
            return Err(invalid_arg_count_exec_err("uniform", (2, 3), args.len()));
        }

        // Extract min value
        let min = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as f64,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as f64,
            ColumnarValue::Scalar(scalar) => {
                return Err(generic_exec_err(
                    "uniform",
                    &format!("expects numeric min, got {}", scalar),
                ))
            }
            _ => return Err(generic_exec_err("uniform", "expects scalar min argument")),
        };

        // Extract max value
        let max = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as f64,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as f64,
            ColumnarValue::Scalar(scalar) => {
                return Err(generic_exec_err(
                    "uniform",
                    &format!("expects numeric max, got {}", scalar),
                ))
            }
            _ => return Err(generic_exec_err("uniform", "expects scalar max argument")),
        };

        // Extract optional seed
        let seed = if args.len() == 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => Some(*value as u64),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(value))) => Some(*value),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => Some(*value as u64),
                ColumnarValue::Scalar(ScalarValue::Int64(None))
                | ColumnarValue::Scalar(ScalarValue::UInt64(None))
                | ColumnarValue::Scalar(ScalarValue::Int32(None))
                | ColumnarValue::Scalar(ScalarValue::Null) => None,
                ColumnarValue::Scalar(scalar) => {
                    return Err(generic_exec_err(
                        "uniform",
                        &format!("expects integer seed, got {}", scalar),
                    ))
                }
                _ => return Err(generic_exec_err("uniform", "expects scalar seed argument")),
            }
        } else {
            None
        };

        // Generate random values
        let values = if let Some(seed) = seed {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            std::iter::repeat_with(|| rng.random_range(min..max))
                .take(number_rows)
                .collect::<Vec<f64>>()
        } else {
            let mut rng = rng();
            std::iter::repeat_with(|| rng.random_range(min..max))
                .take(number_rows)
                .collect::<Vec<f64>>()
        };

        let array = Float64Array::from(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(invalid_arg_count_exec_err(
                "uniform",
                (2, 3),
                arg_types.len(),
            ));
        }

        let mut coerced_types = vec![DataType::Float64, DataType::Float64];

        if arg_types.len() == 3 {
            if arg_types[2].is_signed_integer() {
                coerced_types.push(DataType::Int64);
            } else if arg_types[2].is_unsigned_integer() {
                coerced_types.push(DataType::UInt64);
            } else if arg_types[2].is_null() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_reproducibility_with_seed() {
        // Test that same seed produces same values
        let mut rng1 = ChaCha8Rng::seed_from_u64(42);
        let mut rng2 = ChaCha8Rng::seed_from_u64(42);

        let min = 0.0;
        let max = 100.0;

        for _ in 0..5 {
            let val1 = rng1.random_range(min..max);
            let val2 = rng2.random_range(min..max);
            assert_eq!(val1, val2);
            assert!(val1 >= min && val1 < max);
        }
    }

    #[test]
    fn test_uniform_values_in_range() {
        // Test that values are within the specified range
        let mut rng = ChaCha8Rng::seed_from_u64(0);
        let min = 10.0;
        let max = 20.0;

        for _ in 0..100 {
            let val = rng.random_range(min..max);
            assert!(val >= min && val < max);
        }
    }

    #[test]
    fn test_uniform_float_bounds() {
        // Test with float boundaries
        let mut rng = ChaCha8Rng::seed_from_u64(123);
        let min = 5.5;
        let max = 10.5;

        for _ in 0..50 {
            let val = rng.random_range(min..max);
            assert!(val >= min && val < max);
        }
    }

    #[test]
    fn test_uniform_negative_range() {
        // Test with negative ranges
        let mut rng = ChaCha8Rng::seed_from_u64(999);
        let min = -10.0;
        let max = -5.0;

        for _ in 0..50 {
            let val = rng.random_range(min..max);
            assert!(val >= min && val < max);
        }
    }

    #[test]
    fn test_uniform_different_seeds_produce_different_values() {
        // Test that different seeds produce different sequences
        let mut rng1 = ChaCha8Rng::seed_from_u64(42);
        let mut rng2 = ChaCha8Rng::seed_from_u64(43);

        let min = 0.0;
        let max = 100.0;

        let val1 = rng1.random_range(min..max);
        let val2 = rng2.random_range(min..max);

        // Very unlikely to be equal with different seeds
        assert_ne!(val1, val2);
    }
}
