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
            args,
            number_rows,
            ..
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
                let mut min_v = *min_val;
                let mut max_v = *max_val;

                if min_v > max_v {
                    std::mem::swap(&mut min_v, &mut max_v);
                }

                if min_v == max_v {
                    let array = Int64Array::from(vec![min_v; number_rows]);
                    return Ok(ColumnarValue::Array(Arc::new(array)));
                }

                let values: Vec<i64> = if let Some(seed_val) = seed {
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

                let array = Int64Array::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ScalarValue::Float64(Some(min_val)), ScalarValue::Float64(Some(max_val))) => {
                let mut min_v = *min_val;
                let mut max_v = *max_val;

                if min_v > max_v {
                    std::mem::swap(&mut min_v, &mut max_v);
                }

                if min_v == max_v {
                    let array = Float64Array::from(vec![min_v; number_rows]);
                    return Ok(ColumnarValue::Array(Arc::new(array)));
                }

                let values: Vec<f64> = if let Some(seed_val) = seed {
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

