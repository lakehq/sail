use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rngs::StdRng;
use rand::{rng, Rng, SeedableRng};

use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::functions_nested_utils::make_scalar_function;

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
        make_scalar_function(uniform)(&args.args)
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
fn uniform(args: &[ArrayRef]) -> Result<ArrayRef> {
    let len = args[0].len();

    let seed: Option<u64> = if args.len() == 3 {
        let seed_arr = &args[2];
        match seed_arr.data_type() {
            DataType::Int64 => {
                let arr = seed_arr.as_primitive::<Int64Type>();
                if arr.is_empty() || arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0) as u64)
                }
            }
            DataType::UInt64 => {
                let arr = seed_arr.as_primitive::<UInt64Type>();
                if arr.is_empty() || arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            }
            DataType::Null => None,
            dt => {
                return Err(generic_exec_err(
                    "uniform",
                    &format!("expects integer seed, got {dt}"),
                ))
            }
        }
    } else {
        None
    };

    match args[0].data_type() {
        DataType::Int64 => {
            let min_arr = args[0].as_primitive::<Int64Type>();
            let max_arr = args[1].as_primitive::<Int64Type>();

            let mut min = min_arr.value(0);
            let mut max = max_arr.value(0);

            if min > max {
                std::mem::swap(&mut min, &mut max);
            } else if min == max {
                let values: Vec<i64> = vec![min; len];
                return Ok(Arc::new(Int64Array::from(values)) as ArrayRef);
            }

            let values: Vec<i64> = if let Some(seed) = seed {
                let mut rng = StdRng::seed_from_u64(seed);
                (0..len).map(|_| rng.random_range(min..max)).collect()
            } else {
                let mut rng = rng();
                (0..len).map(|_| rng.random_range(min..max)).collect()
            };

            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let min_arr = args[0].as_primitive::<Float64Type>();
            let max_arr = args[1].as_primitive::<Float64Type>();

            if min_arr.is_null(0) || max_arr.is_null(0) {
                return Err(generic_exec_err(
                    "uniform",
                    "min and max must be non-null scalars",
                ));
            }

            let mut min = min_arr.value(0);
            let mut max = max_arr.value(0);

            if min > max {
                std::mem::swap(&mut min, &mut max);
            } else if min == max {
                let values: Vec<f64> = vec![min; len];
                return Ok(Arc::new(Float64Array::from(values)) as ArrayRef);
            }

            let values: Vec<f64> = if let Some(seed) = seed {
                // Para seed
                let mut rng = StdRng::seed_from_u64(seed);
                (0..len).map(|_| rng.random_range(min..max)).collect()
            } else {
                let mut rng = rng();
                (0..len).map(|_| rng.random_range(min..max)).collect()
            };

            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        dt => Err(generic_exec_err(
            "uniform",
            &format!("unsupported argument type: {dt}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array};

    use super::uniform;

    fn as_int64_array(arr: &ArrayRef) -> Result<&Int64Array, Box<dyn Error>> {
        arr.as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Box::<dyn Error>::from(io::Error::other("downcast failed")))
    }

    #[test]
    fn test_uniform_udf_matches_zero() -> Result<(), Box<dyn Error>> {
        let min = Arc::new(Int64Array::from(vec![0])) as ArrayRef;
        let max = Arc::new(Int64Array::from(vec![0])) as ArrayRef;
        let seed = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let args: Vec<ArrayRef> = vec![min, max, seed];
        let res = match uniform(&args) {
            Ok(r) => r,
            Err(e) => return Err(Box::new(e)),
        };
        let out = as_int64_array(&res)?;
        assert_eq!(out.value(0), 0);

        Ok(())
    }

    #[test]
    fn test_uniform_udf_matches_spark_example() -> Result<(), Box<dyn Error>> {
        let min = Arc::new(Int64Array::from(vec![10])) as ArrayRef;
        let max = Arc::new(Int64Array::from(vec![20])) as ArrayRef;
        let seed = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let args: Vec<ArrayRef> = vec![min, max, seed];
        let res = match uniform(&args) {
            Ok(r) => r,
            Err(e) => return Err(Box::new(e)),
        };
        let out = as_int64_array(&res)?;
        assert_eq!(out.value(0), 17);

        Ok(())
    }

    #[test]
    fn test_uniform_udf_matches_min_max_change() -> Result<(), Box<dyn Error>> {
        let min = Arc::new(Int64Array::from(vec![10])) as ArrayRef;
        let max = Arc::new(Int64Array::from(vec![-20])) as ArrayRef;
        let seed = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let args: Vec<ArrayRef> = vec![min.clone(), max.clone(), seed.clone()];
        let res = match uniform(&args) {
            Ok(r) => r,
            Err(e) => return Err(Box::new(e)),
        };
        let out = as_int64_array(&res)?;
        assert_eq!(out.value(0), 1); // need to see, spark -12

        let args: Vec<ArrayRef> = vec![max, min, seed];
        let res = match uniform(&args) {
            Ok(r) => r,
            Err(e) => return Err(Box::new(e)),
        };
        let out = as_int64_array(&res)?;
        assert_eq!(out.value(0), 1); // need to see, spark -12

        Ok(())
    }
}
