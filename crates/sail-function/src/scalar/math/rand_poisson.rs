use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rngs::StdRng;
use rand::{rng, SeedableRng};
use rand_distr::{Distribution, Poisson};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RandPoisson {
    signature: Signature,
}

impl Default for RandPoisson {
    fn default() -> Self {
        Self::new()
    }
}

impl RandPoisson {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandPoisson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "random_poisson"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() {
            return exec_err!(
                "`random_poisson` must be called with either 1 or 2 arguments, got {}",
                args.len()
            );
        }
        if args.len() > 2 {
            return exec_err!(
                "`random_poisson` must be called with either 1 or 2 arguments, got {}",
                args.len()
            );
        }
        let lambda = &args[0];
        let lambda = match lambda {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Float64(Some(value)) => *value,
                ScalarValue::Float64(None) | ScalarValue::Null => 1.0,
                _ => {
                    return exec_err!(
                        "`random_poisson` expects a scalar Float64 for `lambda`, got {scalar}"
                    )
                }
            },
            _ => {
                return exec_err!(
                    "`random_poisson` expects a scalar Float64 for `lambda`, got {lambda}"
                )
            }
        };
        if args.len() == 1 {
            return invoke_no_seed(lambda, number_rows);
        }
        let seed = &args[1];

        match seed {
            ColumnarValue::Scalar(scalar) => {
                let seed: u64 = match scalar {
                    ScalarValue::Int64(Some(value)) => *value as u64,
                    ScalarValue::UInt64(Some(value)) => *value,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(lambda, number_rows)
                    }
                    _ => {
                        return exec_err!("`random_poisson` expects an integer seed, got {scalar}")
                    }
                };
                let poisson = Poisson::new(lambda).map_err(|e| {
                    exec_datafusion_err!("Failed to create Poisson distribution: {e}")
                })?;
                let mut rng = StdRng::seed_from_u64(seed);

                let values = (0..number_rows).map(|_| poisson.sample(&mut rng) as i64);
                let array = Int64Array::from_iter_values(values);

                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => exec_err!(
                "`random_poisson` expects a scalar seed argument, got {}",
                seed.data_type()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 1 {
            let lambda_type = if arg_types[0].is_floating() {
                Ok(DataType::Float64)
            } else {
                Err(unsupported_data_types_exec_err(
                    "random_poisson",
                    "Floating Type for lambda",
                    arg_types,
                ))
            }?;
            Ok(vec![lambda_type])
        } else if arg_types.len() == 2 {
            if arg_types[0].is_floating() && arg_types[1].is_numeric() {
                let seed_type = if arg_types[1].is_signed_integer() {
                    Ok(DataType::Int64)
                } else if arg_types[1].is_unsigned_integer() {
                    Ok(DataType::UInt64)
                } else if arg_types[1].is_null() {
                    Ok(DataType::Null)
                } else {
                    Err(unsupported_data_types_exec_err(
                        "random_poisson",
                        "Integer Type for seed",
                        arg_types,
                    ))
                }?;
                let lambda_type = if arg_types[0].is_floating() {
                    Ok(DataType::Float64)
                } else {
                    Err(unsupported_data_types_exec_err(
                        "random_poisson",
                        "Floating Type for lambda",
                        arg_types,
                    ))
                }?;
                Ok(vec![lambda_type, seed_type])
            } else {
                Err(unsupported_data_types_exec_err(
                    "random_poisson",
                    "Integer Type for seed",
                    arg_types,
                ))
            }
        } else {
            Err(invalid_arg_count_exec_err(
                "random_poisson",
                (1, 2),
                arg_types.len(),
            ))
        }
    }
}

fn invoke_no_seed(lambda: f64, number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let poisson = Poisson::new(lambda)
        .map_err(|e| exec_datafusion_err!("Failed to create Poisson distribution: {e}"))?;
    let values = std::iter::repeat_with(|| poisson.sample(&mut rng) as i64).take(number_rows);
    let array = Int64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
