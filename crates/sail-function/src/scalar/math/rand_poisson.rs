use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rngs::StdRng;
use rand::{SeedableRng, rng};
use rand_distr::{Distribution, Poisson};

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
    fn name(&self) -> &str {
        "random_poisson"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::math_rand_poisson::return_type(_arg_types)
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
                    );
                }
            },
            _ => {
                return exec_err!(
                    "`random_poisson` expects a scalar Float64 for `lambda`, got {lambda}"
                );
            }
        };
        let seed = match args.get(1) {
            None => None,
            Some(ColumnarValue::Scalar(scalar)) => match scalar {
                ScalarValue::Int64(Some(value)) => Some(*value as u64),
                ScalarValue::UInt64(Some(value)) => Some(*value),
                ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => None,
                _ => {
                    return exec_err!("`random_poisson` expects an integer seed, got {scalar}");
                }
            },
            Some(ColumnarValue::Array(seed)) => {
                return exec_err!(
                    "`random_poisson` expects a scalar seed argument, got {}",
                    seed.data_type()
                );
            }
        };

        // `Poisson::new` rejects zero, but Spark permits a zero sampling fraction and emits no
        // copies of each input row, so return one zero count per row without a distribution.
        if lambda == 0.0 {
            let values = Int64Array::from_value(0, number_rows);
            return Ok(ColumnarValue::Array(Arc::new(values)));
        }

        let Some(seed) = seed else {
            return invoke_no_seed(lambda, number_rows);
        };
        let poisson = Poisson::new(lambda)
            .map_err(|e| exec_datafusion_err!("Failed to create Poisson distribution: {e}"))?;
        let mut rng = StdRng::seed_from_u64(seed);

        let values = (0..number_rows).map(|_| poisson.sample(&mut rng) as i64);
        let array = Int64Array::from_iter_values(values);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::math_rand_poisson::coerce_types(arg_types)
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
