use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rand::rngs::StdRng;
use rand::{rng, SeedableRng};
use rand_distr::{Distribution, Poisson};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug)]
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
            signature: Signature {
                type_signature: TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                volatility: Volatility::Volatile,
            },
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
                "`random_poisson` should be called with at most 2 argument, got {}",
                args.len()
            );
        }
        if args.len() > 2 {
            return exec_err!(
                "`random_poisson` should be called with at most 2 argument, got {}",
                args.len()
            );
        }
        let lambda = &args[0];
        let lambda = match lambda {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Float64(Some(value)) => *value,
                ScalarValue::Float64(None) | ScalarValue::Null => 1.0,
                _ => return exec_err!("`random_poisson` expects a float64 lambda, got {scalar}"),
            },
            _ => return exec_err!("`random_poisson` expects an float64 seed, got {lambda}"),
        };
        if args.len() == 1 {
            return invoke_no_seed(lambda, number_rows);
        }
        let seed = &args[1];

        match seed {
            ColumnarValue::Scalar(scalar) => {
                let seed = match scalar {
                    ScalarValue::Int64(Some(value)) => *value,
                    ScalarValue::UInt64(Some(value)) => *value as i64,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(lambda, number_rows)
                    }
                    _ => {
                        return exec_err!("`random_poisson` expects an integer seed, got {scalar}")
                    }
                };
                let poisson = Poisson::new(lambda).unwrap();
                let mut rng = StdRng::seed_from_u64(seed as u64);

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
        if arg_types.is_empty() {
            Ok(vec![])
        } else if arg_types.len() == 1 {
            if arg_types[0].is_signed_integer() {
                Ok(vec![DataType::Int64])
            } else if arg_types[0].is_unsigned_integer() {
                Ok(vec![DataType::UInt64])
            } else if arg_types[0].is_null() {
                Ok(vec![DataType::Null])
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
                (0, 1000),
                arg_types.len(),
            ))
        }
    }
}

fn invoke_no_seed(lambda: f64, number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let poisson = Poisson::new(lambda).unwrap();
    let values = std::iter::repeat_with(|| poisson.sample(&mut rng) as i64).take(number_rows);
    let array = Int64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
