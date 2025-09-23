use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::{rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, StandardNormal};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Randn {
    signature: Signature,
}

impl Default for Randn {
    fn default() -> Self {
        Self::new()
    }
}

impl Randn {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for Randn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "randn"
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
        if args.is_empty() {
            return invoke_no_seed(number_rows);
        }

        let [seed] = args.as_slice() else {
            return exec_err!(
                "randn should be called with at most 1 argument, got {}",
                args.len()
            );
        };
        match seed {
            ColumnarValue::Scalar(scalar) => {
                let seed = match scalar {
                    ScalarValue::Int64(Some(value)) => *value as u64,
                    ScalarValue::UInt64(Some(value)) => *value,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(number_rows)
                    }
                    _ => return exec_err!("`randn` expects an integer seed, got {scalar}"),
                };
                let mut rng = ChaCha8Rng::seed_from_u64(seed);
                let values =
                    std::iter::repeat_with(|| StandardNormal.sample(&mut rng)).take(number_rows);
                let array = Float64Array::from_iter_values(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => exec_err!(
                "`randn` expects a scalar seed argument, got {}",
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
                    "randn",
                    "Integer Type for seed",
                    arg_types,
                ))
            }
        } else {
            Err(invalid_arg_count_exec_err("randn", (0, 1), arg_types.len()))
        }
    }
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let values = std::iter::repeat_with(|| StandardNormal.sample(&mut rng)).take(number_rows);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
