use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rng;
use rand_distr::{Distribution, StandardNormal};

use super::xorshift::SparkXorShiftRandom;

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
    fn name(&self) -> &str {
        "randn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::math_randn::return_type(_arg_types)
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
                    ScalarValue::Int64(Some(value)) => *value,
                    ScalarValue::UInt64(Some(value)) => *value as i64,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(number_rows);
                    }
                    _ => return exec_err!("`randn` expects an integer seed, got {scalar}"),
                };
                let mut rng = SparkXorShiftRandom::new(seed);
                let values = std::iter::repeat_with(|| rng.next_gaussian()).take(number_rows);
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
        sail_type_system::policies::math_randn::coerce_types(arg_types)
    }
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let values = std::iter::repeat_with(|| StandardNormal.sample(&mut rng)).take(number_rows);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
