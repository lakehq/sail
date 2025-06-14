use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::rng;
use rand_distr::Distribution;

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
            //signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Volatile),
            signature: Signature::any(1, Volatility::Volatile),
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
                "random should be called with at most 1 argument, got {}",
                args.len()
            );
        }

        let [seed] = args.as_slice() else {
            return exec_err!(
                "random should be called with at most 1 argument, got {}",
                args.len()
            );
        };

        match seed {
            ColumnarValue::Scalar(_scalar) => {
                return invoke_no_seed(number_rows);
            }
            _ => exec_err!(
                "`random` expects a scalar seed argument, got {}",
                seed.data_type()
            ),
        }
    }
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    use rand_distr::Poisson;

    let mut rng = rng();
    let poisson = Poisson::new(1.0).unwrap();

    let values = std::iter::repeat_with(|| poisson.sample(&mut rng) as i64).take(number_rows);

    let array = Int64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
