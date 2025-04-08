use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rand::{rng, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, StandardNormal};

#[derive(Debug)]
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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![]),
                    TypeSignature::Uniform(
                        1,
                        vec![
                            DataType::Int8,
                            DataType::Int16,
                            DataType::Int32,
                            DataType::Int64,
                            DataType::UInt8,
                            DataType::UInt16,
                            DataType::UInt32,
                            DataType::UInt64,
                            DataType::Null,
                        ],
                    ),
                ],
                Volatility::Volatile,
            ),
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
                    ScalarValue::Int8(Some(value)) => *value as u64,
                    ScalarValue::Int16(Some(value)) => *value as u64,
                    ScalarValue::Int32(Some(value)) => *value as u64,
                    ScalarValue::Int64(Some(value)) => *value as u64,
                    ScalarValue::UInt8(Some(value)) => *value as u64,
                    ScalarValue::UInt16(Some(value)) => *value as u64,
                    ScalarValue::UInt32(Some(value)) => *value as u64,
                    ScalarValue::UInt64(Some(value)) => *value,
                    ScalarValue::Int8(None)
                    | ScalarValue::Int16(None)
                    | ScalarValue::Int32(None)
                    | ScalarValue::Int64(None)
                    | ScalarValue::UInt8(None)
                    | ScalarValue::UInt16(None)
                    | ScalarValue::UInt32(None)
                    | ScalarValue::UInt64(None)
                    | ScalarValue::Null => return invoke_no_seed(number_rows),
                    _ => return exec_err!("`randn` expects an integer seed, got {}", scalar),
                };
                let value = ChaCha8Rng::seed_from_u64(seed).sample(StandardNormal);
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
            }
            _ => exec_err!(
                "`randn` expects a scalar seed argument, got {}",
                seed.data_type()
            ),
        }
    }
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let values = std::iter::repeat_with(|| StandardNormal.sample(&mut rng)).take(number_rows);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
