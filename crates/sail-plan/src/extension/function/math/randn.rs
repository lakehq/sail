use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return invoke_no_seed();
        }

        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "randn should be called with at most 1 argument, got {}",
                args.len()
            )));
        }

        match &args[0] {
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
                    ScalarValue::Null => return invoke_no_seed(),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "randn expects an integer seed, got {}",
                            scalar
                        )))
                    }
                };
                let value = ChaCha8Rng::seed_from_u64(seed).sample(StandardNormal);
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
            }
            _ => Err(DataFusionError::Execution(format!(
                "randn expects a scalar seed argument, got {}",
                args[0].data_type()
            ))),
        }
    }

    fn invoke_no_args(&self, num_rows: usize) -> Result<ColumnarValue> {
        let mut rng = rng();
        let values = std::iter::repeat_with(|| StandardNormal.sample(&mut rng)).take(num_rows);
        let array = Float64Array::from_iter_values(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

fn invoke_no_seed() -> Result<ColumnarValue> {
    let value = rng().sample(StandardNormal);
    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
}
