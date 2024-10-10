use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_comet_spark_expr::scalar_funcs::spark_murmur3_hash;
use datafusion_common::ScalarValue;

#[derive(Debug)]
pub(crate) struct SparkMurmur3Hash {
    signature: Signature,
}

impl SparkMurmur3Hash {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMurmur3Hash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_murmur3_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let length = args.len();
        if length < 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "spark_hash requires at least one argument".to_string(),
            ));
        }
        let seed = &args[length - 1];
        let mut args = args.to_vec();
        match seed {
            ColumnarValue::Scalar(ScalarValue::Int32(_)) => {}
            _ => {
                args.push(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))));
            }
        }
        spark_murmur3_hash(&args[..])
    }
}
