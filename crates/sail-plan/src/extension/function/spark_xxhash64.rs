use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_comet_spark_expr::scalar_funcs::spark_xxhash64;
use datafusion_common::ScalarValue;

#[derive(Debug)]
pub(crate) struct SparkXxhash64 {
    signature: Signature,
}

impl SparkXxhash64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkXxhash64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let length = args.len();
        if length < 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "spark_xxhash64 requires at least one argument".to_string(),
            ));
        }
        let seed = &args[length - 1];
        let mut args = args.to_vec();
        match seed {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
                let new_scalar = ScalarValue::Int64(Some(*seed as i64));
                args[length - 1] = ColumnarValue::Scalar(new_scalar);
            }
            ColumnarValue::Scalar(ScalarValue::Int64(_)) => {}
            _ => {
                args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(42))));
            }
        }
        spark_xxhash64(&args[..])
    }
}
