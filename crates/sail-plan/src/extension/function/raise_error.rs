use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::ScalarValue;

#[derive(Debug, Clone)]
pub struct RaiseError {
    signature: Signature,
}

impl Default for RaiseError {
    fn default() -> Self {
        Self::new()
    }
}

impl RaiseError {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RaiseError {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "raise_error"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "raise_error should only be called with one argument, got {}",
                args.len()
            )));
        }
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(msg)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(msg)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(msg))) => {
                Err(DataFusionError::Execution(msg.to_string()))
            }
            _ => Err(DataFusionError::Internal(
                "raise_error expects a single UTF-8 string argument".to_string(),
            )),
        }
    }
}
