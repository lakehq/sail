use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::ScalarValue;

#[derive(Debug, Clone)]
pub(crate) struct RaiseError {
    signature: Signature,
}

impl RaiseError {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
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

        let err_msg = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(msg))) => msg.clone(),
            _ => {
                return Err(DataFusionError::Internal(
                    "raise_error expects a single UTF-8 string argument".to_string(),
                ))
            }
        };

        Err(DataFusionError::Execution(err_msg))
    }
}
