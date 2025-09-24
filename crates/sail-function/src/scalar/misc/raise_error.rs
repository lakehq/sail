use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{internal_err, ScalarValue};
use datafusion_expr::ScalarFunctionArgs;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let Ok(arg) = args.one() else {
            return internal_err!("raise_error should only be called with one argument");
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(message))) => {
                Err(DataFusionError::Execution(message))
            }
            _ => internal_err!("raise_error expects a single UTF-8 string argument"),
        }
    }
}
