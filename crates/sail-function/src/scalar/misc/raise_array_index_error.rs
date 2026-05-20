use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarFunctionArgs;
use sail_common_datafusion::error::ArrayIndexOutOfBoundsError;
use sail_common_datafusion::utils::items::ItemTaker;

/// A UDF that raises an [`ArrayIndexOutOfBoundsError`] wrapped in
/// [`DataFusionError::External`].  The error is recognised by the
/// Spark-Connect error-classification layer and surfaced as a
/// `java.lang.ArrayIndexOutOfBoundsException` to the client.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RaiseArrayIndexError {
    signature: Signature,
}

impl Default for RaiseArrayIndexError {
    fn default() -> Self {
        Self::new()
    }
}

impl RaiseArrayIndexError {
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

impl ScalarUDFImpl for RaiseArrayIndexError {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "raise_array_index_error"
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
            return Err(DataFusionError::Internal(
                "raise_array_index_error should only be called with one argument".to_string(),
            ));
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(message))) => Err(
                DataFusionError::External(Box::new(ArrayIndexOutOfBoundsError(message))),
            ),
            _ => Err(DataFusionError::Internal(
                "raise_array_index_error expects a single UTF-8 string argument".to_string(),
            )),
        }
    }
}
