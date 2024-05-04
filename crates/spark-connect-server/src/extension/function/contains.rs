use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::ScalarValue;

#[derive(Debug, Clone)]
pub(crate) struct Contains {
    signature: Signature,
}

impl Contains {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Contains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "contains should only be called with two arguments, got {:?}",
                args.len()
            )));
        }

        let (left, right) = (&args[0], &args[1]);

        match (left, right) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(left)),
                ColumnarValue::Scalar(ScalarValue::Utf8(right)),
            ) => {
                let res = match (left, right) {
                    (Some(left), Some(right)) => Some(left.contains(right)),
                    _ => None, // one or both arguments were NULL
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(res)))
            }
            _ => Err(DataFusionError::Internal(format!(
                "contains should only be called with two string arguments, got {:?}",
                args
            ))),
        }
    }
}
