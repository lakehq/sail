use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_common::{plan_err, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::extension::function::url::url_decode::UrlDecode;

#[derive(Debug)]
pub struct TryUrlDecode {
    signature: Signature,
}

impl Default for TryUrlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl TryUrlDecode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryUrlDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_url_decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => plan_err!("1st argument should be STRING, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result = UrlDecode::new().invoke_with_args(args);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}
