use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use urlencoding::decode;

#[derive(Debug)]
pub struct UrlDecode {
    signature: Signature,
}

impl Default for UrlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlDecode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
    fn decode(value: &str) -> Result<String> {
        decode(value)
            .map_err(|e| exec_datafusion_err!("{e}"))
            .map(|parsed| parsed.into_owned())
    }
}

impl ScalarUDFImpl for UrlDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "url_decode"
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
            _ => plan_err!("1st argument should be String, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!("{} expects 1 argument, but got {}", self.name(), args.len());
        };
        spark_url_decode(arg)
    }
}

fn spark_url_decode(values: &ColumnarValue) -> Result<ColumnarValue> {
    match values {
        ColumnarValue::Scalar(scalar_str) => match scalar_str {
            ScalarValue::Utf8(Some(expr))
            | ScalarValue::LargeUtf8(Some(expr))
            | ScalarValue::Utf8View(Some(expr)) => {
                let result = Some(UrlDecode::decode(expr)?);
                match scalar_str {
                    ScalarValue::LargeUtf8(Some(_)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result))),
                }
            }
            other => exec_err!("`url_decode`: Expr must be STRING, got {other:?}"),
        },

        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => as_string_array(&array)?
                .iter()
                .map(|x| x.map(UrlDecode::decode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            DataType::LargeUtf8 => as_large_string_array(&array)?
                .iter()
                .map(|x| x.map(UrlDecode::decode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            DataType::Utf8View => as_string_view_array(&array)?
                .iter()
                .map(|x| x.map(UrlDecode::decode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            other => exec_err!("`url_decode`: Expr must be STRING, got {other:?}"),
        },
    }
}
