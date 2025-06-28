use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use urlencoding::encode;

#[derive(Debug)]
pub struct UrlEncode {
    signature: Signature,
}

impl Default for UrlEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlEncode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
    fn encode(value: &str) -> Result<String> {
        Ok(encode(value).into_owned())
    }
}

impl ScalarUDFImpl for UrlEncode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "url_encode"
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
        let [first_arg] = args.as_slice() else {
            return exec_err!("{} expects 1 argument, but got {}", self.name(), args.len());
        };
        spark_url_encode(first_arg)
    }
}

fn spark_url_encode(values: &ColumnarValue) -> Result<ColumnarValue> {
    match values {
        ColumnarValue::Scalar(scalar_str) => match scalar_str {
            ScalarValue::Utf8(Some(expr))
            | ScalarValue::LargeUtf8(Some(expr))
            | ScalarValue::Utf8View(Some(expr)) => {
                let result = Some(UrlEncode::encode(expr)?);
                match scalar_str {
                    ScalarValue::LargeUtf8(Some(_)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result))),
                }
            }
            other => exec_err!("`url_encode`: Expr must be STRING, got {other:?}"),
        },

        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => as_string_array(&array)?
                .iter()
                .map(|x| x.map(UrlEncode::encode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            DataType::LargeUtf8 => as_large_string_array(&array)?
                .iter()
                .map(|x| x.map(UrlEncode::encode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            DataType::Utf8View => as_string_view_array(&array)?
                .iter()
                .map(|x| x.map(UrlEncode::encode).transpose())
                .collect::<Result<StringArray>>()
                .map(|array| ColumnarValue::Array(Arc::new(array))),
            other => exec_err!("`url_encode`: Expr must be STRING, got {other:?}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use datafusion_common::Result;

    use super::*;

    #[test]
    fn test_encode() -> Result<()> {
        let string1_array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "123", "abc", "xyz", "kitten",
        ])));
        let _string2_array = Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));

        spark_url_encode(&string1_array)?;

        // assert_eq!(&expected, result);

        Ok(())
    }
}
