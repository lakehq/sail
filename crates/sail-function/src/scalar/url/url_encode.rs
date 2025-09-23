use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, Result};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use url::form_urlencoded::byte_serialize;

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
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

    /// Encode a string to application/x-www-form-urlencoded format.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to encode
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - The encoded string
    ///
    fn encode(value: &str) -> Result<String> {
        Ok(byte_serialize(value.as_bytes()).collect::<String>())
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
        make_scalar_function(spark_url_encode, vec![])(&args)
    }
}

/// Core implementation of URL encoding function.
///
/// # Arguments
///
/// * `args` - A slice containing exactly one ArrayRef with the strings to encode
///
/// # Returns
///
/// * `Ok(ArrayRef)` - A new array of the same type containing encoded strings
/// * `Err(DataFusionError)` - If invalid arguments are provided
///
fn spark_url_encode(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`url_encode` expects 1 argument");
    }

    match &args[0].data_type() {
        DataType::Utf8 => as_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlEncode::encode).transpose())
            .collect::<Result<StringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::LargeUtf8 => as_large_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlEncode::encode).transpose())
            .collect::<Result<LargeStringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::Utf8View => as_string_view_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlEncode::encode).transpose())
            .collect::<Result<StringViewArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        other => exec_err!("`url_encode`: Expr must be STRING, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion_common::Result;

    use super::*;

    #[test]
    fn test_encode() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            None,
        ]));
        let expected = StringArray::from(vec![
            Some("inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%5C%3Bparam%3Fquery%5C%3Bp2"),
            None,
        ]);

        let result = spark_url_encode(&[input as ArrayRef])?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);

        Ok(())
    }
}
