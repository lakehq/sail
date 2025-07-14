use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use crate::extension::function::functions_utils::make_scalar_function;
use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use percent_encoding::percent_decode;

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
        let replaced = Self::replace_plus(value.as_bytes());
        percent_decode(&replaced)
            .decode_utf8()
            .map_err(|e| exec_datafusion_err!("Invalid UTF-8 sequence: {e}"))
            .map(|parsed| parsed.into_owned())
    }
    /// Replace b'+' with b' '
    fn replace_plus(input: &[u8]) -> Cow<'_, [u8]> {
        match input.iter().position(|&b| b == b'+') {
            None => Cow::Borrowed(input),
            Some(first_position) => {
                let mut replaced = input.to_owned();
                replaced[first_position] = b' ';
                for byte in &mut replaced[first_position + 1..] {
                    if *byte == b'+' {
                        *byte = b' ';
                    }
                }
                Cow::Owned(replaced)
            }
        }
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
        make_scalar_function(spark_url_decode, vec![])(&args)
    }
}

fn spark_url_decode(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`url_decode` expects 1 argument");
    }

    let result = match &args[0].data_type() {
        DataType::Utf8 => as_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<StringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::LargeUtf8 => as_large_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<LargeStringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::Utf8View => as_string_view_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<StringViewArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        other => exec_err!("`url_decode`: Expr must be STRING, got {other:?}"),
    };
    result
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use datafusion_common::Result;

    use super::*;

    #[test]
    fn test_decode() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("https%3A%2F%2Fspark.apache.org"),
            // Some("http%3A%2F%2spark.apache.org"), // Throws an errors
            Some("inva+lid://user:pass@host/file\\;param?query\\;p2"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            None,
        ]));
        let expected = StringArray::from(vec![
            Some("https://spark.apache.org"),
            // Some("https://spark.apache.org"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            None,
        ]);

        let result = spark_url_decode(&[input.clone()])?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);

        Ok(())
    }
}
