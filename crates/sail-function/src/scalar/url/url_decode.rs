use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use percent_encoding::percent_decode;

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
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

    /// Decodes a URL-encoded string from application/x-www-form-urlencoded format.
    ///
    /// # Arguments
    ///
    /// * `value` - The URL-encoded string to decode
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - The decoded string
    /// * `Err(DataFusionError)` - If the input is malformed or contains invalid UTF-8
    ///
    fn decode(value: &str) -> Result<String> {
        // Check if the string has valid percent encoding
        // TODO: Support `try_url_decode`
        Self::validate_percent_encoding(value)?;

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

    /// Validate percent-encoding of the string
    fn validate_percent_encoding(value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'%' {
                // Check if we have at least 2 more characters
                if i + 2 >= bytes.len() {
                    return exec_err!(
                        "Invalid percent-encoding: incomplete sequence at position {}",
                        i
                    );
                }

                let hex1 = bytes[i + 1];
                let hex2 = bytes[i + 2];

                if !hex1.is_ascii_hexdigit() || !hex2.is_ascii_hexdigit() {
                    return exec_err!(
                        "Invalid percent-encoding: invalid hex sequence '%{}{}' at position {}",
                        hex1 as char,
                        hex2 as char,
                        i
                    );
                }
                i += 3;
            } else {
                i += 1;
            }
        }
        Ok(())
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
            _ => plan_err!("1st argument should be STRING, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_url_decode, vec![])(&args)
    }
}

/// Core implementation of URL decoding function.
///
/// # Arguments
///
/// * `args` - A slice containing exactly one ArrayRef with the URL-encoded strings to decode
///
/// # Returns
///
/// * `Ok(ArrayRef)` - A new array of the same type containing decoded strings
/// * `Err(DataFusionError)` - If validation fails or invalid arguments are provided
///
fn spark_url_decode(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`url_decode` expects 1 argument");
    }

    match &args[0].data_type() {
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
    }
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
            Some("inva+lid://user:pass@host/file\\;param?query\\;p2"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            None,
        ]));
        let expected = StringArray::from(vec![
            Some("https://spark.apache.org"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            Some("inva lid://user:pass@host/file\\;param?query\\;p2"),
            None,
        ]);

        let result = spark_url_decode(&[input as ArrayRef])?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn test_decode_error() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("http%3A%2F%2spark.apache.org"), // '%2s' is not a valid percent encoded character
            // Valid cases
            Some("https%3A%2F%2Fspark.apache.org"),
            None,
        ]));

        let result = spark_url_decode(&[input]);
        assert!(result.is_err_and(|e| e.to_string().contains("Invalid percent-encoding")));

        Ok(())
    }
}
