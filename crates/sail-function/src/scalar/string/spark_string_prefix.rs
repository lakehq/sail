use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// The planner uses this for substring(string, 0 or 1, nonnegative literal length).
/// Spark's UTF8String.substringSQL counts Unicode code points and clips at the end.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStringPrefix {
    signature: Signature,
}

impl Default for SparkStringPrefix {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStringPrefix {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
                    .into_iter()
                    .map(|string| TypeSignature::Exact(vec![string, DataType::Int64]))
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkStringPrefix {
    fn name(&self) -> &str {
        "spark_string_prefix"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Keep Sail's existing substring output type, including for Utf8View input.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [
            string,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(length))),
        ] = args.args.as_slice()
        else {
            return internal_err!("string prefix requires a string and a literal length");
        };
        if !(0..=i64::from(i32::MAX)).contains(length) {
            return internal_err!("string prefix length must be a nonnegative int32");
        }
        let length = *length as usize;
        match string {
            ColumnarValue::Scalar(string) => {
                let Some(string) = string.try_as_str() else {
                    return internal_err!("string prefix requires a string input");
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    string.map(|s| prefix(s, length).to_owned()),
                )))
            }
            ColumnarValue::Array(array) => {
                // Utf8 input already fits its offset range. For large/view input,
                // bound the output by four bytes per requested code point. Retain
                // the existing fallible cast if it might exceed Utf8's offsets.
                if array.data_type() != &DataType::Utf8
                    && array.len().saturating_mul(length).saturating_mul(4) > i32::MAX as usize
                {
                    args.args
                        .insert(1, ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
                    args.arg_fields
                        .insert(1, Arc::new(Field::new("position", DataType::Int64, false)));
                    let result = datafusion_functions::unicode::substr()
                        .inner()
                        .invoke_with_args(args)?;
                    let ColumnarValue::Array(result) = result else {
                        return internal_err!("substring of an array must return an array");
                    };
                    return Ok(ColumnarValue::Array(cast(&result, &DataType::Utf8)?));
                }
                let result: StringArray = match array.data_type() {
                    DataType::Utf8 => array
                        .as_string::<i32>()
                        .iter()
                        .map(|s| s.map(|s| prefix(s, length)))
                        .collect(),
                    DataType::LargeUtf8 => array
                        .as_string::<i64>()
                        .iter()
                        .map(|s| s.map(|s| prefix(s, length)))
                        .collect(),
                    DataType::Utf8View => array
                        .as_string_view()
                        .iter()
                        .map(|s| s.map(|s| prefix(s, length)))
                        .collect(),
                    other => return internal_err!("unexpected string prefix input type: {other}"),
                };
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

fn prefix(string: &str, length: usize) -> &str {
    if length >= string.len() {
        return string;
    }
    // Only inspect the requested prefix: scanning the entire string penalizes
    // short prefixes of long inputs. ASCII bytes also establish a UTF-8 boundary.
    let end = if string.as_bytes()[..length].is_ascii() {
        length
    } else {
        string
            .char_indices()
            .nth(length)
            .map_or(string.len(), |(offset, _)| offset)
    };
    &string[..end]
}
