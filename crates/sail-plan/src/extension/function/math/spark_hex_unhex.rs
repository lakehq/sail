use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

use datafusion::arrow::array::{
    as_dictionary_array, as_largestring_array, as_string_array, BinaryBuilder, OffsetSizeTrait,
    StringArray,
};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::{
    as_binary_array, as_fixed_size_binary_array, as_generic_string_array, as_int64_array,
    as_string_view_array,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr_common::signature::TypeSignature;

#[derive(Debug)]
pub struct SparkHex {
    signature: Signature,
}

impl Default for SparkHex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(
                "spark_hex expects exactly one argument".to_string(),
            ));
        }

        match &args[0] {
            ColumnarValue::Array(_) => spark_hex(args),
            ColumnarValue::Scalar(scalar) => {
                let scalar = if let ScalarValue::Int32(value) = scalar {
                    &ScalarValue::Int64(value.map(|v| v as i64))
                } else {
                    scalar
                };
                let array = scalar.to_array()?;
                let array_args = [ColumnarValue::Array(array)];
                spark_hex(&array_args)
            }
        }
    }
}

#[derive(Debug)]
pub struct SparkUnHex {
    signature: Signature,
}

impl Default for SparkUnHex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_unhex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        spark_unhex(args)
    }
}

// [Credit]: <https://github.com/apache/datafusion-comet/blob/bfd7054c02950219561428463d3926afaf8edbba/native/spark-expr/src/scalar_funcs/hex.rs>

fn hex_int64(num: i64) -> String {
    format!("{:X}", num)
}

#[inline(always)]
fn hex_encode<T: AsRef<[u8]>>(data: T, lower_case: bool) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    if lower_case {
        for b in data.as_ref() {
            // Writing to a string never errors, so we can unwrap here.
            write!(&mut s, "{b:02x}").unwrap();
        }
    } else {
        for b in data.as_ref() {
            // Writing to a string never errors, so we can unwrap here.
            write!(&mut s, "{b:02X}").unwrap();
        }
    }
    s
}

#[allow(dead_code)]
#[inline(always)]
pub(super) fn hex_strings<T: AsRef<[u8]>>(data: T) -> String {
    hex_encode(data, true)
}

#[inline(always)]
fn hex_bytes<T: AsRef<[u8]>>(bytes: T) -> Result<String, std::fmt::Error> {
    let hex_string = hex_encode(bytes, false);
    Ok(hex_string)
}

/// Spark-compatible `hex` function
pub fn spark_hex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "hex expects exactly one argument".to_string(),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => {
                let array = as_int64_array(array)?;

                let hexed_array: StringArray = array.iter().map(|v| v.map(hex_int64)).collect();

                Ok(ColumnarValue::Array(Arc::new(hexed_array)))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(hex_bytes).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(hex_bytes).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(array);

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(hex_bytes).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(hex_bytes).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(hex_bytes).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Dictionary(_, value_type) => {
                let dict = as_dictionary_array::<Int32Type>(&array);

                let values = match **value_type {
                    DataType::Int64 => as_int64_array(dict.values())?
                        .iter()
                        .map(|v| v.map(hex_int64))
                        .collect::<Vec<_>>(),
                    DataType::Utf8 => as_string_array(dict.values())
                        .iter()
                        .map(|v| v.map(hex_bytes).transpose())
                        .collect::<Result<_, _>>()?,
                    DataType::Utf8View => as_string_view_array(dict.values())?
                        .iter()
                        .map(|v| v.map(hex_bytes).transpose())
                        .collect::<Result<_, _>>()?,
                    DataType::LargeUtf8 => as_largestring_array(dict.values())
                        .iter()
                        .map(|v| v.map(hex_bytes).transpose())
                        .collect::<Result<_, _>>()?,
                    DataType::Binary => as_binary_array(dict.values())?
                        .iter()
                        .map(|v| v.map(hex_bytes).transpose())
                        .collect::<Result<_, _>>()?,
                    _ => exec_err!(
                        "hex got an unexpected argument type: {:?}",
                        array.data_type()
                    )?,
                };

                let new_values: Vec<Option<String>> = dict
                    .keys()
                    .iter()
                    .map(|key| key.map(|k| values[k as usize].clone()).unwrap_or(None))
                    .collect();

                let string_array_values = StringArray::from(new_values);

                Ok(ColumnarValue::Array(Arc::new(string_array_values)))
            }
            _ => exec_err!(
                "hex got an unexpected argument type: {:?}",
                array.data_type()
            ),
        },
        _ => exec_err!("native hex does not support scalar values at this time"),
    }
}

// [Credit]: <https://github.com/apache/datafusion-comet/blob/bfd7054c02950219561428463d3926afaf8edbba/native/spark-expr/src/scalar_funcs/unhex.rs>

/// Helper function to convert a hex digit to a binary value.
fn unhex_digit(c: u8) -> Result<u8, DataFusionError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'A'..=b'F' => Ok(10 + c - b'A'),
        b'a'..=b'f' => Ok(10 + c - b'a'),
        _ => Err(DataFusionError::Execution(
            "Input to unhex_digit is not a valid hex digit".to_string(),
        )),
    }
}

/// Convert a hex string to binary and store the result in `result`. Returns an error if the input
/// is not a valid hex string.
fn unhex(hex_str: &str, result: &mut Vec<u8>) -> Result<(), DataFusionError> {
    let bytes = hex_str.as_bytes();

    let mut i = 0;

    if (bytes.len() & 0x01) != 0 {
        let v = unhex_digit(bytes[0])?;

        result.push(v);
        i += 1;
    }

    while i < bytes.len() {
        let first = unhex_digit(bytes[i])?;
        let second = unhex_digit(bytes[i + 1])?;
        result.push((first << 4) | second);

        i += 2;
    }

    Ok(())
}

fn spark_unhex_inner<T: OffsetSizeTrait>(
    array: &ColumnarValue,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match array {
        ColumnarValue::Array(array) => {
            let string_array = as_generic_string_array::<T>(array)?;

            let mut encoded = Vec::new();
            let mut builder = BinaryBuilder::new();

            for item in string_array.iter() {
                if let Some(s) = item {
                    if unhex(s, &mut encoded).is_ok() {
                        builder.append_value(encoded.as_slice());
                    } else if fail_on_error {
                        return exec_err!("Input to unhex is not a valid hex string: {s}");
                    } else {
                        builder.append_null();
                    }
                    encoded.clear();
                } else {
                    builder.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(string)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(string)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(string))) => {
            let mut encoded = Vec::new();

            if unhex(string, &mut encoded).is_ok() {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(encoded))))
            } else if fail_on_error {
                exec_err!("Input to unhex is not a valid hex string: {string}")
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
        }
        _ => {
            exec_err!(
                "The first argument must be a string scalar or array, but got: {:?}",
                array
            )
        }
    }
}

/// Spark-compatible `unhex` expression
pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() > 2 {
        return exec_err!("unhex takes at most 2 arguments, but got: {}", args.len());
    }

    let val_to_unhex = &args[0];
    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => *fail_on_error,
            _ => {
                return exec_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match val_to_unhex.data_type() {
        DataType::Utf8 | DataType::Utf8View => {
            spark_unhex_inner::<i32>(val_to_unhex, fail_on_error)
        }
        DataType::LargeUtf8 => spark_unhex_inner::<i64>(val_to_unhex, fail_on_error),
        other => exec_err!("The first argument must be a Utf8, Utf8View, or LargeUtf8: {other:?}"),
    }
}
