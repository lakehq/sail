use std::sync::Arc;

use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use base64::engine::DecodePaddingMode;
use base64::{alphabet, Engine as _};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BinaryViewArray, FixedSizeBinaryArray,
    GenericStringBuilder, LargeBinaryArray, LargeBinaryBuilder, LargeStringArray, OffsetSizeTrait,
    StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

const SPARK_BASE64_DECODE: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBase64 {
    signature: Signature,
}

impl Default for SparkBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBase64 {
    fn name(&self) -> &str {
        "spark_base64"
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
            DataType::Utf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::BinaryView => Ok(DataType::Utf8),
            DataType::LargeUtf8 | DataType::LargeBinary => Ok(DataType::LargeUtf8),
            DataType::Null => Ok(DataType::Utf8),
            _ => plan_err!(
                "1st argument should be String or Binary, got {}",
                arg_types[0]
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "Spark `base64` function requires 1 argument, got {}",
                args.len()
            );
        };

        match arg {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(
                    value
                        .as_ref()
                        .map(|value| STANDARD.encode(value.as_slice())),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(
                    value
                        .as_ref()
                        .map(|value| STANDARD.encode(value.as_bytes())),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(value))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(value))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value
                        .as_ref()
                        .map(|value| STANDARD.encode(value.as_slice())),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value
                        .as_ref()
                        .map(|value| STANDARD.encode(value.as_bytes())),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Binary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to BinaryArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i32>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i),
                    ))
                }
                DataType::BinaryView => {
                    let array = array
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to BinaryViewArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i32>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i),
                    ))
                }
                DataType::FixedSizeBinary(_) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to FixedSizeBinaryArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i32>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i),
                    ))
                }
                DataType::LargeBinary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to LargeBinaryArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i64>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i),
                    ))
                }
                DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to StringArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i32>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i).as_bytes(),
                    ))
                }
                DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to LargeStringArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i64>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i).as_bytes(),
                    ))
                }
                DataType::Utf8View => {
                    let array = array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `base64`: Failed to downcast Expr to StringViewArray"
                            )
                        })?;
                    Ok(encode_spark_base64_array::<i32>(
                        array.len(),
                        |i| array.is_null(i),
                        |i| array.value(i).as_bytes(),
                    ))
                }
                DataType::Null => Ok(encode_spark_base64_array::<i32>(
                    array.len(),
                    |_| true,
                    |_| &[],
                )),
                other => {
                    exec_err!("Spark `base64`: Expr array must be BINARY or STRING, got array of type {other}")
                }
            }
            .map(ColumnarValue::Array),
            other => exec_err!("Spark `base64`: Expr must be BINARY or STRING, got {other:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnbase64 {
    signature: Signature,
}

impl Default for SparkUnbase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnbase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(
                vec![
                    DataType::Null,
                    DataType::Utf8View,
                    DataType::Utf8,
                    DataType::LargeUtf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

fn encode_spark_base64_array<'a, O: OffsetSizeTrait>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    value: impl Fn(usize) -> &'a [u8],
) -> ArrayRef {
    // base64 expands 3 input bytes into 4 output chars; without per-row input
    // sizes, pre-size the values buffer with a small average to cut regrows.
    // Encode into one reused `String` buffer (no per-row allocation) and append
    // straight into the builder instead of collecting a `Vec<Option<String>>`.
    let mut builder = GenericStringBuilder::<O>::with_capacity(len, len * 16);
    let mut buf = String::new();
    for i in 0..len {
        if is_null(i) {
            builder.append_null();
        } else {
            buf.clear();
            STANDARD.encode_string(value(i), &mut buf);
            builder.append_value(&buf);
        }
    }
    Arc::new(builder.finish())
}

fn decode_spark_base64(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    match SPARK_BASE64_DECODE.decode(bytes) {
        Ok(value) => Ok(value),
        Err(error) => {
            if bytes.iter().any(|byte| !is_spark_base64_byte(*byte)) {
                let filtered_bytes: Vec<u8> = bytes
                    .iter()
                    .copied()
                    .filter(|byte| is_spark_base64_byte(*byte))
                    .collect();
                SPARK_BASE64_DECODE
                    .decode(filtered_bytes.as_slice())
                    .map_err(|e| exec_datafusion_err!("Spark `unbase64`: {e}"))
            } else {
                Err(exec_datafusion_err!("Spark `unbase64`: {error}"))
            }
        }
    }
}

fn is_spark_base64_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')
}

fn decode_spark_base64_array<'a>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    value: impl Fn(usize) -> &'a str,
) -> Result<Vec<Option<Vec<u8>>>> {
    let mut results = Vec::with_capacity(len);
    for i in 0..len {
        if is_null(i) {
            results.push(None);
        } else {
            results.push(Some(decode_spark_base64(value(i))?));
        }
    }
    Ok(results)
}

impl ScalarUDFImpl for SparkUnbase64 {
    fn name(&self) -> &str {
        "spark_unbase64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = arg_types else {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        };
        match arg_type {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
            DataType::LargeUtf8 => Ok(DataType::LargeBinary),
            DataType::Null => Ok(DataType::Binary),
            _ => plan_err!("1st argument should be String, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "Spark `unbase64` function requires 1 argument, got {}",
                args.len()
            );
        };

        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(value))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                    value
                        .as_ref()
                        .map(|value| decode_spark_base64(value))
                        .transpose()?,
                )))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(
                    value
                        .as_ref()
                        .map(|value| decode_spark_base64(value))
                        .transpose()?,
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `unbase64`: Failed to downcast Expr to StringArray"
                            )
                        })?;
                    decode_spark_base64_array(array.len(), |i| array.is_null(i), |i| array.value(i))
                }
                DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `unbase64`: Failed to downcast Expr to LargeStringArray"
                            )
                        })?;
                    decode_spark_base64_array(array.len(), |i| array.is_null(i), |i| array.value(i))
                }
                DataType::Utf8View => {
                    let array = array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "Spark `unbase64`: Failed to downcast Expr to StringViewArray"
                            )
                        })?;
                    decode_spark_base64_array(array.len(), |i| array.is_null(i), |i| array.value(i))
                }
                DataType::Null => decode_spark_base64_array(array.len(), |_| true, |_| ""),
                other => exec_err!(
                    "Spark `unbase64`: Expr array must be STRING, got array of type {other}"
                ),
            }
            .and_then(|results| match args[0].data_type() {
                DataType::Null | DataType::Utf8 | DataType::Utf8View => {
                    let mut builder = BinaryBuilder::new();
                    for value in results {
                        match value {
                            Some(value) => builder.append_value(value.as_slice()),
                            None => builder.append_null(),
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                }
                DataType::LargeUtf8 => {
                    let mut builder = LargeBinaryBuilder::new();
                    for value in results {
                        match value {
                            Some(value) => builder.append_value(value.as_slice()),
                            None => builder.append_null(),
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                }
                _ => plan_err!("1st argument should be String, got {}", args[0].data_type()),
            }),
            other => exec_err!("Spark `unbase64`: Expr must be STRING, got {other:?}"),
        }
    }
}
