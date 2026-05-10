use std::any::Any;
use std::sync::Arc;

use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use base64::engine::DecodePaddingMode;
use base64::{alphabet, Engine as _};
use datafusion::arrow::array::{
    Array, BinaryArray, BinaryBuilder, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray,
    LargeBinaryBuilder, LargeStringArray, StringArray, StringViewArray,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

        let results = match arg {
            ColumnarValue::Scalar(ScalarValue::Binary(None))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(None))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, None))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(None))
            | ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
            | ColumnarValue::Scalar(ScalarValue::Null) => Ok(vec![None]),
            ColumnarValue::Scalar(ScalarValue::Binary(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(expr))) => {
                let results = vec![Some(STANDARD.encode(expr.as_slice()))];
                Ok(results)
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => {
                let results = vec![Some(STANDARD.encode(expr.as_bytes()))];
                Ok(results)
            }
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut results = Vec::with_capacity(len);

                for i in 0..len {
                    if array.is_null(i) {
                        results.push(None);
                        continue;
                    }
                    let value = match array.data_type() {
                        DataType::Binary => {
                            let array = array.as_any().downcast_ref::<BinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to BinaryArray"))?;
                            Ok(array.value(i))
                        },
                        DataType::BinaryView => {
                            let array = array.as_any().downcast_ref::<BinaryViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to LargeBinaryArray"))?;
                            Ok(array.value(i))
                        },
                        DataType::FixedSizeBinary(_) => {
                            let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to FixedSizeBinaryArray"))?;
                            Ok(array.value(i))
                        },
                        DataType::LargeBinary => {
                            let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to LargeBinaryArray"))?;
                            Ok(array.value(i))
                        },
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to StringArray"))?;
                            Ok(array.value(i).as_bytes())
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to LargeStringArray"))?;
                            Ok(array.value(i).as_bytes())
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `base64`: Failed to downcast Expr to StringViewArray"))?;
                            Ok(array.value(i).as_bytes())
                        },
                        other => exec_err!("Spark `base64`: Expr array must be BINARY or STRING, got array of type {other}")
                    }?;
                    results.push(Some(STANDARD.encode(value)));
                }
                Ok(results)
            }
            other => exec_err!("Spark `base64`: Expr must be BINARY or STRING, got {other:?}"),
        }?;

        match args[0].data_type() {
            DataType::Utf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::BinaryView => {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
            }
            DataType::LargeUtf8 | DataType::LargeBinary => Ok(ColumnarValue::Array(Arc::new(
                LargeStringArray::from(results),
            ))),
            DataType::Null => Ok(ColumnarValue::Array(Arc::new(StringArray::from(results)))),
            _ => plan_err!(
                "1st argument should be String or Binary, got {}",
                args[0].data_type()
            ),
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

fn decode_spark_base64(value: &str) -> Option<Vec<u8>> {
    let filtered_bytes: Vec<u8> = value
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    SPARK_BASE64_DECODE.decode(filtered_bytes.as_slice()).ok()
}

impl ScalarUDFImpl for SparkUnbase64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

        let results = match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
            | ColumnarValue::Scalar(ScalarValue::Null) => Ok(vec![None]),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => {
                let results = vec![decode_spark_base64(expr.as_str())];
                Ok(results)
            }
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut results = Vec::with_capacity(len);

                for i in 0..len {
                    if array.is_null(i) {
                        results.push(None);
                        continue;
                    }
                    let value = match array.data_type() {
                        DataType::Utf8 => {
                            let array =
                                array
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .ok_or_else(|| {
                                        exec_datafusion_err!(
                                        "Spark `unbase64`: Failed to downcast Expr to StringArray"
                                    )
                                    })?;
                            Ok(array.value(i))
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
                            Ok(array.value(i))
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
                            Ok(array.value(i))
                        }
                        other => exec_err!(
                            "Spark `unbase64`: Expr array must be STRING, got array of type {other}"
                        ),
                    }?;
                    results.push(decode_spark_base64(value));
                }
                Ok(results)
            }
            other => exec_err!("Spark `unbase64`: Expr must be STRING, got {other:?}"),
        }?;

        match args[0].data_type() {
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
        }
    }
}
