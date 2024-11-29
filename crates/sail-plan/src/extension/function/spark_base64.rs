use std::any::Any;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use datafusion::arrow::array::{
    BinaryArray, BinaryBuilder, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray,
    LargeBinaryBuilder, LargeStringArray, StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
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
            _ => plan_err!(
                "1st argument should be String or Binary, got {}",
                arg_types[0]
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `base64` function requires 1 argument, got {:?}",
                args
            );
        }

        let results = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(expr))) => {
                let results = vec![STANDARD.encode(expr.as_slice())];
                Ok(results)
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => {
                let results = vec![STANDARD.encode(expr.as_bytes())];
                Ok(results)
            }
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut results = Vec::with_capacity(len);

                for i in 0..len {
                    let value =  match array.data_type() {
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
                    results.push(STANDARD.encode(value));
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
            _ => plan_err!(
                "1st argument should be String or Binary, got {}",
                args[0].data_type()
            ),
        }
    }
}

#[derive(Debug)]
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
                vec![DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
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
        if arg_types.len() != 1 {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
            DataType::LargeUtf8 => Ok(DataType::LargeBinary),
            _ => plan_err!("1st argument should be String, got {}", arg_types[0]),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `unbase64` function requires 1 argument, got {:?}",
                args
            );
        }

        let results = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => {
                let results = vec![STANDARD.decode(expr.as_str()).map_err(|e| {
                    exec_datafusion_err!("Spark `unbase64`: to decode base64 string: {e}")
                })?];
                Ok(results)
            }
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut results = Vec::with_capacity(len);

                for i in 0..len {
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
                    results.push(STANDARD.decode(value).map_err(|e| {
                        exec_datafusion_err!("Spark `unbase64`: to decode base64 string: {e}")
                    })?);
                }
                Ok(results)
            }
            other => exec_err!("Spark `unbase64`: Expr must be STRING, got {other:?}"),
        }?;

        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => {
                let mut builder = BinaryBuilder::new();
                for value in results {
                    builder.append_value(value.as_slice());
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            DataType::LargeUtf8 => {
                let mut builder = LargeBinaryBuilder::new();
                for value in results {
                    builder.append_value(value.as_slice());
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => plan_err!("1st argument should be String, got {}", args[0].data_type()),
        }
    }
}
