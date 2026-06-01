use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::functions::unicode::reverse::ReverseFunc;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions_nested::reverse::array_reverse_inner;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::spark_to_string::SparkToUtf8;

/// Spark-compatible `reverse` function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#reverse>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkReverse {
    signature: Signature,
}

impl Default for SparkReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn is_array_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

fn is_string_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn needs_spark_format(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Timestamp(_, _)
    )
}

fn append_reversed_bytes<'a, I>(iter: I, builder: &mut StringBuilder, buf: &mut Vec<u8>)
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    for bytes_opt in iter {
        match bytes_opt {
            None => builder.append_null(),
            Some(bytes) => {
                buf.clear();
                buf.extend_from_slice(bytes);
                buf.reverse();
                builder.append_value(String::from_utf8_lossy(buf).as_ref());
            }
        }
    }
}

fn reverse_binary(arg: &ColumnarValue) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(sv) => {
            use datafusion_common::ScalarValue;
            let bytes = match sv {
                ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => b,
                other => {
                    return Err(unsupported_data_type_exec_err(
                        "reverse",
                        "binary scalar",
                        &other.data_type(),
                    ));
                }
            };
            let reversed: Vec<u8> = bytes.iter().copied().rev().collect();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from_utf8_lossy(&reversed).into_owned(),
            ))))
        }
        ColumnarValue::Array(array) => {
            let mut buf = Vec::new();
            let mut builder =
                StringBuilder::with_capacity(array.len(), array.get_buffer_memory_size());
            match array.data_type() {
                DataType::Binary => {
                    append_reversed_bytes(array.as_binary::<i32>().iter(), &mut builder, &mut buf);
                }
                DataType::LargeBinary => {
                    append_reversed_bytes(array.as_binary::<i64>().iter(), &mut builder, &mut buf);
                }
                DataType::BinaryView => {
                    append_reversed_bytes(array.as_binary_view().iter(), &mut builder, &mut buf);
                }
                other => {
                    return Err(unsupported_data_type_exec_err(
                        "reverse",
                        "binary array",
                        other,
                    ));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }
}

impl ScalarUDFImpl for SparkReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(dt) if is_array_type(dt) || is_string_type(dt) => Ok(dt.clone()),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let dt = args.args[0].data_type();
        if is_string_type(&dt) {
            ReverseFunc::new().invoke_with_args(args)
        } else if is_binary_type(&dt) {
            reverse_binary(&args.args[0])
        } else if is_array_type(&dt) {
            make_scalar_function(array_reverse_inner)(&args.args)
        } else if needs_spark_format(&dt) {
            // Arrow's native cast diverges from Spark for some types:
            //   float Infinity → "inf" (Arrow) vs "Infinity" (Spark)
            //   timestamp     → ISO 8601 (Arrow) vs "2024-01-15 12:30:45" (Spark)
            // SparkToUtf8 uses sail's ArrayFormatter which matches Spark semantics.
            let number_rows = args.number_rows;
            let return_field = Arc::clone(&args.return_field);
            let config_options = Arc::clone(&args.config_options);
            let arg_field = Arc::new(Field::new("", DataType::Utf8, true));
            let utf8_val = SparkToUtf8::new().invoke_with_args(args)?;
            ReverseFunc::new().invoke_with_args(ScalarFunctionArgs {
                args: vec![utf8_val],
                arg_fields: vec![arg_field],
                number_rows,
                return_field,
                config_options,
            })
        } else {
            Err(unsupported_data_type_exec_err(
                "reverse",
                "string, binary, or array",
                &dt,
            ))
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "reverse",
                (1, 1),
                arg_types.len(),
            ));
        }
        let dt = &arg_types[0];
        if is_array_type(dt) || is_string_type(dt) || is_binary_type(dt) {
            Ok(vec![dt.clone()])
        } else if matches!(dt, DataType::Null) {
            Ok(vec![DataType::Utf8])
        } else if matches!(dt, DataType::Map(_, _) | DataType::Struct(_)) {
            plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] \
                 The argument to the `reverse` function must be a string, binary, or array type, \
                 not `{dt}`."
            )
        } else if needs_spark_format(dt) {
            Ok(vec![dt.clone()])
        } else {
            Ok(vec![DataType::Utf8])
        }
    }
}
