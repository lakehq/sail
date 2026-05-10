use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    as_largestring_array, as_string_array, Array, ArrayRef, AsArray, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion_common::cast::as_string_view_array;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

const BIN_SUPPORTED_TYPES: &str = "Int64 or String";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBin {
    signature: Signature,
}

impl Default for SparkBin {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBin {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(invalid_arg_count_exec_err("bin", (1, 1), args.args.len()));
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value.map(bin))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.as_deref().and_then(bin_string)),
            )),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Int64 => {
                    let array = array.as_primitive::<Int64Type>();
                    let result = if array.null_count() == 0 {
                        // Fast path: No nulls
                        StringArray::from_iter_values(
                            array.values().iter().map(|value| bin(*value)),
                        )
                    } else {
                        // Safe path: Has nulls
                        array.iter().map(|v| v.map(bin)).collect()
                    };
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                DataType::Utf8 => {
                    let array = as_string_array(array);
                    let result: StringArray =
                        array.iter().map(|v| v.and_then(bin_string)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                DataType::LargeUtf8 => {
                    let array = as_largestring_array(array);
                    let result: StringArray =
                        array.iter().map(|v| v.and_then(bin_string)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                DataType::Utf8View => {
                    let array = as_string_view_array(array)?;
                    let result: StringArray =
                        array.iter().map(|v| v.and_then(bin_string)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                other => Err(unsupported_data_type_exec_err(
                    "bin",
                    BIN_SUPPORTED_TYPES,
                    other,
                )),
            },
            other => Err(unsupported_data_type_exec_err(
                "bin",
                BIN_SUPPORTED_TYPES,
                &other.data_type(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("bin", (1, 1), arg_types.len()));
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(vec![arg_types[0].clone()])
            }
            _ => Ok(vec![DataType::Int64]),
        }
    }
}

fn bin_string(value: &str) -> Option<String> {
    value.trim().parse::<i64>().ok().map(bin)
}

fn bin(value: i64) -> String {
    // TODO: This is not performant, but it's a simple implementation
    if value >= 0 {
        format!("{value:b}")
    } else {
        format!("{value:064b}")
    }
}
