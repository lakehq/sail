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
    parse_spark_string_i64(value).map(bin)
}

fn parse_spark_string_i64(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(value) = value.parse::<i64>() {
        return Some(value);
    }

    let (negative, value) = match value.as_bytes()[0] {
        b'+' => (false, &value[1..]),
        b'-' => (true, &value[1..]),
        _ => (false, value),
    };
    let (integer, fraction) = value.split_once('.')?;
    if fraction.contains('.') || (integer.is_empty() && fraction.is_empty()) {
        return None;
    }
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if integer.trim_start_matches('0').len() > 19 {
        return None;
    }

    let magnitude = integer.bytes().try_fold(0_u128, |acc, b| {
        acc.checked_mul(10)?.checked_add((b - b'0') as u128)
    })?;
    if negative {
        match magnitude {
            0..=9223372036854775807 => Some(-(magnitude as i64)),
            9223372036854775808 => Some(i64::MIN),
            _ => None,
        }
    } else if magnitude <= i64::MAX as u128 {
        Some(magnitude as i64)
    } else {
        None
    }
}

fn bin(value: i64) -> String {
    // TODO: This is not performant, but it's a simple implementation
    if value >= 0 {
        format!("{value:b}")
    } else {
        format!("{value:064b}")
    }
}
