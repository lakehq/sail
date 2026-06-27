use datafusion::arrow::array::types::{Decimal128Type, Int32Type, Time64MicrosecondType};
use datafusion::arrow::array::{AsArray, Int32Array, PrimitiveArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// [Credit]: <https://github.com/apache/datafusion/blob/d8e4e92daf7f20eef9af6919a8061192f7505043/datafusion/functions/src/datetime/common.rs#L45-L67>
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str, skip: usize) -> Result<()> {
    for (idx, a) in args.iter().skip(skip).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

// Shared array conversion helpers for make_timestamp functions

pub(crate) fn to_time64_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Time64MicrosecondType>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Time64MicrosecondType>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(value))) => {
            Ok(PrimitiveArray::<Time64MicrosecondType>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

/// Reads a `Decimal128` column as its raw unscaled `i128` values.
pub(crate) fn to_decimal128_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Decimal128Type>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Decimal128Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), _, _)) => {
            Ok(PrimitiveArray::<Decimal128Type>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

pub(crate) fn to_int32_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<Int32Array> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Int32Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
            Ok(Int32Array::from_value(*value, number_rows))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}
