use datafusion::arrow::array::types::{Decimal128Type, Int32Type, Time64MicrosecondType};
use datafusion::arrow::array::{Array, AsArray, Int32Array, PrimitiveArray};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::ColumnarValue;

pub(crate) fn string_array_iter(
    array: &dyn Array,
) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
    match array.data_type() {
        datafusion::arrow::datatypes::DataType::Utf8 => {
            Ok(Box::new(as_string_array(array)?.iter()))
        }
        datafusion::arrow::datatypes::DataType::LargeUtf8 => {
            Ok(Box::new(as_large_string_array(array)?.iter()))
        }
        datafusion::arrow::datatypes::DataType::Utf8View => {
            Ok(Box::new(as_string_view_array(array)?.iter()))
        }
        data_type => exec_err!("expected string array, got {data_type}"),
    }
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

/// Mirrors Spark's `INVALID_PARAMETER_VALUE.TIME_UNIT`, raised by
/// `QueryExecutionErrors.invalidTimeUnitError` for `time_diff` and `time_trunc`.
/// The doubled quotes around the unit are Spark's: `toSQLValue` already quotes the value and
/// the message template quotes it again.
pub(crate) fn invalid_time_unit_err<T>(fn_name: &str, unit: &str) -> Result<T> {
    exec_err!(
        "The value of parameter(s) `unit` in `{fn_name}` is invalid: expects one of the units \
         'HOUR', 'MINUTE', 'SECOND', 'MILLISECOND', 'MICROSECOND', but got ''{unit}''."
    )
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
