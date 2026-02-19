use datafusion::arrow::array::types::{
    Date32Type, Float64Type, Int32Type, Time64MicrosecondType, UInt32Type,
};
use datafusion::arrow::array::{AsArray, Float64Array, Int32Array, PrimitiveArray, UInt32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use regex::Regex;

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

pub fn spark_datetime_format_to_chrono_strftime(format: &str) -> Result<String> {
    // TODO: This doesn't cover everything.
    //  https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    //  https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers

    let patterns = [
        // Fractional seconds patterns (from nanoseconds to deciseconds)
        ("SSSSSSSSS", "%.9f"), // Nanoseconds
        ("SSSSSSSS", "%.8f"),
        ("SSSSSSS", "%.7f"),
        ("SSSSSS", "%.6f"), // Microseconds
        ("SSSSS", "%.5f"),
        ("SSSS", "%.4f"),
        ("SSS", "%.3f"), // Milliseconds
        ("SS", "%.2f"),  // Centiseconds
        ("S", "%.1f"),   // Deciseconds
        // Year patterns
        ("yyyy", "%Y"),
        ("yyy", "%Y"),
        ("yy", "%y"),
        ("y", "%Y"),
        // Day-of-year pattern
        ("D", "%j"),
        // Month patterns
        ("MMMM", "%B"),
        ("MMM", "%b"),
        ("MM", "%m"),
        ("M", "%-m"),
        ("LLLL", "%B"),
        ("LLL", "%b"),
        ("LL", "%m"),
        ("L", "%-m"),
        // Day-of-month patterns
        ("dd", "%d"),
        ("d", "%-d"),
        // Weekday patterns
        ("EEEE", "%A"),
        ("EEE", "%a"),
        ("E", "%a"),
        // Hour patterns
        ("hh", "%I"), // 12-hour clock (01–12)
        ("h", "%-I"), // 12-hour clock (1–12)
        ("HH", "%H"), // 24-hour clock (00–23)
        ("H", "%-H"), // 24-hour clock (0–23)
        ("KK", "%I"), // 12-hour clock (01–12), but Spark's 'K' is 0–11
        ("K", "%l"),  // 12-hour clock (1–12), space-padded
        // Minute patterns
        ("mm", "%M"),
        ("m", "%-M"),
        // Second patterns
        ("ss", "%S"),
        ("s", "%-S"),
        // AM/PM
        ("a", "%p"),
        // Timezone patterns
        ("XXXXX", "%::z"), // ±HH:MM:SS
        ("XXXX", "%z"),    // ±HHMM
        ("XXX", "%:z"),    // ±HH:MM
        ("XX", "%z"),      // ±HHMM
        ("X", "%z"),       // ±HHMM
        ("xxxxx", "%::z"), // ±HH:MM:SS
        ("xxxx", "%z"),    // ±HHMM
        ("xxx", "%:z"),    // ±HH:MM
        ("xx", "%z"),      // ±HHMM
        ("x", "%z"),       // ±HHMM
        ("ZZZZZ", "%::z"), // ±HH:MM:SS
        ("ZZZZ", "%:z"),   // ±HH:MM
        ("ZZZ", "%z"),     // ±HHMM
        ("ZZ", "%z"),      // ±HHMM
        ("Z", "%z"),       // ±HHMM
        ("zzzz", "%Z"),
        ("zzz", "%Z"),
        ("zz", "%Z"),
        ("z", "%Z"),
        ("OOOO", "%Z"),
        ("OO", "%Z"),
        ("VV", "%Z"),
    ];

    // TODO: Create regex using lazy_static
    let mut result = format.to_string();
    for &(pattern, replacement) in &patterns {
        let regex_pattern = format!(r"(?P<pre>^|[^%]){}", regex::escape(pattern));
        let re = Regex::new(&regex_pattern).map_err(|e| {
            plan_datafusion_err!("failed to create regex pattern for '{pattern}': {e}")
        })?;
        let replacement_str = format!("${{pre}}{replacement}");
        result = re
            .replace_all(&result, replacement_str.as_str())
            .to_string()
    }

    // Fix double-dot issue: chrono's %.Nf already includes a leading dot,
    // so when the Spark format has a literal '.' before S-patterns (e.g., "ss.SSS"),
    // the result would have ".%.Nf" which produces "..NNN". Remove the extra dot.
    result = result.replace(".%.", "%.");

    Ok(result)
}

// Shared array conversion helpers for make_timestamp functions

pub(crate) fn to_date32_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Date32Type>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Date32Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Date32(Some(value))) => Ok(
            PrimitiveArray::<Date32Type>::from_value(*value, number_rows),
        ),
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

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

pub(crate) fn to_uint32_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<UInt32Array> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<UInt32Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(value))) => {
            Ok(UInt32Array::from_value(*value, number_rows))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

pub(crate) fn to_float64_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<Float64Array> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Float64Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
            Ok(Float64Array::from_value(*value, number_rows))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}
