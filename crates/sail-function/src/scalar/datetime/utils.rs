use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, plan_datafusion_err, Result};
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

    Ok(result)
}
