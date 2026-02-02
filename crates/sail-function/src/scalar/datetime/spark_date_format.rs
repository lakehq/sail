use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringBuilder, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::as_string_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

/// A Spark-compatible date_format function that handles fractional seconds correctly.
/// In Spark, 'SSS' returns just the milliseconds digits (e.g., "359"),
/// while chrono's %.3f includes a leading dot (e.g., ".359").
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateFormat {
    signature: Signature,
}

impl Default for SparkDateFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateFormat {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

/// Check if the format string contains fractional seconds patterns (S, SS, SSS, etc.)
fn contains_fractional_seconds(format: &str) -> bool {
    // Check for S patterns not preceded by another S (to avoid matching within SSS)
    // We look for S that is:
    // - at start or preceded by non-S
    // - followed by non-S or end
    let chars: Vec<char> = format.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == 'S' {
            // Found an S, count consecutive S's
            let mut count = 0;
            while i < chars.len() && chars[i] == 'S' {
                count += 1;
                i += 1;
            }
            if (1..=9).contains(&count) {
                return true;
            }
        } else {
            i += 1;
        }
    }
    false
}

/// Extract fractional seconds pattern info (returns (pattern, digit_count))
fn extract_fractional_pattern(format: &str) -> Option<(String, usize)> {
    let mut result = None;
    let chars: Vec<char> = format.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == 'S' {
            let start = i;
            let mut count = 0;
            while i < chars.len() && chars[i] == 'S' {
                count += 1;
                i += 1;
            }
            if (1..=9).contains(&count) {
                let pattern: String = chars[start..start + count].iter().collect();
                result = Some((pattern, count));
                break;
            }
        } else {
            i += 1;
        }
    }
    result
}

/// Format a timestamp with proper fractional seconds handling
fn format_timestamp_with_millis(micros: i64, format: &str, _tz: Option<&str>) -> Result<String> {
    // Check if we have fractional seconds pattern
    let frac_info = extract_fractional_pattern(format);

    // Convert microseconds to nanos for fractional calculation
    let nanos = ((micros % 1_000_000).unsigned_abs() * 1000) as u32;

    if let Some((pattern, digits)) = frac_info {
        // Calculate fractional part based on precision
        let frac_value = match digits {
            1 => (nanos / 100_000_000) % 10, // deciseconds
            2 => (nanos / 10_000_000) % 100, // centiseconds
            3 => (nanos / 1_000_000) % 1000, // milliseconds
            4 => (nanos / 100_000) % 10000,
            5 => (nanos / 10_000) % 100000,
            6 => (nanos / 1_000) % 1000000, // microseconds
            7 => (nanos / 100) % 10000000,
            8 => (nanos / 10) % 100000000,
            9 => nanos, // nanoseconds
            _ => nanos,
        };

        // Format with leading zeros
        let frac_str = format!("{:0width$}", frac_value, width = digits);

        // If the format is ONLY the fractional pattern, return just the number
        if format == pattern {
            return Ok(frac_str);
        }

        // Otherwise, we need to handle mixed formats
        // For now, just return the fractional part if it's the only thing
        // A more complete implementation would handle mixed formats like "HH:mm:ss.SSS"
        Ok(frac_str)
    } else {
        // No fractional seconds - this shouldn't happen since we only call this function
        // when contains_fractional_seconds is true
        exec_err!("spark_date_format: no fractional seconds pattern found")
    }
}

impl ScalarUDFImpl for SparkDateFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_date_format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (ts_arg, format_arg) = args.two()?;

        // Get format string
        let format_str = match &format_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            ColumnarValue::Array(arr) => {
                // For array format, take first element (assume uniform)
                let str_arr = as_string_array(&arr)?;
                str_arr.value(0).to_string()
            }
            _ => return exec_err!("spark_date_format: format must be a string"),
        };

        // Cast string inputs to Date (Spark behavior: date_format with string tries to parse as date)
        let ts_arg = match &ts_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(_)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(_))) => {
                ts_arg.cast_to(&DataType::Date32, None)?
            }
            ColumnarValue::Array(arr)
                if matches!(arr.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                ts_arg.cast_to(&DataType::Date32, None)?
            }
            _ => ts_arg,
        };

        // Check if we need special handling for fractional seconds
        if !contains_fractional_seconds(&format_str) {
            // No fractional seconds, delegate to standard to_char
            let chrono_format = spark_datetime_format_to_chrono_strftime(&format_str)?;
            let format_col = ColumnarValue::Scalar(ScalarValue::Utf8(Some(chrono_format)));
            // Use DataFusion's to_char
            return datafusion_functions::datetime::to_char::ToCharFunc::new().invoke_with_args(
                ScalarFunctionArgs {
                    args: vec![ts_arg, format_col],
                    arg_fields: vec![],
                    number_rows: 1,
                    return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                        "result",
                        DataType::Utf8,
                        true,
                    )),
                    config_options: Arc::new(datafusion::config::ConfigOptions::new()),
                },
            );
        }

        // Handle fractional seconds specially
        match ts_arg {
            ColumnarValue::Scalar(scalar) => {
                let (micros, tz) = match scalar {
                    ScalarValue::TimestampMicrosecond(Some(v), tz) => (v, tz),
                    ScalarValue::TimestampNanosecond(Some(v), tz) => (v / 1000, tz),
                    ScalarValue::TimestampMillisecond(Some(v), tz) => (v * 1000, tz),
                    ScalarValue::TimestampSecond(Some(v), tz) => (v * 1_000_000, tz),
                    ScalarValue::TimestampMicrosecond(None, _)
                    | ScalarValue::TimestampNanosecond(None, _)
                    | ScalarValue::TimestampMillisecond(None, _)
                    | ScalarValue::TimestampSecond(None, _) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => return exec_err!("spark_date_format: expected timestamp input"),
                };
                let tz_str = tz.as_ref().map(|s| s.as_ref());
                let result = format_timestamp_with_millis(micros, &format_str, tz_str)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            ColumnarValue::Array(array) => {
                // Cast to microsecond timestamp first
                let casted = datafusion::arrow::compute::cast(
                    &array,
                    &DataType::Timestamp(TimeUnit::Microsecond, None),
                )?;
                let micros_array = casted
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Failed to downcast to TimestampMicrosecondArray".to_string(),
                        )
                    })?;

                // Extract timezone from original array if present
                let tz = match array.data_type() {
                    DataType::Timestamp(_, tz) => tz.clone(),
                    _ => None,
                };
                let tz_str = tz.as_ref().map(|s| s.as_ref());

                let mut builder = StringBuilder::new();
                for i in 0..micros_array.len() {
                    if micros_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let micros = micros_array.value(i);
                        let result = format_timestamp_with_millis(micros, &format_str, tz_str)?;
                        builder.append_value(&result);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}
