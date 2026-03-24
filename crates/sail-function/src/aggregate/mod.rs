/// Utility macro to match string data types and create corresponding ScalarValue.
///
/// This macro handles the three string type variants (Utf8View, LargeUtf8, Utf8)
/// and creates the appropriate ScalarValue variant.
///
/// # Arguments
/// * `$data_type` - The DataType enum to match against
/// * `$value` - The Option<String> value to wrap in the ScalarValue
///
/// # Returns
/// * `Result<ScalarValue>` - The corresponding string ScalarValue variant
///
/// # Example
/// ```ignore
/// let scalar = match_string_type!(DataType::Utf8View, Some("hello".to_string()))?;
/// ```
macro_rules! match_string_type {
    ($data_type:expr, $value:expr) => {
        match $data_type {
            DataType::Utf8View => Ok(ScalarValue::Utf8View($value)),
            DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8($value)),
            _ => Ok(ScalarValue::Utf8($value)),
        }
    };
}

/// Utility macro to create a None ScalarValue for interval types.
///
/// This macro handles the three interval unit variants (YearMonth, DayTime, MonthDayNano)
/// and creates the appropriate ScalarValue::Interval* variant with None value.
///
/// # Arguments
/// * `$unit` - The IntervalUnit enum to match against
///
/// # Returns
/// * `Result<ScalarValue>` - The corresponding interval ScalarValue variant with None
///
/// # Example
/// ```ignore
/// let scalar = interval_none!(IntervalUnit::YearMonth)?;
/// ```
macro_rules! interval_none {
    ($unit:expr) => {
        match $unit {
            datafusion::arrow::datatypes::IntervalUnit::YearMonth => {
                Ok(ScalarValue::IntervalYearMonth(None))
            }
            datafusion::arrow::datatypes::IntervalUnit::DayTime => {
                Ok(ScalarValue::IntervalDayTime(None))
            }
            datafusion::arrow::datatypes::IntervalUnit::MonthDayNano => {
                Ok(ScalarValue::IntervalMonthDayNano(None))
            }
        }
    };
}

/// Macro to handle Duration type matching for None values
macro_rules! duration_none {
    ($unit:expr) => {
        match $unit {
            datafusion::arrow::datatypes::TimeUnit::Second => Ok(ScalarValue::DurationSecond(None)),
            datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                Ok(ScalarValue::DurationMillisecond(None))
            }
            datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                Ok(ScalarValue::DurationMicrosecond(None))
            }
            datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                Ok(ScalarValue::DurationNanosecond(None))
            }
        }
    };
}

pub mod histogram_numeric;
pub mod kurtosis;
pub mod max_min_by;
pub mod mode;
pub mod percentile;
pub mod percentile_disc;
pub mod percentile_disc_groups;
pub mod skewness;
pub mod try_avg;
pub mod utils;
