macro_rules! match_string_type {
    ($data_type:expr, $value:expr) => {
        match $data_type {
            DataType::Utf8View => Ok(ScalarValue::Utf8View($value)),
            DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8($value)),
            _ => Ok(ScalarValue::Utf8($value)),
        }
    };
}

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

pub mod kurtosis;
pub mod max_min_by;
pub mod mode;
pub mod percentile;
pub mod skewness;
pub mod try_avg;
pub mod try_sum;
