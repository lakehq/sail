use std::sync::Arc;

use chrono::{Datelike, Days, NaiveDateTime, Offset, TimeZone, Timelike, Utc};
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, FieldRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::temporal_conversions::as_datetime;
use datafusion_common::cast::{
    as_large_string_array, as_primitive_array, as_string_array, as_string_view_array,
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;
use sail_common_datafusion::utils::datetime::{
    SparkTimeZone, localize_with_fallback, localize_with_preferred_offset, parse_spark_timezone,
};

#[derive(Debug, Clone, Copy)]
enum Granularity {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Granularity {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "MICROSECOND" => Some(Self::Microsecond),
            "MILLISECOND" => Some(Self::Millisecond),
            "SECOND" => Some(Self::Second),
            "MINUTE" => Some(Self::Minute),
            "HOUR" => Some(Self::Hour),
            "DAY" | "DD" => Some(Self::Day),
            "WEEK" => Some(Self::Week),
            "MON" | "MONTH" | "MM" => Some(Self::Month),
            "QUARTER" => Some(Self::Quarter),
            "YEAR" | "YYYY" | "YY" => Some(Self::Year),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
    session_timezone: Arc<str>,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new(Arc::from("UTC"))
    }
}

impl SparkDateTrunc {
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            inner: DateTruncFunc::new(),
            session_timezone,
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(timestamp) = args.args.get(1) else {
            return exec_err!("date_trunc expects two arguments");
        };
        if !matches!(timestamp.data_type(), DataType::Timestamp(_, Some(_))) {
            return self.inner.invoke_with_args(args);
        }

        let granularity = &args.args[0];
        let timezone = parse_spark_timezone(&self.session_timezone)?;

        if !matches!(timestamp, ColumnarValue::Scalar(_))
            || !matches!(granularity, ColumnarValue::Scalar(_))
        {
            let array = timestamp.clone().into_array(args.number_rows)?;
            return match array.data_type() {
                DataType::Timestamp(TimeUnit::Second, Some(_)) => {
                    truncate_array::<TimestampSecondType>(&array, granularity, timezone)
                }
                DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
                    truncate_array::<TimestampMillisecondType>(&array, granularity, timezone)
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                    truncate_array::<TimestampMicrosecondType>(&array, granularity, timezone)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                    truncate_array::<TimestampNanosecondType>(&array, granularity, timezone)
                }
                data_type => exec_err!("date_trunc expected LTZ timestamp, got {data_type}"),
            };
        }

        let granularity = granularity_at(granularity, 0)?;

        match timestamp {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(value, _)) => {
                truncate_scalar::<TimestampSecondType>(*value, granularity, timezone)
            }
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(value, _)) => {
                truncate_scalar::<TimestampMillisecondType>(*value, granularity, timezone)
            }
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, _)) => {
                truncate_scalar::<TimestampMicrosecondType>(*value, granularity, timezone)
            }
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(value, _)) => {
                truncate_scalar::<TimestampNanosecondType>(*value, granularity, timezone)
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Timestamp(TimeUnit::Second, Some(_)) => {
                    truncate_array::<TimestampSecondType>(array, &args.args[0], timezone)
                }
                DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
                    truncate_array::<TimestampMillisecondType>(array, &args.args[0], timezone)
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                    truncate_array::<TimestampMicrosecondType>(array, &args.args[0], timezone)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                    truncate_array::<TimestampNanosecondType>(array, &args.args[0], timezone)
                }
                data_type => exec_err!("date_trunc expected LTZ timestamp, got {data_type}"),
            },
            _ => self.inner.invoke_with_args(args),
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

fn truncate_scalar<T: ArrowTimestampType>(
    value: Option<i64>,
    granularity: Option<Granularity>,
    timezone: SparkTimeZone,
) -> Result<ColumnarValue> {
    let value = match (value, granularity) {
        (Some(value), Some(granularity)) => {
            Some(truncate_value::<T>(value, granularity, timezone)?)
        }
        _ => None,
    };
    Ok(ColumnarValue::Scalar(ScalarValue::new_timestamp::<T>(
        value,
        Some(Arc::from("UTC")),
    )))
}

fn truncate_array<T: ArrowTimestampType>(
    array: &ArrayRef,
    granularity: &ColumnarValue,
    timezone: SparkTimeZone,
) -> Result<ColumnarValue> {
    let array = as_primitive_array::<T>(array)?;
    let values = (0..array.len())
        .map(|index| {
            if array.is_null(index) {
                return Ok(None);
            }
            let Some(granularity) = granularity_at(granularity, index)? else {
                return Ok(None);
            };
            Ok(Some(truncate_value::<T>(
                array.value(index),
                granularity,
                timezone,
            )?))
        })
        .collect::<Result<Vec<_>>>()?;
    let array = PrimitiveArray::<T>::from_iter(values).with_timezone("UTC");
    Ok(ColumnarValue::Array(Arc::new(array)))
}

fn granularity_at(value: &ColumnarValue, index: usize) -> Result<Option<Granularity>> {
    let value = match value {
        ColumnarValue::Scalar(value) => value.try_as_str().flatten(),
        ColumnarValue::Array(array) if array.is_null(index) => None,
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Some(as_string_array(array)?.value(index)),
            DataType::LargeUtf8 => Some(as_large_string_array(array)?.value(index)),
            DataType::Utf8View => Some(as_string_view_array(array)?.value(index)),
            data_type => return exec_err!("date_trunc expected string format, got {data_type}"),
        },
    };
    Ok(value.and_then(Granularity::parse))
}

fn truncate_value<T: ArrowTimestampType>(
    value: i64,
    granularity: Granularity,
    timezone: SparkTimeZone,
) -> Result<i64> {
    let divisor = match (T::UNIT, granularity) {
        (TimeUnit::Nanosecond, Granularity::Microsecond) => Some(1_000),
        (TimeUnit::Nanosecond, Granularity::Millisecond) => Some(1_000_000),
        (TimeUnit::Nanosecond, Granularity::Second) => Some(1_000_000_000),
        (TimeUnit::Microsecond, Granularity::Millisecond) => Some(1_000),
        (TimeUnit::Microsecond, Granularity::Second) => Some(1_000_000),
        (TimeUnit::Millisecond, Granularity::Second) => Some(1_000),
        (_, Granularity::Microsecond | Granularity::Millisecond | Granularity::Second) => None,
        _ => return truncate_calendar::<T>(value, granularity, timezone),
    };
    Ok(divisor.map_or(value, |divisor| value - value.rem_euclid(divisor)))
}

fn truncate_calendar<T: ArrowTimestampType>(
    value: i64,
    granularity: Granularity,
    timezone: SparkTimeZone,
) -> Result<i64> {
    let utc = as_datetime::<T>(value)
        .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range"))?;
    let zoned = Utc.from_utc_datetime(&utc).with_timezone(&timezone);
    let local = zoned.naive_local();

    let instant = match granularity {
        Granularity::Minute | Granularity::Hour | Granularity::Day => {
            let local = truncate_local_time(local, granularity)?;
            localize_with_preferred_offset(
                &timezone,
                &local,
                zoned.offset().fix().local_minus_utc(),
            )?
            .to_utc()
        }
        Granularity::Week | Granularity::Month | Granularity::Quarter | Granularity::Year => {
            let date = match granularity {
                Granularity::Week => local
                    .date()
                    .checked_sub_days(Days::new(local.weekday().num_days_from_monday() as u64)),
                Granularity::Month => local.date().with_day(1),
                Granularity::Quarter => local
                    .date()
                    .with_day(1)
                    .and_then(|date| date.with_month((date.month0() / 3) * 3 + 1)),
                Granularity::Year => local.date().with_ordinal(1),
                _ => unreachable!(),
            }
            .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range"))?;
            let local = date
                .and_hms_micro_opt(0, 0, 0, 0)
                .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range"))?;
            localize_with_fallback(&timezone, &local)?
        }
        _ => unreachable!(),
    };

    match T::UNIT {
        TimeUnit::Second => Ok(instant.timestamp()),
        TimeUnit::Millisecond => Ok(instant.timestamp_millis()),
        TimeUnit::Microsecond => Ok(instant.timestamp_micros()),
        TimeUnit::Nanosecond => instant
            .timestamp_nanos_opt()
            .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range")),
    }
}

fn truncate_local_time(local: NaiveDateTime, granularity: Granularity) -> Result<NaiveDateTime> {
    let local = match granularity {
        Granularity::Minute => local
            .with_second(0)
            .and_then(|value| value.with_nanosecond(0)),
        Granularity::Hour => local
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0)),
        Granularity::Day => local.date().and_hms_micro_opt(0, 0, 0, 0),
        _ => unreachable!(),
    };
    local.ok_or_else(|| exec_datafusion_err!("cannot truncate timestamp {local:?}"))
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone};

    use super::*;

    #[test]
    fn test_truncate_hour_preserves_later_overlap_offset() -> Result<()> {
        let timezone = parse_spark_timezone("America/Los_Angeles")?;
        let actual = truncate_value::<TimestampMicrosecondType>(
            1_572_773_445_987_654,
            Granularity::Hour,
            timezone,
        )?;

        assert_eq!(actual, 1_572_771_600_000_000);
        Ok(())
    }

    #[test]
    fn test_truncate_day_shifts_across_midnight_gap() -> Result<()> {
        let timezone = parse_spark_timezone("America/Sao_Paulo")?;
        let actual = truncate_value::<TimestampMicrosecondType>(
            1_541_302_200_000_000,
            Granularity::Day,
            timezone,
        )?;

        assert_eq!(actual, 1_541_300_400_000_000);
        Ok(())
    }

    #[test]
    fn test_truncate_supports_historical_second_offset() -> Result<()> {
        let timezone = parse_spark_timezone("Asia/Kathmandu")?;
        let local = NaiveDate::from_ymd_opt(1769, 10, 17)
            .and_then(|date| date.and_hms_micro_opt(17, 10, 2, 123_456))
            .expect("test timestamp is valid");
        let input = localize_with_fallback(&timezone, &local)?.timestamp_micros();
        let actual =
            truncate_value::<TimestampMicrosecondType>(input, Granularity::Minute, timezone)?;
        let actual = Utc
            .timestamp_micros(actual)
            .single()
            .expect("test timestamp is valid")
            .with_timezone(&timezone)
            .naive_local();

        assert_eq!(
            actual,
            NaiveDate::from_ymd_opt(1769, 10, 17)
                .and_then(|date| date.and_hms_opt(17, 10, 0))
                .expect("test timestamp is valid")
        );
        Ok(())
    }

    #[test]
    fn test_invalid_and_null_granularities_are_null() -> Result<()> {
        assert!(
            granularity_at(
                &ColumnarValue::Scalar(ScalarValue::Utf8(Some("invalid".to_string()))),
                0,
            )?
            .is_none()
        );
        assert!(granularity_at(&ColumnarValue::Scalar(ScalarValue::Utf8(None)), 0)?.is_none());
        Ok(())
    }
}
