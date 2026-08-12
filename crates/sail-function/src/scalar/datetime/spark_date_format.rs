use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Offset, TimeZone};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date64Array, PrimitiveArray, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::temporal_conversions::{
    as_datetime, date32_to_datetime, date64_to_datetime,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::{
    SparkTimeZone, localize_with_fallback, parse_spark_timezone,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::scalar::datetime::format::{
    DateTimeFormat, DateTimeFormatInput, TimePrecision, TimeZoneDisplay, TimestampKind,
    cached_format,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateFormat {
    session_timezone: Arc<str>,
    signature: Signature,
}

impl SparkDateFormat {
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }
}

impl ScalarUDFImpl for SparkDateFormat {
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
        if args.len() != 2 {
            return exec_err!("spark_date_format requires 2 arguments");
        }
        let (timestamp_arg, format_arg) = args.two()?;

        match (timestamp_arg, format_arg) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                let format_str = match scalar.try_as_str().flatten() {
                    Some(s) => s,
                    None => return Ok(null_string_array(array.len())),
                };

                let format = DateTimeFormat::for_formatting(format_str)?;

                let result: StringArray = match array.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        format_string_array_as_timestamp(&array, &self.session_timezone, &format)?
                    }
                    DataType::Date32 => format_date32_array(&array, &format)?,
                    DataType::Date64 => format_date64_array(&array, &format)?,
                    DataType::Timestamp(unit, tz) => format_timestamp_array_by_unit(
                        &array,
                        *unit,
                        tz.as_ref().map(|_| &self.session_timezone),
                        &format,
                    )?,
                    _ => {
                        return exec_err!(
                            "spark_date_format: expected date or timestamp array, got {:?}",
                            array.data_type()
                        );
                    }
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (ColumnarValue::Array(timestamp_array), ColumnarValue::Array(format_array)) => {
                // Convert string arrays to timestamp arrays if needed
                let (timestamp_array, tz): (
                    Arc<dyn datafusion::arrow::array::Array>,
                    Option<Arc<str>>,
                ) = match timestamp_array.clone().data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        let ts_array: Arc<dyn datafusion::arrow::array::Array> =
                            Arc::new(parse_string_array_to_timestamp(
                                &timestamp_array,
                                &self.session_timezone,
                            )?);
                        (ts_array, Some(self.session_timezone.clone()))
                    }
                    DataType::Timestamp(_, Some(_)) => {
                        (timestamp_array, Some(self.session_timezone.clone()))
                    }
                    DataType::Timestamp(_, None) => (timestamp_array, None),
                    _ => (timestamp_array, None),
                };
                format_timestamp_array_dynamic(&timestamp_array, &format_array, tz.as_ref())
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Scalar(format_scalar)) => {
                let format_str = match format_scalar.try_as_str().flatten() {
                    Some(s) => s,
                    None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                let format = DateTimeFormat::for_formatting(format_str)?;

                let result = match scalar {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => {
                        let micros = parse_timestamp_string(&value, &self.session_timezone)?;
                        match micros {
                            Some(micros) => format_timestamp_value(
                                micros,
                                &TimeUnit::Microsecond,
                                Some(&self.session_timezone),
                                &format,
                            )?,
                            None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                        }
                    }
                    ScalarValue::TimestampMicrosecond(Some(value), tz) => format_timestamp_value(
                        value,
                        &TimeUnit::Microsecond,
                        tz.as_ref().map(|_| &self.session_timezone),
                        &format,
                    )?,
                    ScalarValue::TimestampMillisecond(Some(value), tz) => format_timestamp_value(
                        value,
                        &TimeUnit::Millisecond,
                        tz.as_ref().map(|_| &self.session_timezone),
                        &format,
                    )?,
                    ScalarValue::TimestampSecond(Some(value), tz) => format_timestamp_value(
                        value,
                        &TimeUnit::Second,
                        tz.as_ref().map(|_| &self.session_timezone),
                        &format,
                    )?,
                    ScalarValue::TimestampNanosecond(Some(value), tz) => format_timestamp_value(
                        value,
                        &TimeUnit::Nanosecond,
                        tz.as_ref().map(|_| &self.session_timezone),
                        &format,
                    )?,
                    ScalarValue::Date32(Some(value)) => format_date32_value(value, &format)?,
                    ScalarValue::Date64(Some(value)) => format_date64_value(value, &format)?,
                    ScalarValue::Utf8(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::TimestampMicrosecond(None, _)
                    | ScalarValue::TimestampMillisecond(None, _)
                    | ScalarValue::TimestampSecond(None, _)
                    | ScalarValue::TimestampNanosecond(None, _)
                    | ScalarValue::Date32(None)
                    | ScalarValue::Date64(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return exec_err!(
                            "spark_date_format: expected timestamp scalar, got {:?}",
                            scalar
                        );
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(format_array)) => {
                // Convert scalar string to timestamp scalar if needed
                let scalar = match scalar {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => {
                        let micros = parse_timestamp_string(&value, &self.session_timezone)?;
                        match micros {
                            Some(micros) => ScalarValue::TimestampMicrosecond(
                                Some(micros),
                                Some(Arc::from("UTC")),
                            ),
                            None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                        }
                    }
                    _ => scalar,
                };
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar.clone()),
                    ColumnarValue::Array(format_array),
                ])?;
                let timestamp_array = arrays[0].clone();
                let format_array = arrays[1].clone();
                // LTZ metadata marks the value as an instant; the Spark session
                // zone, rather than the canonical Arrow metadata, controls display.
                let tz = match scalar {
                    ScalarValue::TimestampMicrosecond(_, Some(_))
                    | ScalarValue::TimestampMillisecond(_, Some(_))
                    | ScalarValue::TimestampSecond(_, Some(_))
                    | ScalarValue::TimestampNanosecond(_, Some(_)) => {
                        Some(self.session_timezone.clone())
                    }
                    ScalarValue::TimestampMicrosecond(_, None)
                    | ScalarValue::TimestampMillisecond(_, None)
                    | ScalarValue::TimestampSecond(_, None)
                    | ScalarValue::TimestampNanosecond(_, None) => None,
                    _ => Some(self.session_timezone.clone()),
                };
                format_timestamp_array_dynamic(&timestamp_array, &format_array, tz.as_ref())
            }
        }
    }
}

fn null_string_array(len: usize) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(StringArray::from(vec![
        Option::<String>::None;
        len
    ])))
}

/// Parse a timestamp string to microseconds since epoch.
fn parse_timestamp_string(value: &str, timezone: &str) -> Result<Option<i64>> {
    let Ok(timezone) = parse_spark_timezone(timezone) else {
        return Ok(None);
    };
    parse_timestamp_string_in_zone(value, &timezone)
}

fn parse_timestamp_string_in_zone(
    value: &str,
    default_timezone: &SparkTimeZone,
) -> Result<Option<i64>> {
    let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
    let (datetime, parsed_timezone) = match parsed {
        Ok(v) => v,
        Err(_e) => return Ok(None),
    };
    // Use the timezone from the parsed string if present, otherwise use the provided timezone
    let timezone = if parsed_timezone.is_empty() {
        *default_timezone
    } else {
        match parse_spark_timezone(parsed_timezone) {
            Ok(v) => v,
            Err(_e) => return Ok(None),
        }
    };
    let datetime = match localize_with_fallback(&timezone, &datetime) {
        Ok(v) => v,
        Err(_e) => return Ok(None),
    };
    Ok(Some(datetime.timestamp_micros()))
}

/// Parse a string array to a TimestampMicrosecondArray.
fn parse_string_array_to_timestamp(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    timezone: &str,
) -> Result<TimestampMicrosecondArray> {
    let mut builder = TimestampMicrosecondArray::builder(array.len());
    let Ok(timezone) = parse_spark_timezone(timezone) else {
        builder.append_nulls(array.len());
        return Ok(builder.finish());
    };

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = as_string_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_timestamp_string_in_zone(v, &timezone)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        DataType::LargeUtf8 => {
            let string_array = as_large_string_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_timestamp_string_in_zone(v, &timezone)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        DataType::Utf8View => {
            let string_array = as_string_view_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_timestamp_string_in_zone(v, &timezone)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        _ => {
            return exec_err!(
                "parse_string_array_to_timestamp: expected string array, got {:?}",
                array.data_type()
            );
        }
    }

    Ok(builder.finish())
}

/// Format a string array by first parsing strings as timestamps, then formatting.
fn format_string_array_as_timestamp(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    timezone: &str,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let timestamp_array = parse_string_array_to_timestamp(array, timezone)?;
    let tz: Arc<str> = Arc::from(timezone);
    let timestamp_array: Arc<dyn datafusion::arrow::array::Array> = Arc::new(timestamp_array);
    format_timestamp_array_by_unit(&timestamp_array, TimeUnit::Microsecond, Some(&tz), format)
}

fn format_timestamp_array_dynamic(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
) -> Result<ColumnarValue> {
    if timestamp_array.len() != format_array.len() {
        return exec_err!(
            "spark_date_format: timestamp and format arrays must have the same length"
        );
    }
    let mut cache = HashMap::<String, DateTimeFormat>::new();
    let result = match format_array.data_type() {
        DataType::Utf8 => {
            let formats = as_string_array(format_array)?;
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), tz, &mut cache)?
        }
        DataType::LargeUtf8 => {
            let formats = as_large_string_array(format_array)?;
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), tz, &mut cache)?
        }
        DataType::Utf8View => {
            let formats = as_string_view_array(format_array)?;
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), tz, &mut cache)?
        }
        _ => return exec_err!("spark_date_format: expected string array for format argument"),
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn format_timestamp_array_with_formats<'f>(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
    tz: Option<&Arc<str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
) -> Result<StringArray> {
    match timestamp_array.data_type() {
        DataType::Date32 => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("spark_date_format: failed to downcast to Date32Array")
                })?;
            format_date32_values(values.iter(), formats, cache)
        }
        DataType::Date64 => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("spark_date_format: failed to downcast to Date64Array")
                })?;
            format_date64_values(values.iter(), formats, cache)
        }
        DataType::Timestamp(TimeUnit::Microsecond, array_tz) => {
            format_timestamp_values_for_type::<TimestampMicrosecondType>(
                timestamp_array,
                formats,
                TimeUnit::Microsecond,
                tz.or(array_tz.as_ref()),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, array_tz) => {
            format_timestamp_values_for_type::<TimestampMillisecondType>(
                timestamp_array,
                formats,
                TimeUnit::Millisecond,
                tz.or(array_tz.as_ref()),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Second, array_tz) => {
            format_timestamp_values_for_type::<TimestampSecondType>(
                timestamp_array,
                formats,
                TimeUnit::Second,
                tz.or(array_tz.as_ref()),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, array_tz) => {
            format_timestamp_values_for_type::<TimestampNanosecondType>(
                timestamp_array,
                formats,
                TimeUnit::Nanosecond,
                tz.or(array_tz.as_ref()),
                cache,
            )
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // Handle string arrays by parsing them as timestamps
            let tz_str = tz.map(|s| s.as_ref()).unwrap_or_else(|| {
                // This shouldn't happen in practice since we always pass a timezone
                // from the caller, but we provide a fallback
                "UTC"
            });
            let timestamp_array = parse_string_array_to_timestamp(timestamp_array, tz_str)?;
            let timestamp_array: Arc<dyn datafusion::arrow::array::Array> =
                Arc::new(timestamp_array);
            // Recursively call with the parsed timestamp array
            format_timestamp_array_with_formats(&timestamp_array, formats, tz, cache)
        }
        _ => exec_err!(
            "spark_date_format: expected date or timestamp array, got {:?}",
            timestamp_array.data_type()
        ),
    }
}

fn format_date32_values<'v, 'f>(
    values: impl Iterator<Item = Option<i32>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
) -> Result<StringArray> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = cached_format(cache, format, DateTimeFormat::for_formatting)?;
                format_date32_value(value, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn format_date64_values<'v, 'f>(
    values: impl Iterator<Item = Option<i64>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
) -> Result<StringArray> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = cached_format(cache, format, DateTimeFormat::for_formatting)?;
                format_date64_value(value, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn format_timestamp_values<'v, 'f>(
    values: impl Iterator<Item = Option<i64>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    time_unit: &TimeUnit,
    tz: Option<&Arc<str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
) -> Result<StringArray> {
    let timezone = parse_format_timezone(tz)?;
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = cached_format(cache, format, DateTimeFormat::for_formatting)?;
                format_timestamp_value_in_zone(value, time_unit, timezone, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn format_timestamp_values_for_type<'f, T: ArrowTimestampType>(
    array: &ArrayRef,
    formats: impl Iterator<Item = Option<&'f str>>,
    time_unit: TimeUnit,
    tz: Option<&Arc<str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
) -> Result<StringArray> {
    let values = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| exec_datafusion_err!("spark_date_format: invalid timestamp array"))?;
    format_timestamp_values(values.iter(), formats, &time_unit, tz, cache)
}

#[derive(Clone, Copy)]
struct FormatTimeZone<'a> {
    id: &'a str,
    timezone: SparkTimeZone,
}

fn parse_format_timezone(tz: Option<&Arc<str>>) -> Result<Option<FormatTimeZone<'_>>> {
    tz.map(|id| {
        Ok(FormatTimeZone {
            id,
            timezone: parse_spark_timezone(id)?,
        })
    })
    .transpose()
}

fn format_timestamp_value(
    value: i64,
    time_unit: &TimeUnit,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<String> {
    format_timestamp_value_in_zone(value, time_unit, parse_format_timezone(tz)?, format)
}

fn format_timestamp_value_in_zone(
    value: i64,
    time_unit: &TimeUnit,
    timezone: Option<FormatTimeZone<'_>>,
    format: &DateTimeFormat,
) -> Result<String> {
    let naive_datetime = match time_unit {
        TimeUnit::Microsecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampMicrosecondType>(value)
        }
        TimeUnit::Millisecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampMillisecondType>(value)
        }
        TimeUnit::Second => as_datetime::<datafusion::arrow::datatypes::TimestampSecondType>(value),
        TimeUnit::Nanosecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampNanosecondType>(value)
        }
    }
    .ok_or_else(|| {
        exec_datafusion_err!(
            "spark_date_format: cannot convert timestamp value {} to datetime",
            value
        )
    })?;

    match timezone {
        Some(timezone) => {
            let datetime = timezone.timezone.from_utc_datetime(&naive_datetime);
            format.format(DateTimeFormatInput {
                datetime: datetime.naive_local(),
                timezone: Some(TimeZoneDisplay {
                    offset: datetime.offset().fix(),
                    name: Some(timezone.id),
                }),
                zone_id: Some(timezone.id),
                timestamp_kind: TimestampKind::Normal,
                precision: TimePrecision::Microsecond,
            })
        }
        None => format.format(DateTimeFormatInput {
            datetime: naive_datetime,
            timezone: None,
            zone_id: None,
            timestamp_kind: TimestampKind::Normal,
            precision: TimePrecision::Microsecond,
        }),
    }
}

pub(crate) fn format_file_timestamp_array(
    array: &ArrayRef,
    session_timezone: &str,
    pattern: &str,
) -> Result<ArrayRef> {
    let format = DateTimeFormat::for_formatting(pattern)?;
    let timezone = Arc::<str>::from(session_timezone);
    let output = match array.data_type() {
        DataType::Timestamp(unit, Some(_)) => {
            format_timestamp_array_by_unit(array, *unit, Some(&timezone), &format)?
        }
        data_type => {
            return exec_err!(
                "file timestamp formatter expected a zoned timestamp, got {data_type}"
            );
        }
    };
    Ok(Arc::new(output))
}

fn format_date32_value(value: i32, format: &DateTimeFormat) -> Result<String> {
    let datetime = date32_to_datetime(value).ok_or_else(|| {
        exec_datafusion_err!("spark_date_format: cannot convert date32 value {value} to datetime")
    })?;
    format.format(DateTimeFormatInput {
        datetime,
        timezone: None,
        zone_id: None,
        timestamp_kind: TimestampKind::Normal,
        precision: TimePrecision::Microsecond,
    })
}

fn format_date64_value(value: i64, format: &DateTimeFormat) -> Result<String> {
    let datetime = date64_to_datetime(value).ok_or_else(|| {
        exec_datafusion_err!("spark_date_format: cannot convert date64 value {value} to datetime")
    })?;
    format.format(DateTimeFormatInput {
        datetime,
        timezone: None,
        zone_id: None,
        timestamp_kind: TimestampKind::Normal,
        precision: TimePrecision::Microsecond,
    })
}

fn format_date32_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to Date32Array")
        })?;

    let result: Vec<Option<String>> = date_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_date32_value(value, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_date64_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to Date64Array")
        })?;

    let result: Vec<Option<String>> = date_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_date64_value(value, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array<T: ArrowTimestampType>(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    time_unit: TimeUnit,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| exec_datafusion_err!("spark_date_format: invalid timestamp array"))?;
    let timezone = parse_format_timezone(tz)?;
    ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value_in_zone(value, &time_unit, timezone, format))
                .transpose()
        })
        .collect()
}

fn format_timestamp_array_by_unit(
    array: &ArrayRef,
    time_unit: TimeUnit,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    match time_unit {
        TimeUnit::Second => {
            format_timestamp_array::<TimestampSecondType>(array, time_unit, tz, format)
        }
        TimeUnit::Millisecond => {
            format_timestamp_array::<TimestampMillisecondType>(array, time_unit, tz, format)
        }
        TimeUnit::Microsecond => {
            format_timestamp_array::<TimestampMicrosecondType>(array, time_unit, tz, format)
        }
        TimeUnit::Nanosecond => {
            format_timestamp_array::<TimestampNanosecondType>(array, time_unit, tz, format)
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_dynamic_format_uses_session_timezone_for_scalar_ltz() -> Result<()> {
        let udf = SparkDateFormat::new(Arc::from("+01:02:03"));
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(0),
                    Some(Arc::from("UTC")),
                )),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    "yyyy-MM-dd HH:mm:ss",
                    "HH:mm:ss",
                ]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("timestamp", timestamp_type, false)),
                Arc::new(Field::new("format", DataType::Utf8, false)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let result = result.into_array(2)?;
        let result = as_string_array(&result)?;

        assert_eq!(result.value(0), "1970-01-01 01:02:03");
        assert_eq!(result.value(1), "01:02:03");
        Ok(())
    }
}
