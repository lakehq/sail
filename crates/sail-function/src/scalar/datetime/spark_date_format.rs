use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Offset, TimeZone};
use datafusion::arrow::array::{
    Array, Date32Array, Date64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::temporal_conversions::{
    as_datetime, date32_to_datetime, date64_to_datetime,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::format::{
    DateTimeFormat, DateTimeFormatInput, TimePrecision, TimeZoneDisplay, TimestampKind,
};

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

                let format = DateTimeFormat::parse(format_str)?;

                let result: StringArray = match array.data_type() {
                    DataType::Date32 => format_date32_array(&array, &format)?,
                    DataType::Date64 => format_date64_array(&array, &format)?,
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        format_timestamp_array_microsecond(&array, tz.as_ref(), &format)?
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        format_timestamp_array_millisecond(&array, tz.as_ref(), &format)?
                    }
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        format_timestamp_array_second(&array, tz.as_ref(), &format)?
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        format_timestamp_array_nanosecond(&array, tz.as_ref(), &format)?
                    }
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
                format_timestamp_array_dynamic(&timestamp_array, &format_array)
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Scalar(format_scalar)) => {
                let format_str = match format_scalar.try_as_str().flatten() {
                    Some(s) => s,
                    None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                let format = DateTimeFormat::parse(format_str)?;

                let result = match scalar {
                    ScalarValue::TimestampMicrosecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Microsecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampMillisecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Millisecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampSecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Second, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampNanosecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Nanosecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::Date32(Some(value)) => format_date32_value(value, &format)?,
                    ScalarValue::Date64(Some(value)) => format_date64_value(value, &format)?,
                    ScalarValue::TimestampMicrosecond(None, _)
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
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar),
                    ColumnarValue::Array(format_array),
                ])?;
                let timestamp_array = arrays[0].clone();
                let format_array = arrays[1].clone();
                format_timestamp_array_dynamic(&timestamp_array, &format_array)
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

fn format_timestamp_array_dynamic(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
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
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), &mut cache)?
        }
        DataType::LargeUtf8 => {
            let formats = as_large_string_array(format_array)?;
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), &mut cache)?
        }
        DataType::Utf8View => {
            let formats = as_string_view_array(format_array)?;
            format_timestamp_array_with_formats(timestamp_array, formats.iter(), &mut cache)?
        }
        _ => return exec_err!("spark_date_format: expected string array for format argument"),
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn format_timestamp_array_with_formats<'f>(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
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
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampMicrosecondArray"
                    )
                })?;
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Microsecond,
                tz.as_ref(),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampMillisecondArray"
                    )
                })?;
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Millisecond,
                tz.as_ref(),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampSecondArray"
                    )
                })?;
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Second,
                tz.as_ref(),
                cache,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampNanosecondArray"
                    )
                })?;
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Nanosecond,
                tz.as_ref(),
                cache,
            )
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
                let format = get_or_parse_format(cache, format)?;
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
                let format = get_or_parse_format(cache, format)?;
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
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format)?;
                format_timestamp_value(value, time_unit, tz, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
) -> Result<&'a DateTimeFormat> {
    let cache_key = pattern.to_string();
    if !cache.contains_key(&cache_key) {
        cache.insert(cache_key.clone(), DateTimeFormat::parse(pattern)?);
    }
    Ok(cache.get(&cache_key).expect("datetime format was inserted"))
}

fn format_timestamp_value(
    value: i64,
    time_unit: &TimeUnit,
    tz: Option<&Arc<str>>,
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

    match tz {
        Some(tz_str) => {
            let tz = tz_str
                .parse::<chrono_tz::Tz>()
                .map_err(|e| exec_datafusion_err!("Invalid timezone '{}': {}", tz_str, e))?;
            let datetime = tz.from_utc_datetime(&naive_datetime);
            format.format(DateTimeFormatInput {
                datetime: datetime.naive_local(),
                timezone: Some(TimeZoneDisplay {
                    offset: datetime.offset().fix(),
                    name: Some(tz_str),
                }),
                zone_id: Some(tz_str),
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

fn format_timestamp_array_microsecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampMicrosecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Microsecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_millisecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampMillisecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Millisecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_second(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to TimestampSecondArray")
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Second, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_nanosecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampNanosecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Nanosecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}
