use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, PrimitiveArray, new_null_array};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, FieldRef, Int64Type, TimeUnit};
use datafusion_common::cast::{
    as_date32_array, as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::datetime::format::DateTimeFormat;

const DEFAULT_PATTERN: &str = "yyyy-MM-dd HH:mm:ss";

enum ScalarUnixFormat {
    Null,
    Format(DateTimeFormat),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixTimestamp {
    signature: Signature,
    session_timezone: Arc<str>,
    ansi_mode: bool,
}

impl SparkUnixTimestamp {
    pub fn new(session_timezone: Arc<str>, ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            session_timezone,
            ansi_mode,
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkUnixTimestamp {
    fn name(&self) -> &str {
        "spark_unix_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 1 | 2) {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 2),
                arg_types.len(),
            ));
        }
        match &arg_types[0] {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Null => {}
            other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "STRING, DATE, TIMESTAMP or NULL",
                    other,
                ));
            }
        }

        let mut coerced = arg_types.to_vec();
        if let Some(format) = arg_types.get(1) {
            match format {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                DataType::Null => coerced[1] = DataType::Utf8,
                other => {
                    return Err(unsupported_data_type_exec_err(self.name(), "STRING", other));
                }
            }
        }
        Ok(coerced)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = !self.ansi_mode || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [first, ..] = args.args.as_slice() else {
            return exec_err!("spark_unix_timestamp function requires 1 or more arguments");
        };
        let format = match args.args.len() {
            1 => None,
            2 => Some(&args.args[1]),
            _ => return exec_err!("spark_unix_timestamp function requires 1 or 2 arguments"),
        };
        let safe = !self.ansi_mode;
        match first.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => match format {
                Some(format) => self.invoke_with_format(first, format, safe),
                None => self.invoke_with_scalar_format(
                    first,
                    ScalarUnixFormat::Format(DateTimeFormat::for_parsing(DEFAULT_PATTERN)?),
                    safe,
                ),
            },
            DataType::Date64 | DataType::Date32 => self.invoke_with_date(first),
            DataType::Timestamp(_, timezone) => first
                .cast_to(&DataType::Timestamp(TimeUnit::Second, timezone), None)?
                .cast_to(&DataType::Int64, None),
            DataType::Null => null_int64(first),
            other => {
                exec_err!("spark_unix_timestamp function unsupported data type: {other}")
            }
        }
    }
}

impl SparkUnixTimestamp {
    fn invoke_with_format(
        &self,
        first: &ColumnarValue,
        format: &ColumnarValue,
        safe: bool,
    ) -> Result<ColumnarValue> {
        match format {
            ColumnarValue::Scalar(format_scalar) => {
                self.invoke_with_scalar_format(first, parse_format_scalar(format_scalar)?, safe)
            }
            ColumnarValue::Array(format_array) => match first {
                ColumnarValue::Array(array) => {
                    self.invoke_array_with_format_array(array, format_array, safe)
                }
                ColumnarValue::Scalar(scalar) => {
                    let arrays = ColumnarValue::values_to_arrays(&[
                        ColumnarValue::Scalar(scalar.clone()),
                        ColumnarValue::Array(format_array.clone()),
                    ])?;
                    self.invoke_array_with_format_array(&arrays[0], &arrays[1], safe)
                }
            },
        }
    }

    fn invoke_with_scalar_format(
        &self,
        first: &ColumnarValue,
        format: ScalarUnixFormat,
        safe: bool,
    ) -> Result<ColumnarValue> {
        match first {
            ColumnarValue::Array(array) => {
                let ScalarUnixFormat::Format(format) = format else {
                    return Ok(ColumnarValue::Array(new_null_array(
                        &DataType::Int64,
                        array.len(),
                    )));
                };
                let array: PrimitiveArray<Int64Type> = match array.data_type() {
                    DataType::Utf8 => as_string_array(array)?
                        .iter()
                        .map(|value| {
                            value
                                .map(|value| self.formatted_string_to_seconds(value, &format, safe))
                                .transpose()
                                .map(Option::flatten)
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(array)?
                        .iter()
                        .map(|value| {
                            value
                                .map(|value| self.formatted_string_to_seconds(value, &format, safe))
                                .transpose()
                                .map(Option::flatten)
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(array)?
                        .iter()
                        .map(|value| {
                            value
                                .map(|value| self.formatted_string_to_seconds(value, &format, safe))
                                .transpose()
                                .map(Option::flatten)
                        })
                        .collect::<Result<_>>()?,
                    other => {
                        return exec_err!(
                            "spark_unix_timestamp function unsupported formatted input data type: {other}"
                        );
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let ScalarUnixFormat::Format(format) = format else {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
                };
                let value = match scalar.try_as_str() {
                    Some(value) => value
                        .map(|value| self.formatted_string_to_seconds(value, &format, safe))
                        .transpose()?
                        .flatten(),
                    None => {
                        return exec_err!(
                            "spark_unix_timestamp function expected string scalar for formatted input"
                        );
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(value)))
            }
        }
    }

    fn invoke_with_date(&self, first: &ColumnarValue) -> Result<ColumnarValue> {
        let timezone: Tz = self.session_timezone.parse()?;
        match first.cast_to(&DataType::Date32, None)? {
            ColumnarValue::Array(array) => {
                let dates = as_date32_array(&array)?;
                let seconds: PrimitiveArray<Int64Type> = dates
                    .iter()
                    .map(|days| {
                        days.map(|days| date32_to_seconds(days, &timezone))
                            .transpose()
                    })
                    .collect::<Result<_>>()?;
                Ok(ColumnarValue::Array(Arc::new(seconds)))
            }
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                let seconds = days
                    .map(|days| date32_to_seconds(days, &timezone))
                    .transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(seconds)))
            }
            other => exec_err!(
                "spark_unix_timestamp expected date input after coercion, got {}",
                other.data_type()
            ),
        }
    }

    fn invoke_array_with_format_array(
        &self,
        array: &dyn datafusion::arrow::array::Array,
        format_array: &dyn datafusion::arrow::array::Array,
        safe: bool,
    ) -> Result<ColumnarValue> {
        if array.len() != format_array.len() {
            return exec_err!(
                "spark_unix_timestamp value and format arrays must have the same length"
            );
        }
        let mut cache = HashMap::<String, DateTimeFormat>::new();
        let array = match format_array.data_type() {
            DataType::Utf8 => {
                let formats = as_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            DataType::LargeUtf8 => {
                let formats = as_large_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            DataType::Utf8View => {
                let formats = as_string_view_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            _ => return exec_err!("spark_unix_timestamp format argument must be a string array"),
        };
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn parse_array_with_formats<'f>(
        &self,
        array: &dyn datafusion::arrow::array::Array,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        safe: bool,
    ) -> Result<PrimitiveArray<Int64Type>> {
        match array.data_type() {
            DataType::Utf8 => {
                self.parse_values_with_formats(as_string_array(array)?.iter(), formats, cache, safe)
            }
            DataType::LargeUtf8 => self.parse_values_with_formats(
                as_large_string_array(array)?.iter(),
                formats,
                cache,
                safe,
            ),
            DataType::Utf8View => self.parse_values_with_formats(
                as_string_view_array(array)?.iter(),
                formats,
                cache,
                safe,
            ),
            other => exec_err!(
                "spark_unix_timestamp function unsupported formatted input data type: {other}"
            ),
        }
    }

    fn parse_values_with_formats<'v, 'f>(
        &self,
        values: impl Iterator<Item = Option<&'v str>>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        safe: bool,
    ) -> Result<PrimitiveArray<Int64Type>> {
        values
            .zip(formats)
            .map(|(value, format)| match (value, format) {
                (Some(value), Some(format)) => {
                    let format = get_or_parse_format(cache, format)?;
                    self.formatted_string_to_seconds(value, format, safe)
                }
                _ => Ok(None),
            })
            .collect::<Result<_>>()
    }

    fn formatted_string_to_seconds(
        &self,
        value: &str,
        format: &DateTimeFormat,
        safe: bool,
    ) -> Result<Option<i64>> {
        let Some(parsed) = timestamp_parse_result(format.parse_datetime_value(value), safe)? else {
            return Ok(None);
        };
        let timestamp = if let Some(offset) = parsed.offset {
            let localized = parsed
                .datetime
                .and_local_timezone(offset)
                .single()
                .ok_or_else(|| exec_datafusion_err!("cannot apply parsed offset"));
            let Some(localized) = timestamp_parse_result(localized, safe)? else {
                return Ok(None);
            };
            localized.to_utc().timestamp()
        } else {
            let timezone = parsed
                .timezone
                .as_deref()
                .unwrap_or(&self.session_timezone)
                .parse::<Tz>();
            let Some(timezone) = timestamp_parse_result(timezone, safe)? else {
                return Ok(None);
            };
            let Some(localized) =
                timestamp_parse_result(localize_with_fallback(&timezone, &parsed.datetime), safe)?
            else {
                return Ok(None);
            };
            localized.timestamp()
        };
        Ok(Some(timestamp))
    }
}

fn parse_format_scalar(scalar: &ScalarValue) -> Result<ScalarUnixFormat> {
    match scalar.try_as_str() {
        Some(Some(pattern)) => Ok(ScalarUnixFormat::Format(DateTimeFormat::for_parsing(
            pattern,
        )?)),
        Some(None) => Ok(ScalarUnixFormat::Null),
        None => exec_err!("spark_unix_timestamp format argument must be a string scalar"),
    }
}

fn null_int64(value: &ColumnarValue) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(new_null_array(
            &DataType::Int64,
            array.len(),
        ))),
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
    }
}

fn date32_to_seconds(days: i32, timezone: &Tz) -> Result<i64> {
    let date = Date32Type::to_naive_date_opt(days).ok_or_else(|| {
        exec_datafusion_err!("spark_unix_timestamp cannot convert date value {days}")
    })?;
    let datetime = date.and_hms_opt(0, 0, 0).ok_or_else(|| {
        exec_datafusion_err!("spark_unix_timestamp cannot construct midnight for {date}")
    })?;
    Ok(localize_with_fallback(timezone, &datetime)?.timestamp())
}

fn timestamp_parse_result<T, E>(result: std::result::Result<T, E>, safe: bool) -> Result<Option<T>>
where
    E: Display,
{
    match result {
        Ok(value) => Ok(Some(value)),
        Err(_) if safe => Ok(None),
        Err(error) => Err(timestamp_parse_error(error)),
    }
}

fn timestamp_parse_error(error: impl Display) -> DataFusionError {
    exec_datafusion_err!("Error parsing timestamp: [CANNOT_PARSE_TIMESTAMP] {error}")
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
) -> Result<&'a DateTimeFormat> {
    match cache.entry(pattern.to_string()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(DateTimeFormat::for_parsing(pattern)?)),
    }
}
