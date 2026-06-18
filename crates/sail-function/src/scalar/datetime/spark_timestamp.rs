use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::scalar::datetime::format::DateTimeFormat;

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}

impl TimestampParser {
    fn string_to_microseconds(&self, value: &str, is_try: bool) -> Result<Option<i64>> {
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let (datetime, timezone) = match parsed {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(exec_datafusion_err!("{e}")),
                };
                let timezone: Tz = if timezone.is_empty() {
                    match default_timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                };
                let datetime = match localize_with_fallback(&timezone, &datetime) {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(e),
                };
                Ok(Some(datetime.timestamp_micros()))
            }
            TimestampParser::Ntz => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let (datetime, _timezone) = match parsed {
                    Ok(v) => v,
                    Err(_e) if is_try => return Ok(None),
                    Err(e) => return Err(exec_datafusion_err!("{e}")),
                };
                Ok(Some(datetime.and_utc().timestamp_micros()))
            }
        }
    }

    fn formatted_string_to_microseconds(
        &self,
        value: &str,
        format: &DateTimeFormat,
        is_try: bool,
    ) -> Result<Option<i64>> {
        let parsed = match format.parse_datetime_value(value) {
            Ok(v) => v,
            Err(_e) if is_try => return Ok(None),
            Err(e) => return Err(e),
        };
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let datetime = if let Some(offset) = parsed.offset {
                    parsed
                        .datetime
                        .and_local_timezone(offset)
                        .single()
                        .map(|x| x.to_utc())
                        .ok_or_else(|| exec_datafusion_err!("cannot apply parsed offset"))?
                } else {
                    let timezone: Tz = match default_timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    };
                    match localize_with_fallback(&timezone, &parsed.datetime) {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e),
                    }
                };
                Ok(Some(datetime.timestamp_micros()))
            }
            TimestampParser::Ntz => Ok(Some(parsed.datetime.and_utc().timestamp_micros())),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: TimestampParser,
    signature: Signature,
    is_try: bool,
}

impl SparkTimestamp {
    /// Creates a SparkTimestamp.
    ///
    /// When `is_try` is true, returns NULL on invalid input (for try_cast).
    /// When `is_try` is false, throws an error on invalid input (for cast).
    pub fn try_new(timezone: Option<Arc<str>>, is_try: bool) -> Result<Self> {
        let parser = if let Some(ref timezone) = timezone {
            TimestampParser::Ltz {
                default_timezone: timezone.as_ref().to_string(),
            }
        } else {
            TimestampParser::Ntz
        };
        Ok(Self {
            timezone,
            parser,
            signature: Signature::variadic_any(Volatility::Immutable),
            is_try,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn name(&self) -> &str {
        "spark_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            self.timezone.clone(),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (arg, format) = match args.len() {
            1 => (args.one()?, None),
            2 => {
                let (arg, format) = args.two()?;
                (arg, Some(format))
            }
            _ => return exec_err!("spark_timestamp requires 1 or 2 arguments"),
        };
        let is_try = self.is_try;
        match (arg, format) {
            (ColumnarValue::Array(array), Some(ColumnarValue::Array(format_array))) => {
                self.parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Scalar(scalar), Some(ColumnarValue::Array(format_array))) => {
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar),
                    ColumnarValue::Array(format_array),
                ])?;
                let array = arrays[0].clone();
                let format_array = arrays[1].clone();
                self.parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Array(array), format) => {
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => scalar
                        .try_as_str()
                        .flatten()
                        .map(DateTimeFormat::parse)
                        .transpose()?,
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                None => self.parser.string_to_microseconds(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                None => self.parser.string_to_microseconds(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                None => self.parser.string_to_microseconds(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                let array = array.with_timezone_opt(self.timezone.clone());
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ColumnarValue::Scalar(scalar), format) => {
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => scalar
                        .try_as_str()
                        .flatten()
                        .map(DateTimeFormat::parse)
                        .transpose()?,
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| match &format {
                            Some(format) => self
                                .parser
                                .formatted_string_to_microseconds(v, format, is_try),
                            None => self.parser.string_to_microseconds(v, is_try),
                        })
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!("expected string scalar for `timestamp`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    value,
                    self.timezone.clone(),
                )))
            }
        }
    }
}

impl SparkTimestamp {
    fn parse_array_with_format_array(
        &self,
        array: &Arc<dyn datafusion::arrow::array::Array>,
        format_array: &Arc<dyn datafusion::arrow::array::Array>,
        is_try: bool,
    ) -> Result<ColumnarValue> {
        if array.len() != format_array.len() {
            return exec_err!("spark_timestamp value and format arrays must have the same length");
        }
        let mut cache = HashMap::<String, DateTimeFormat>::new();
        let array = match format_array.data_type() {
            DataType::Utf8 => {
                let formats = as_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
            }
            DataType::LargeUtf8 => {
                let formats = as_large_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
            }
            DataType::Utf8View => {
                let formats = as_string_view_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
            }
            _ => return exec_err!("spark_timestamp format argument must be a string array"),
        };
        Ok(ColumnarValue::Array(Arc::new(
            array.with_timezone_opt(self.timezone.clone()),
        )))
    }

    fn parse_array_with_formats<'f>(
        &self,
        array: &Arc<dyn datafusion::arrow::array::Array>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        is_try: bool,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        match array.data_type() {
            DataType::Utf8 => self.parse_values_with_formats(
                as_string_array(array)?.iter(),
                formats,
                cache,
                is_try,
            ),
            DataType::LargeUtf8 => self.parse_values_with_formats(
                as_large_string_array(array)?.iter(),
                formats,
                cache,
                is_try,
            ),
            DataType::Utf8View => self.parse_values_with_formats(
                as_string_view_array(array)?.iter(),
                formats,
                cache,
                is_try,
            ),
            _ => exec_err!("expected string array for `timestamp`"),
        }
    }

    fn parse_values_with_formats<'v, 'f>(
        &self,
        values: impl Iterator<Item = Option<&'v str>>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        is_try: bool,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        values
            .zip(formats)
            .map(|(value, format)| match (value, format) {
                (Some(value), Some(format)) => {
                    let format = get_or_parse_format(cache, format)?;
                    self.parser
                        .formatted_string_to_microseconds(value, format, is_try)
                }
                _ => Ok(None),
            })
            .collect::<Result<_>>()
    }
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
) -> Result<&'a DateTimeFormat> {
    if !cache.contains_key(pattern) {
        cache.insert(pattern.to_string(), DateTimeFormat::parse(pattern)?);
    }
    Ok(cache.get(pattern).expect("datetime format was inserted"))
}
