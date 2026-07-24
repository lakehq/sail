use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::scalar::datetime::format::DateTimeFormat;

/// Truncates a DateTime's nanoseconds to microseconds.
/// This preserves fractional seconds when converting from nanosecond precision to microsecond precision.
fn truncate_datetime_to_microseconds(datetime: &chrono::DateTime<chrono::Utc>) -> i64 {
    use chrono::Timelike;

    let timestamp_secs = datetime.timestamp();
    let nanos = datetime.nanosecond();

    // Convert nanoseconds to microseconds by truncation
    // 1 microsecond = 1000 nanoseconds
    let micros_from_nanos = nanos as i64 / 1000;

    // Combine seconds and microseconds
    timestamp_secs * 1_000_000 + micros_from_nanos
}

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}

enum ScalarFormat {
    Omitted,
    Null,
    Format(DateTimeFormat),
}

impl TimestampParser {
    /// Localize a naive datetime (with an optional timezone parsed from the input
    /// string) into a microsecond instant, honoring the LTZ/NTZ semantics.
    fn localize(&self, datetime: NaiveDateTime, timezone: &str, safe: bool) -> Result<Option<i64>> {
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let tz: Tz = if timezone.is_empty() {
                    match default_timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if safe => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if safe => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                };
                match localize_with_fallback(&tz, &datetime) {
                    Ok(v) => Ok(Some(v.timestamp_micros())),
                    Err(_e) if safe => Ok(None),
                    Err(e) => Err(e),
                }
            }
            // NTZ ignores any timezone in the input and keeps the wall clock.
            TimestampParser::Ntz => Ok(Some(datetime.and_utc().timestamp_micros())),
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
            Err(e) if is_invalid_leap_second(&e) => return Ok(None),
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
                    let timezone_name = parsed.timezone.as_deref().unwrap_or(default_timezone);
                    let timezone: Tz = match timezone_name.parse() {
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
                // Truncate nanoseconds to microseconds to preserve fractional seconds
                let micros = truncate_datetime_to_microseconds(&datetime);
                Ok(Some(micros))
            }
            TimestampParser::Ntz => {
                let datetime = if let Some(offset) = parsed.offset {
                    parsed
                        .datetime
                        .and_local_timezone(offset)
                        .single()
                        .map(|x| x.to_utc())
                        .ok_or_else(|| exec_datafusion_err!("cannot apply parsed offset"))?
                } else {
                    parsed.datetime.and_utc()
                };
                // Truncate nanoseconds to microseconds to preserve fractional seconds
                let micros = truncate_datetime_to_microseconds(&datetime);
                Ok(Some(micros))
            }
        }
    }

    fn string_to_microseconds(&self, value: &str, safe: bool) -> Result<Option<i64>> {
        let (datetime, timezone) = match parse_timestamp(value).and_then(|x| x.into_naive()) {
            Ok(v) => v,
            Err(_e) if safe => return Ok(None),
            Err(e) => return Err(exec_datafusion_err!("{e}")),
        };
        self.localize(datetime, timezone, safe)
    }

    fn string_to_microseconds_with_format(
        &self,
        value: &str,
        format: &str,
        safe: bool,
    ) -> Result<Option<i64>> {
        // `format` is already a chrono format (the planner runs `to_chrono_fmt`).
        let datetime = match NaiveDateTime::parse_from_str(value, format) {
            Ok(v) => v,
            // A date-only format (e.g. `yyyy-MM-dd`) can't parse as a datetime;
            // fall back to a date and default the time to midnight, matching Spark.
            Err(_) => match NaiveDate::parse_from_str(value, format) {
                Ok(d) => match d.and_hms_opt(0, 0, 0) {
                    Some(v) => v,
                    None => return exec_err!("invalid midnight for {value}"),
                },
                Err(_e) if safe => return Ok(None),
                Err(e) => return Err(exec_datafusion_err!("{e}")),
            },
        };
        self.localize(datetime, "", safe)
    }
}

fn is_invalid_leap_second(error: &datafusion_common::DataFusionError) -> bool {
    error
        .to_string()
        .contains("valid leap second must be 23:59:60")
}

/// Spark-compatible `to_timestamp` / `try_to_timestamp` (and their `_ntz`
/// counterparts / `CAST(str AS TIMESTAMP[_NTZ])`).
///
/// Honors `spark.sql.ansi.enabled` via two flags (same shape as the ANSI-aware
/// date/make_interval UDFs):
/// - `is_try` selects the safe variant (`try_to_timestamp`) and drives `name()`.
/// - `ansi_mode` is the session flag captured at planning.
///
/// A parse/cast failure returns NULL when `is_try || !ansi_mode` and errors
/// otherwise. The `timezone` field selects LTZ (Some, applies the input's
/// timezone or the session default) vs NTZ (None, keeps wall clock).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: TimestampParser,
    signature: Signature,
    ansi_mode: bool,
    is_try: bool,
}

impl SparkTimestamp {
    pub fn try_new(timezone: Option<Arc<str>>, ansi_mode: bool, is_try: bool) -> Result<Self> {
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
            ansi_mode,
            is_try,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    /// Whether a parse/cast failure yields NULL: `try_*` always, or the strict
    /// variant when ANSI is disabled.
    fn safe(&self) -> bool {
        self.is_try || !self.ansi_mode
    }

    fn string_array_iter(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
        match array.data_type() {
            DataType::Utf8 => Ok(Box::new(as_string_array(array)?.iter())),
            DataType::LargeUtf8 => Ok(Box::new(as_large_string_array(array)?.iter())),
            DataType::Utf8View => Ok(Box::new(as_string_view_array(array)?.iter())),
            other => exec_err!("expected string array, got {other}"),
        }
    }

    fn kernel(
        parser: &TimestampParser,
        safe: bool,
        args: &[ArrayRef],
    ) -> Result<TimestampMicrosecondArray> {
        let value_arr = &args[0];
        match args.get(1) {
            Some(format_arr) => {
                if value_arr.len() != format_arr.len() {
                    return exec_err!("value/format array length mismatch");
                }
                let values = Self::string_array_iter(value_arr)?;
                let formats = Self::string_array_iter(format_arr)?;
                values
                    .zip(formats)
                    .map(|(v, f)| match (v, f) {
                        (Some(s), Some(fmt)) => {
                            parser.string_to_microseconds_with_format(s, fmt, safe)
                        }
                        _ => Ok(None),
                    })
                    .collect::<Result<_>>()
            }
            None => Self::string_array_iter(value_arr)?
                .map(|v| match v {
                    Some(s) => parser.string_to_microseconds(s, safe),
                    None => Ok(None),
                })
                .collect::<Result<_>>(),
        }
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn name(&self) -> &str {
        match (&self.parser, self.is_try) {
            (TimestampParser::Ltz { .. }, false) => "to_timestamp",
            (TimestampParser::Ltz { .. }, true) => "try_to_timestamp",
            (TimestampParser::Ntz, false) => "to_timestamp_ntz",
            (TimestampParser::Ntz, true) => "try_to_timestamp_ntz",
        }
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 1 | 2) {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 2),
                arg_types.len(),
            ));
        }
        match &arg_types[0] {
            // String-only, matching the kernel (which parses strings) and the
            // sibling parsers `SparkDate`/`SparkTime`. The planner casts/handles
            // DATE/TIMESTAMP inputs directly, so they never reach this UDF.
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
            other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "STRING or NULL",
                    other,
                ));
            }
        }
        let mut coerced = arg_types.to_vec();
        if let Some(format) = arg_types.get(1) {
            match format {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                // A NULL format yields a NULL result; coerce it to a Utf8 null.
                DataType::Null => coerced[1] = DataType::Utf8,
                other => {
                    return Err(unsupported_data_type_exec_err(self.name(), "STRING", other));
                }
            }
        }
        Ok(coerced)
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
                let format = parse_scalar_format(format)?;
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, is_try)
                                }
                                ScalarFormat::Null => Ok(None),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, is_try)
                                }
                                ScalarFormat::Null => Ok(None),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, is_try),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, is_try)
                                }
                                ScalarFormat::Null => Ok(None),
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
                let format = parse_scalar_format(format)?;
                if matches!(format, ScalarFormat::Null) {
                    return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        None,
                        self.timezone.clone(),
                    )));
                }

                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| match &format {
                            ScalarFormat::Format(format) => self
                                .parser
                                .formatted_string_to_microseconds(v, format, is_try),
                            ScalarFormat::Omitted => self.parser.string_to_microseconds(v, is_try),
                            ScalarFormat::Null => unreachable!(),
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

fn parse_scalar_format(format: Option<ColumnarValue>) -> Result<ScalarFormat> {
    match format {
        Some(ColumnarValue::Scalar(scalar)) => match scalar.try_as_str() {
            Some(Some(format)) => Ok(ScalarFormat::Format(DateTimeFormat::parse(format)?)),
            Some(None) => Ok(ScalarFormat::Null),
            None => exec_err!("spark_timestamp format argument must be a string scalar"),
        },
        Some(ColumnarValue::Array(_)) => unreachable!(),
        None => Ok(ScalarFormat::Omitted),
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
    match cache.entry(pattern.to_string()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(DateTimeFormat::parse(pattern)?)),
    }
}
