use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::parse_timestamp;

/// Parse a time-only string as a NaiveDateTime at epoch (1970-01-01).
///
/// Spark's `CAST('HH:MM:SS' AS TIMESTAMP)` accepts time-only strings by
/// composing with the epoch date. Sail's `parse_timestamp` doesn't handle
/// time-only input, so we fall back to this helper when it fails.
fn try_time_only_naive(value: &str) -> Option<NaiveDateTime> {
    const TIME_FORMATS: &[&str] = &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"];
    for fmt in TIME_FORMATS {
        if let Ok(t) = NaiveTime::parse_from_str(value, fmt) {
            return NaiveDate::from_ymd_opt(1970, 1, 1).map(|d| d.and_time(t));
        }
    }
    None
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}

impl TimestampParser {
    fn string_to_microseconds(&self, value: &str, safe: bool) -> Result<Option<i64>> {
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let (datetime, timezone): (NaiveDateTime, String) = match parsed {
                    Ok((dt, tz)) => (dt, tz.to_string()),
                    Err(e) => match try_time_only_naive(value) {
                        Some(dt) => (dt, String::new()),
                        None if safe => return Ok(None),
                        None => return Err(exec_datafusion_err!("{e}")),
                    },
                };
                let timezone: Tz = if timezone.is_empty() {
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
                let datetime = match localize_with_fallback(&timezone, &datetime) {
                    Ok(v) => v,
                    Err(_e) if safe => return Ok(None),
                    Err(e) => return Err(e),
                };
                Ok(Some(datetime.timestamp_micros()))
            }
            TimestampParser::Ntz => {
                let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
                let datetime: NaiveDateTime = match parsed {
                    Ok((dt, _tz)) => dt,
                    Err(e) => match try_time_only_naive(value) {
                        Some(dt) => dt,
                        None if safe => return Ok(None),
                        None => return Err(exec_datafusion_err!("{e}")),
                    },
                };
                Ok(Some(datetime.and_utc().timestamp_micros()))
            }
        }
    }

    fn string_to_microseconds_with_format(
        &self,
        value: &str,
        format: &str,
        safe: bool,
    ) -> Result<Option<i64>> {
        let parsed = NaiveDateTime::parse_from_str(value, format);
        let datetime = match parsed {
            Ok(v) => v,
            Err(_) if safe => return Ok(None),
            Err(e) => return Err(exec_datafusion_err!("{e}")),
        };
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let tz: Tz = match default_timezone.parse() {
                    Ok(v) => v,
                    Err(_) if safe => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                match tz.from_local_datetime(&datetime).single() {
                    Some(dt) => Ok(Some(dt.timestamp_micros())),
                    None if safe => Ok(None),
                    None => Err(exec_datafusion_err!(
                        "ambiguous or non-existent local datetime"
                    )),
                }
            }
            TimestampParser::Ntz => Ok(Some(datetime.and_utc().timestamp_micros())),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: TimestampParser,
    signature: Signature,
    safe: bool,
}

impl SparkTimestamp {
    /// Creates a SparkTimestamp.
    ///
    /// Handles `to_timestamp` / `try_to_timestamp` for 1 or 2 arguments and all input types.
    /// - `(expr)` — parses strings with default formats, or casts other types.
    /// - `(expr, format)` — parses strings with the given chrono format.
    ///
    /// When `safe` is true, returns NULL on parse/cast failure. When false, errors.
    pub fn try_new(timezone: Option<Arc<str>>, safe: bool) -> Result<Self> {
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
            safe,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    fn parse_strings<F>(
        array: &ArrayRef,
        mut parse: F,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>>
    where
        F: FnMut(&str) -> Result<Option<i64>>,
    {
        match array.data_type() {
            DataType::Utf8 => as_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<_>>(),
            DataType::LargeUtf8 => as_large_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<_>>(),
            DataType::Utf8View => as_string_view_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<_>>(),
            _ => exec_err!("expected string array for `timestamp`"),
        }
    }

    fn extract_scalar_format(value: &ColumnarValue) -> Option<String> {
        match value {
            ColumnarValue::Scalar(scalar) => scalar.try_as_str().flatten().map(|s| s.to_string()),
            _ => None,
        }
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() || args.len() > 2 {
            return exec_err!(
                "spark_timestamp: expected 1 or 2 arguments, got {}",
                args.len()
            );
        }
        let safe = self.safe;
        let value = args[0].clone();
        let format = args.get(1).and_then(Self::extract_scalar_format);

        let array = match &value {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let is_string = matches!(
            array.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );

        let array: ArrayRef = match (is_string, format.as_deref()) {
            (true, Some(fmt)) => {
                let parsed = Self::parse_strings(&array, |s| {
                    self.parser.string_to_microseconds_with_format(s, fmt, safe)
                })?;
                Arc::new(parsed.with_timezone_opt(self.timezone.clone()))
            }
            (true, None) => {
                let parsed =
                    Self::parse_strings(&array, |s| self.parser.string_to_microseconds(s, safe))?;
                Arc::new(parsed.with_timezone_opt(self.timezone.clone()))
            }
            (false, _) => cast_with_options(
                &array,
                &DataType::Timestamp(TimeUnit::Microsecond, self.timezone.clone()),
                &CastOptions {
                    safe,
                    ..Default::default()
                },
            )?,
        };

        match value {
            ColumnarValue::Scalar(_) if number_rows <= 1 => {
                let ts = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>();
                let v = ts.and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                });
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    v,
                    self.timezone.clone(),
                )))
            }
            _ => Ok(ColumnarValue::Array(array)),
        }
    }
}
