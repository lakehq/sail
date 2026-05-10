use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{Local, NaiveDateTime, NaiveTime, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::error::invalid_arg_count_exec_err;

/// Parse a time-only string as a NaiveDateTime at the current local date.
///
/// Spark composes time-only strings with today's date, not the epoch.
fn try_time_only_naive(value: &str) -> Option<NaiveDateTime> {
    const TIME_FORMATS: &[&str] = &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"];
    for fmt in TIME_FORMATS {
        if let Ok(t) = NaiveTime::parse_from_str(value, fmt) {
            return Some(Local::now().date_naive().and_time(t));
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

/// Spark-compatible `to_timestamp` / `try_to_timestamp` function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#to_timestamp>
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
            signature: Signature::user_defined(Volatility::Immutable),
            safe,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    fn string_array_iter(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
        match array.data_type() {
            DataType::Utf8 => Ok(Box::new(as_string_array(array)?.iter())),
            DataType::LargeUtf8 => Ok(Box::new(as_large_string_array(array)?.iter())),
            DataType::Utf8View => Ok(Box::new(as_string_view_array(array)?.iter())),
            other => exec_err!("expected string array, got {other}"),
        }
    }

    fn parse_value_array(
        parser: &TimestampParser,
        safe: bool,
        value_arr: &ArrayRef,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        Self::string_array_iter(value_arr)?
            .map(|v| match v {
                Some(s) => parser.string_to_microseconds(s, safe),
                None => Ok(None),
            })
            .collect::<Result<_>>()
    }

    fn parse_value_with_format_array(
        parser: &TimestampParser,
        safe: bool,
        value_arr: &ArrayRef,
        format_arr: &ArrayRef,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        if value_arr.len() != format_arr.len() {
            return exec_err!(
                "to_timestamp: value array length ({}) does not match format array length ({})",
                value_arr.len(),
                format_arr.len()
            );
        }
        let values = Self::string_array_iter(value_arr)?;
        let formats = Self::string_array_iter(format_arr)?;
        values
            .zip(formats)
            .map(|(v, f)| match (v, f) {
                (Some(s), Some(fmt)) => parser.string_to_microseconds_with_format(s, fmt, safe),
                _ => Ok(None),
            })
            .collect::<Result<_>>()
    }

    /// Inner per-batch kernel. After `make_scalar_function` broadcasting,
    /// scalar inputs are already expanded to arrays so we never need to
    /// special-case scalar vs array here.
    fn kernel(
        parser: &TimestampParser,
        timezone: Option<Arc<str>>,
        safe: bool,
        args: &[ArrayRef],
    ) -> Result<ArrayRef> {
        let value_arr = &args[0];
        let format_arr = args.get(1);
        let is_string = matches!(
            value_arr.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        let parsed: PrimitiveArray<TimestampMicrosecondType> = match (is_string, format_arr) {
            (true, Some(fmt)) => {
                if !matches!(
                    fmt.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) {
                    return exec_err!(
                        "to_timestamp format argument must be a string, got {}",
                        fmt.data_type()
                    );
                }
                Self::parse_value_with_format_array(parser, safe, value_arr, fmt)?
            }
            (true, None) => Self::parse_value_array(parser, safe, value_arr)?,
            (false, _) => {
                return Ok(cast_with_options(
                    value_arr,
                    &DataType::Timestamp(TimeUnit::Microsecond, timezone),
                    &CastOptions {
                        safe,
                        ..Default::default()
                    },
                )?);
            }
        };
        Ok(Arc::new(parsed.with_timezone_opt(timezone)))
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        match (self.safe, self.timezone.is_some()) {
            (true, _) => "try_to_timestamp",
            (false, true) => "to_timestamp",
            (false, false) => "to_timestamp_ntz",
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
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Null => {}
            other => {
                return plan_err!(
                    "{}: value argument must be string, date, timestamp, numeric or null, got {other}",
                    self.name()
                );
            }
        }
        if let Some(format) = arg_types.get(1) {
            match format {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
                other => {
                    return plan_err!(
                        "{}: format argument must be a string, got {other}",
                        self.name()
                    );
                }
            }
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let safe = self.safe;
        let parser = self.parser.clone();
        let timezone = self.timezone.clone();
        make_scalar_function(
            move |a: &[ArrayRef]| Self::kernel(&parser, timezone.clone(), safe, a),
            vec![],
        )(&args.args)
    }
}
