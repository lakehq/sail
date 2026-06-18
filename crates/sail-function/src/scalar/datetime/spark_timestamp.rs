use std::fmt::Debug;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::parser::parse_timestamp;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
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
            signature: Signature::user_defined(Volatility::Immutable),
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
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
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
        let timezone = self.timezone.clone();
        // The parser owns a small String; clone it into the kernel closure so the
        // closure is self-contained.
        let parser = match &self.parser {
            TimestampParser::Ltz { default_timezone } => TimestampParser::Ltz {
                default_timezone: default_timezone.clone(),
            },
            TimestampParser::Ntz => TimestampParser::Ntz,
        };
        let safe = self.safe();
        let result = make_scalar_function(
            move |a: &[ArrayRef]| {
                Self::kernel(&parser, safe, a).map(|arr| Arc::new(arr) as ArrayRef)
            },
            vec![],
        )(&args.args)?;
        // Attach the target timezone (LTZ keeps it, NTZ is None).
        match result {
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| exec_datafusion_err!("expected timestamp array"))?
                    .clone()
                    .with_timezone_opt(timezone);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            // `make_scalar_function` can fold a single-row result back to a scalar;
            // re-attach the target timezone the kernel dropped.
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, _)) => Ok(
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, timezone)),
            ),
            ColumnarValue::Scalar(other) => Ok(ColumnarValue::Scalar(other)),
        }
    }
}
