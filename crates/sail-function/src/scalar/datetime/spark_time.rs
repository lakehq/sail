use std::any::Any;
use std::sync::Arc;

use chrono::{NaiveTime, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, Time64MicrosecondArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

const DEFAULT_TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.f",
    "%H:%M:%S",
    "%H:%M",
    "%H:%M:%S%.f %p",
    "%H:%M:%S %p",
    "%H:%M %p",
];

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTime {
    signature: Signature,
    safe: bool,
}

impl SparkTime {
    /// Creates a SparkTime.
    ///
    /// Handles `to_time` / `try_to_time` for all input types.
    /// Accepts 1 or 2 arguments:
    /// - `(expr)` — parses strings with default formats, or casts other types to Time64.
    /// - `(expr, format)` — parses strings with the given chrono format.
    ///
    /// When `safe` is true, returns NULL on parse/cast failure. When false, errors.
    pub fn new(safe: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            safe,
        }
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    fn naive_time_to_us(t: NaiveTime) -> i64 {
        let seconds = t.num_seconds_from_midnight() as i64;
        let nanos = t.nanosecond() as i64;
        seconds * 1_000_000 + nanos / 1_000
    }

    fn string_to_time_us_default(value: &str, safe: bool) -> Result<Option<i64>> {
        for fmt in DEFAULT_TIME_FORMATS {
            if let Ok(t) = NaiveTime::parse_from_str(value, fmt) {
                return Ok(Some(Self::naive_time_to_us(t)));
            }
        }
        if safe {
            Ok(None)
        } else {
            Err(exec_datafusion_err!(
                "cannot parse '{value}' as time with default formats"
            ))
        }
    }

    fn string_to_time_us_with_format(value: &str, format: &str, safe: bool) -> Result<Option<i64>> {
        match NaiveTime::parse_from_str(value, format) {
            Ok(t) => Ok(Some(Self::naive_time_to_us(t))),
            Err(_) if safe => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
        }
    }

    fn parse_strings<F>(array: &ArrayRef, mut parse: F) -> Result<ArrayRef>
    where
        F: FnMut(&str) -> Result<Option<i64>>,
    {
        let out: Time64MicrosecondArray = match array.data_type() {
            DataType::Utf8 => as_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Time64MicrosecondArray>>()?,
            DataType::LargeUtf8 => as_large_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Time64MicrosecondArray>>()?,
            DataType::Utf8View => as_string_view_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Time64MicrosecondArray>>()?,
            _ => return exec_err!("expected string array for `time`"),
        };
        Ok(Arc::new(out) as ArrayRef)
    }

    fn cast_nonstring_to_time(array: &ArrayRef, safe: bool) -> Result<ArrayRef> {
        Ok(cast_with_options(
            array,
            &DataType::Time64(TimeUnit::Microsecond),
            &CastOptions {
                safe,
                ..Default::default()
            },
        )?)
    }

    /// Require the 2nd argument to be a constant string. Returning `None` here
    /// would cause silent fallback to default parsing, ignoring the user's
    /// format — instead we error explicitly.
    fn require_scalar_format(value: &ColumnarValue) -> Result<String> {
        match value {
            ColumnarValue::Scalar(scalar) => scalar
                .try_as_str()
                .flatten()
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    exec_datafusion_err!("to_time format argument must be a non-null string scalar")
                }),
            ColumnarValue::Array(_) => Err(exec_datafusion_err!(
                "to_time format argument must be a scalar, not an array"
            )),
        }
    }
}

impl ScalarUDFImpl for SparkTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() || args.len() > 2 {
            return exec_err!("spark_time: expected 1 or 2 arguments, got {}", args.len());
        }
        let safe = self.safe;
        let value = args[0].clone();
        let format = match args.get(1) {
            Some(v) => Some(Self::require_scalar_format(v)?),
            None => None,
        };

        let array = match &value {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let is_string = matches!(
            array.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );

        let result: ArrayRef = match (is_string, format.as_deref()) {
            (true, Some(fmt)) => Self::parse_strings(&array, |s| {
                Self::string_to_time_us_with_format(s, fmt, safe)
            })?,
            (true, None) => {
                Self::parse_strings(&array, |s| Self::string_to_time_us_default(s, safe))?
            }
            (false, _) => Self::cast_nonstring_to_time(&array, safe)?,
        };

        match value {
            ColumnarValue::Scalar(_) if number_rows <= 1 => {
                let time_array = result.as_any().downcast_ref::<Time64MicrosecondArray>();
                let v = time_array.and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                });
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(v)))
            }
            _ => Ok(ColumnarValue::Array(result)),
        }
    }
}
