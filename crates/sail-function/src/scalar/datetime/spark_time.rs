use std::sync::Arc;

use chrono::{NaiveTime, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, Time64MicrosecondArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

const DEFAULT_TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.f",
    "%H:%M:%S",
    "%H:%M",
    "%H:%M:%S%.f %p",
    "%H:%M:%S %p",
    "%H:%M %p",
];

/// Spark-compatible `to_time` / `try_to_time` function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#to_time>
///
/// Accepts 1 or 2 arguments:
/// - `(expr)` — parses strings with default formats, or casts other types to Time64.
/// - `(expr, format)` — parses strings with the given chrono format. The format
///   may be a scalar string (broadcast) or a string column (per-row).
///
/// `to_time` always errors on invalid input (Spark's `ToTime` does not honor ANSI);
/// `try_to_time` (`is_try = true`) returns NULL on parse/cast failure — mirroring
/// Spark's `try_to_time = TryEval(ToTime(...))`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTime {
    signature: Signature,
    is_try: bool,
}

impl SparkTime {
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            is_try,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn naive_time_to_us(t: NaiveTime) -> i64 {
        let seconds = t.num_seconds_from_midnight() as i64;
        let nanos = t.nanosecond() as i64;
        seconds * 1_000_000 + nanos / 1_000
    }

    fn string_to_time_us_default(value: &str, is_try: bool) -> Result<Option<i64>> {
        for fmt in DEFAULT_TIME_FORMATS {
            if let Ok(t) = NaiveTime::parse_from_str(value, fmt) {
                return Ok(Some(Self::naive_time_to_us(t)));
            }
        }
        if is_try {
            Ok(None)
        } else {
            Err(exec_datafusion_err!(
                "cannot parse '{value}' as time with default formats"
            ))
        }
    }

    fn string_to_time_us_with_format(
        value: &str,
        format: &str,
        is_try: bool,
    ) -> Result<Option<i64>> {
        match NaiveTime::parse_from_str(value, format) {
            Ok(t) => Ok(Some(Self::naive_time_to_us(t))),
            Err(_) if is_try => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
        }
    }

    fn string_array_iter(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
        match array.data_type() {
            DataType::Utf8 => Ok(Box::new(as_string_array(array)?.iter())),
            DataType::LargeUtf8 => Ok(Box::new(as_large_string_array(array)?.iter())),
            DataType::Utf8View => Ok(Box::new(as_string_view_array(array)?.iter())),
            other => exec_err!("expected string array, got {other}"),
        }
    }

    fn parse_value_array(value_arr: &ArrayRef, is_try: bool) -> Result<ArrayRef> {
        let out: Time64MicrosecondArray = Self::string_array_iter(value_arr)?
            .map(|v| match v {
                Some(s) => Self::string_to_time_us_default(s, is_try),
                None => Ok(None),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(out) as ArrayRef)
    }

    fn parse_value_with_format_array(
        value_arr: &ArrayRef,
        format_arr: &ArrayRef,
        is_try: bool,
    ) -> Result<ArrayRef> {
        if value_arr.len() != format_arr.len() {
            return exec_err!(
                "{}: value array length ({}) does not match format array length ({})",
                if is_try { "try_to_time" } else { "to_time" },
                value_arr.len(),
                format_arr.len()
            );
        }
        let values = Self::string_array_iter(value_arr)?;
        let formats = Self::string_array_iter(format_arr)?;
        let out: Time64MicrosecondArray = values
            .zip(formats)
            .map(|(v, f)| match (v, f) {
                (Some(s), Some(fmt)) => Self::string_to_time_us_with_format(s, fmt, is_try),
                _ => Ok(None),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(out) as ArrayRef)
    }

    fn cast_nonstring_to_time(array: &ArrayRef, is_try: bool) -> Result<ArrayRef> {
        Ok(cast_with_options(
            array,
            &DataType::Time64(TimeUnit::Microsecond),
            &CastOptions {
                safe: is_try,
                ..Default::default()
            },
        )?)
    }

    fn kernel(is_try: bool, args: &[ArrayRef]) -> Result<ArrayRef> {
        let value_arr = &args[0];
        let format_arr = args.get(1);
        let is_string = matches!(
            value_arr.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        match (is_string, format_arr) {
            (true, Some(fmt)) => {
                if !matches!(
                    fmt.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) {
                    return Err(unsupported_data_type_exec_err(
                        if is_try { "try_to_time" } else { "to_time" },
                        "STRING",
                        fmt.data_type(),
                    ));
                }
                Self::parse_value_with_format_array(value_arr, fmt, is_try)
            }
            (true, None) => Self::parse_value_array(value_arr, is_try),
            (false, _) => Self::cast_nonstring_to_time(value_arr, is_try),
        }
    }
}

impl ScalarUDFImpl for SparkTime {
    fn name(&self) -> &str {
        if self.is_try {
            "try_to_time"
        } else {
            "to_time"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
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
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Null => {}
            other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "STRING, TIME, TIMESTAMP or NULL",
                    other,
                ));
            }
        }
        let mut coerced = arg_types.to_vec();
        if let Some(format) = arg_types.get(1) {
            match format {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                // A NULL format yields a NULL result; coerce it to a Utf8 null so
                // the kernel's per-row None handling produces NULL instead of
                // erroring on the `Null` type.
                DataType::Null => coerced[1] = DataType::Utf8,
                other => {
                    return Err(unsupported_data_type_exec_err(self.name(), "STRING", other));
                }
            }
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let is_try = self.is_try;
        make_scalar_function(move |a: &[ArrayRef]| Self::kernel(is_try, a), vec![])(&args.args)
    }
}
