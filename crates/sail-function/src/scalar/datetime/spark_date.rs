use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{Array, ArrayRef, Date32Array};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_sql_analyzer::parser::parse_date;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDate {
    signature: Signature,
    safe: bool,
}

impl SparkDate {
    /// Creates a SparkDate.
    ///
    /// Handles `to_date` / `try_to_date` / date casts for all input types.
    /// Accepts 1 or 2 arguments:
    /// - `(expr)` — parses strings with default formats, or casts other types to Date32.
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

    fn string_to_date32_default(value: &str, safe: bool) -> Result<Option<i32>> {
        match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
            Ok(v) => Ok(Some(v)),
            Err(_) if safe => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
        }
    }

    fn string_to_date32_with_format(value: &str, format: &str, safe: bool) -> Result<Option<i32>> {
        match NaiveDate::parse_from_str(value, format) {
            Ok(d) => Ok(Some(Date32Type::from_naive_date(d))),
            Err(_) if safe => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
        }
    }

    fn parse_strings<F>(array: &ArrayRef, mut parse: F) -> Result<ArrayRef>
    where
        F: FnMut(&str) -> Result<Option<i32>>,
    {
        let out: Date32Array = match array.data_type() {
            DataType::Utf8 => as_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Date32Array>>()?,
            DataType::LargeUtf8 => as_large_string_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Date32Array>>()?,
            DataType::Utf8View => as_string_view_array(array)?
                .iter()
                .map(|x| x.map(&mut parse).transpose().map(|o| o.flatten()))
                .collect::<Result<Date32Array>>()?,
            _ => return exec_err!("expected string array for `date`"),
        };
        Ok(Arc::new(out) as ArrayRef)
    }

    fn cast_nonstring_to_date32(array: &ArrayRef, safe: bool) -> Result<ArrayRef> {
        Ok(cast_with_options(
            array,
            &DataType::Date32,
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
                    exec_datafusion_err!("to_date format argument must be a non-null string scalar")
                }),
            ColumnarValue::Array(_) => Err(exec_datafusion_err!(
                "to_date format argument must be a scalar, not an array"
            )),
        }
    }
}

impl ScalarUDFImpl for SparkDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() || args.len() > 2 {
            return exec_err!("spark_date: expected 1 or 2 arguments, got {}", args.len());
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
            (true, Some(fmt)) => {
                Self::parse_strings(&array, |s| Self::string_to_date32_with_format(s, fmt, safe))?
            }
            (true, None) => {
                Self::parse_strings(&array, |s| Self::string_to_date32_default(s, safe))?
            }
            (false, _) => Self::cast_nonstring_to_date32(&array, safe)?,
        };

        match value {
            ColumnarValue::Scalar(_) if number_rows <= 1 => {
                let date_array = result.as_any().downcast_ref::<Date32Array>();
                let v = date_array.and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                });
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(v)))
            }
            _ => Ok(ColumnarValue::Array(result)),
        }
    }
}
