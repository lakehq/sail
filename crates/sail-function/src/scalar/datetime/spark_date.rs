use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{Array, ArrayRef, Date32Array};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use sail_sql_analyzer::parser::parse_date;

use crate::error::invalid_arg_count_exec_err;

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
    /// - `(expr, format)` — parses strings with the given chrono format. The format
    ///   may be a scalar string (broadcast) or a string column (per-row).
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
        match parse_date(value).and_then(|d| Ok(Date32Type::from_naive_date(d.try_into()?))) {
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

    /// Borrowed view over a string array as `Iterator<Item = Option<&str>>`.
    fn string_array_iter(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
        match array.data_type() {
            DataType::Utf8 => Ok(Box::new(as_string_array(array)?.iter())),
            DataType::LargeUtf8 => Ok(Box::new(as_large_string_array(array)?.iter())),
            DataType::Utf8View => Ok(Box::new(as_string_view_array(array)?.iter())),
            other => exec_err!("expected string array, got {other}"),
        }
    }

    fn parse_value_array(value_arr: &ArrayRef, safe: bool) -> Result<ArrayRef> {
        let out: Date32Array = Self::string_array_iter(value_arr)?
            .map(|v| match v {
                Some(s) => Self::string_to_date32_default(s, safe),
                None => Ok(None),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(out) as ArrayRef)
    }

    fn parse_value_with_format_array(
        value_arr: &ArrayRef,
        format_arr: &ArrayRef,
        safe: bool,
    ) -> Result<ArrayRef> {
        if value_arr.len() != format_arr.len() {
            return exec_err!(
                "to_date: value array length ({}) does not match format array length ({})",
                value_arr.len(),
                format_arr.len()
            );
        }
        let values = Self::string_array_iter(value_arr)?;
        let formats = Self::string_array_iter(format_arr)?;
        let out: Date32Array = values
            .zip(formats)
            .map(|(v, f)| match (v, f) {
                (Some(s), Some(fmt)) => Self::string_to_date32_with_format(s, fmt, safe),
                _ => Ok(None),
            })
            .collect::<Result<_>>()?;
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

    /// Inner per-batch kernel. After `make_scalar_function` broadcasting, scalar
    /// inputs are already expanded to arrays of the batch length, so we never
    /// need to special-case scalar vs array here.
    fn kernel(safe: bool, args: &[ArrayRef]) -> Result<ArrayRef> {
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
                    return exec_err!(
                        "to_date format argument must be a string, got {}",
                        fmt.data_type()
                    );
                }
                Self::parse_value_with_format_array(value_arr, fmt, safe)
            }
            (true, None) => Self::parse_value_array(value_arr, safe),
            (false, _) => Self::cast_nonstring_to_date32(value_arr, safe),
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
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 2),
                args.args.len(),
            ));
        }
        let safe = self.safe;
        make_scalar_function(move |a: &[ArrayRef]| Self::kernel(safe, a), vec![])(&args.args)
    }
}
