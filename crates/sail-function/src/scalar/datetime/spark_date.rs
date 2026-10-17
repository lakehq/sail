use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{Array, Date32Array, new_null_array};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use datafusion_common::cast::as_int32_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_date;

use crate::scalar::datetime::format::DateTimeFormat;
use crate::scalar::datetime::utils::string_array_iter;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDate {
    signature: Signature,
    is_try: bool,
}

impl SparkDate {
    /// Creates a SparkDate.
    ///
    /// When `is_try` is true, returns NULL on invalid input (for try_cast).
    /// When `is_try` is false, throws an error on invalid input (for cast).
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            is_try,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn string_to_date32(value: &str, is_try: bool) -> Result<Option<i32>> {
        // Keep already-supported date strings on the original parser path.
        match parse_date(value)
            .or_else(|error| {
                let prefix = date_string_prefix(value);
                if prefix.len() == value.len() {
                    Err(error)
                } else {
                    parse_date(prefix)
                }
            })
            .and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?)))
        {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(_) => {
                let sql_value = value.replace('\\', "\\\\").replace('\'', "\\'");
                Err(exec_datafusion_err!(
                    "[CAST_INVALID_INPUT] The value '{sql_value}' of the type \"STRING\" cannot be cast to \"DATE\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
                ))
            }
        }
    }

    fn formatted_string_to_date32(
        value: &str,
        format: &DateTimeFormat,
        is_try: bool,
    ) -> Result<Option<i32>> {
        match format
            .parse_date_value(value)
            .map(Date32Type::from_naive_date)
        {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn date_string_prefix(value: &str) -> &str {
    // Spark ignores the suffix after a complete date's `T`; the SQL parser handles spaces.
    let trimmed = value.trim_matches(|c: char| c <= ' ');
    match trimmed.split_once('T') {
        Some((date, _))
            if date
                .trim_start_matches(['+', '-'])
                .bytes()
                .filter(|c| *c == b'-')
                .count()
                == 2 =>
        {
            date
        }
        _ => trimmed,
    }
}

impl ScalarUDFImpl for SparkDate {
    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.len() != 3
            || self.is_try
            || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() == 3 {
            let make_date = datafusion::functions::datetime::make_date();
            if !self.is_try {
                return make_date.invoke_with_args(args);
            }
            // Valid batches keep the native path; only invalid calendar values need NULLs.
            let values = args.args.clone();
            return match make_date.invoke_with_args(args) {
                Err(datafusion_common::DataFusionError::Execution(_)) => {
                    make_date_from_components(values)
                }
                result => result,
            };
        }
        let ScalarFunctionArgs { args, .. } = args;
        let (arg, format) = match args.len() {
            1 => (args.one()?, None),
            2 => {
                let (arg, format) = args.two()?;
                (arg, Some(format))
            }
            _ => return exec_err!("spark_date requires 1, 2, or 3 arguments"),
        };
        let is_try = self.is_try;
        match (arg, format) {
            (ColumnarValue::Array(array), Some(ColumnarValue::Array(format_array))) => {
                parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Scalar(scalar), Some(ColumnarValue::Array(format_array))) => {
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar),
                    ColumnarValue::Array(format_array),
                ])?;
                let array = arrays[0].clone();
                let format_array = arrays[1].clone();
                parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Array(array), format) => {
                // Intentionally return NULL before pattern validation in both ANSI modes.
                // Spark's generated formatter can fail before its NULL check; interpreted
                // evaluation skips it. Restrict this shortcut to supported string/NULL types.
                if (array.data_type().is_string() || array.data_type() == &DataType::Null)
                    && array.null_count() == array.len()
                {
                    return Ok(ColumnarValue::Array(new_null_array(
                        &DataType::Date32,
                        array.len(),
                    )));
                }
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => {
                        match parse_scalar_date_format(&scalar)? {
                            Some(format) => Some(format),
                            None => {
                                return Ok(ColumnarValue::Array(new_null_array(
                                    &DataType::Date32,
                                    array.len(),
                                )));
                            }
                        }
                    }
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let array = string_array_iter(array.as_ref())?
                    .map(|value| {
                        value
                            .map(|value| match &format {
                                Some(format) => {
                                    Self::formatted_string_to_date32(value, format, is_try)
                                }
                                None => Self::string_to_date32(value, is_try),
                            })
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<Date32Array>>()?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ColumnarValue::Scalar(scalar), format) => {
                // Preserve the same NULL-first contract for scalar inputs.
                if (scalar.data_type().is_string() || scalar.data_type() == DataType::Null)
                    && scalar.is_null()
                {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)));
                }
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => {
                        match parse_scalar_date_format(&scalar)? {
                            Some(format) => Some(format),
                            None => {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)));
                            }
                        }
                    }
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| match &format {
                            Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                            None => Self::string_to_date32(v, is_try),
                        })
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!("expected string scalar for `date`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(value)))
            }
        }
    }
}

fn make_date_from_components(args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
    if let [
        ColumnarValue::Scalar(ScalarValue::Int32(year)),
        ColumnarValue::Scalar(ScalarValue::Int32(month)),
        ColumnarValue::Scalar(ScalarValue::Int32(day)),
    ] = args.as_slice()
    {
        return Ok(ColumnarValue::Scalar(ScalarValue::Date32(make_date_value(
            *year, *month, *day,
        ))));
    }
    let arrays = ColumnarValue::values_to_arrays(&args)?;
    let years = as_int32_array(&arrays[0])?;
    let months = as_int32_array(&arrays[1])?;
    let days = as_int32_array(&arrays[2])?;
    let dates = years
        .iter()
        .zip(months)
        .zip(days)
        .map(|((year, month), day)| make_date_value(year, month, day))
        .collect::<Date32Array>();
    Ok(ColumnarValue::Array(Arc::new(dates)))
}

fn make_date_value(year: Option<i32>, month: Option<i32>, day: Option<i32>) -> Option<i32> {
    let date = NaiveDate::from_ymd_opt(
        year?,
        u32::try_from(month?).ok()?,
        u32::try_from(day?).ok()?,
    )?;
    // Date32 counts days since 1970-01-01, which is day 719163 in the CE calendar.
    Some(date.num_days_from_ce() - 719_163)
}

fn parse_scalar_date_format(format: &ScalarValue) -> Result<Option<DateTimeFormat>> {
    match format.try_as_str() {
        Some(Some(pattern)) => Ok(Some(DateTimeFormat::for_parsing(pattern)?)),
        Some(None) => Ok(None),
        None => exec_err!("spark_date format argument must be a string scalar"),
    }
}

fn parse_array_with_format_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
    is_try: bool,
) -> Result<ColumnarValue> {
    if array.len() != format_array.len() {
        return exec_err!("spark_date value and format arrays must have the same length");
    }
    let mut cache = HashMap::<String, DateTimeFormat>::new();
    let formats = string_array_iter(format_array.as_ref())?;
    let array = parse_array_with_formats(array, formats, &mut cache, is_try)?;
    Ok(ColumnarValue::Array(Arc::new(array)))
}

fn parse_array_with_formats<'f>(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    is_try: bool,
) -> Result<Date32Array> {
    parse_values_with_formats(string_array_iter(array.as_ref())?, formats, cache, is_try)
}

fn parse_values_with_formats<'v, 'f>(
    values: impl Iterator<Item = Option<&'v str>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    is_try: bool,
) -> Result<Date32Array> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format)?;
                SparkDate::formatted_string_to_date32(value, format, is_try)
            }
            _ => Ok(None),
        })
        .collect::<Result<_>>()
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
