use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_date;

use crate::scalar::datetime::format::DateTimeFormat;

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
        match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(e) => Err(exec_datafusion_err!("{e}")),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (arg, format) = match args.len() {
            1 => (args.one()?, None),
            2 => {
                let (arg, format) = args.two()?;
                (arg, Some(format))
            }
            _ => return exec_err!("spark_date requires 1 or 2 arguments"),
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
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => scalar
                        .try_as_str()
                        .flatten()
                        .map(DateTimeFormat::parse)
                        .transpose()?,
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    _ => return exec_err!("expected string array for `date`"),
                };
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

fn parse_array_with_format_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
    is_try: bool,
) -> Result<ColumnarValue> {
    if array.len() != format_array.len() {
        return exec_err!("spark_date value and format arrays must have the same length");
    }
    let mut cache = HashMap::<String, DateTimeFormat>::new();
    let array = match format_array.data_type() {
        DataType::Utf8 => {
            let formats = as_string_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        DataType::LargeUtf8 => {
            let formats = as_large_string_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        DataType::Utf8View => {
            let formats = as_string_view_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        _ => return exec_err!("spark_date format argument must be a string array"),
    };
    Ok(ColumnarValue::Array(Arc::new(array)))
}

fn parse_array_with_formats<'f>(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    is_try: bool,
) -> Result<Date32Array> {
    match array.data_type() {
        DataType::Utf8 => {
            parse_values_with_formats(as_string_array(array)?.iter(), formats, cache, is_try)
        }
        DataType::LargeUtf8 => {
            parse_values_with_formats(as_large_string_array(array)?.iter(), formats, cache, is_try)
        }
        DataType::Utf8View => {
            parse_values_with_formats(as_string_view_array(array)?.iter(), formats, cache, is_try)
        }
        _ => exec_err!("expected string array for `date`"),
    }
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
    if !cache.contains_key(pattern) {
        cache.insert(pattern.to_string(), DateTimeFormat::parse(pattern)?);
    }
    Ok(cache.get(pattern).expect("datetime format was inserted"))
}
