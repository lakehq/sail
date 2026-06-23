use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::datetime::to_timestamp::ToTimestampSecondsFunc;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::localize_with_fallback;

use crate::scalar::datetime::format::DateTimeFormat;
use crate::scalar::datetime::utils::validate_data_types;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixTimestamp {
    signature: Signature,
    timezone: Arc<str>,
}

impl SparkUnixTimestamp {
    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            timezone,
        }
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }
}

impl ScalarUDFImpl for SparkUnixTimestamp {
    fn name(&self) -> &str {
        "spark_unix_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [first, ..] = args.args.as_slice() else {
            return exec_err!("spark_unix_timestamp function requires 1 or more arguments");
        };
        let format = match args.args.len() {
            1 => None,
            2 => Some(&args.args[1]),
            _ => return exec_err!("spark_unix_timestamp function requires 1 or 2 arguments"),
        };
        if format.is_none() {
            validate_data_types(&args.args, "spark_unix_timestamp", 1)?;
        }

        if let Some(format) = format {
            return self.invoke_with_format(first, format);
        }

        match first.data_type() {
            DataType::Int32 | DataType::Int64 => args.args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, _) => args.args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                ToTimestampSecondsFunc::new_with_config(args.config_options.as_ref())
                    .invoke_with_args(args)?
                    .cast_to(
                        &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                        None,
                    )?
                    .cast_to(&DataType::Int64, None)
            }
            other => {
                exec_err!("spark_unix_timestamp function unsupported data type: {other}")
            }
        }
    }
}

impl SparkUnixTimestamp {
    fn invoke_with_format(
        &self,
        first: &ColumnarValue,
        format: &ColumnarValue,
    ) -> Result<ColumnarValue> {
        match (first, format) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(format_scalar)) => {
                let format = parse_format_scalar(format_scalar)?;
                let array: PrimitiveArray<datafusion::arrow::datatypes::Int64Type> =
                    match array.data_type() {
                        DataType::Utf8 => as_string_array(array)?
                            .iter()
                            .map(|x| {
                                x.map(|value| self.formatted_string_to_seconds(value, &format))
                                    .transpose()
                                    .map(|opt| opt.flatten())
                            })
                            .collect::<Result<_>>()?,
                        DataType::LargeUtf8 => as_large_string_array(array)?
                            .iter()
                            .map(|x| {
                                x.map(|value| self.formatted_string_to_seconds(value, &format))
                                    .transpose()
                                    .map(|opt| opt.flatten())
                            })
                            .collect::<Result<_>>()?,
                        DataType::Utf8View => as_string_view_array(array)?
                            .iter()
                            .map(|x| {
                                x.map(|value| self.formatted_string_to_seconds(value, &format))
                                    .transpose()
                                    .map(|opt| opt.flatten())
                            })
                            .collect::<Result<_>>()?,
                        other => {
                            return exec_err!(
                                "spark_unix_timestamp function unsupported formatted input data type: {other}"
                            )
                        }
                    };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ColumnarValue::Array(array), ColumnarValue::Array(format_array)) => {
                self.invoke_array_with_format_array(array, format_array)
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Scalar(format_scalar)) => {
                let format = parse_format_scalar(format_scalar)?;
                let value = match scalar.try_as_str() {
                    Some(value) => value
                        .map(|value| self.formatted_string_to_seconds(value, &format))
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!(
                            "spark_unix_timestamp function expected string scalar for formatted input"
                        );
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(value)))
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(format_array)) => {
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar.clone()),
                    ColumnarValue::Array(format_array.clone()),
                ])?;
                let array = arrays[0].clone();
                let format_array = arrays[1].clone();
                self.invoke_array_with_format_array(&array, &format_array)
            }
        }
    }

    fn invoke_array_with_format_array(
        &self,
        array: &dyn datafusion::arrow::array::Array,
        format_array: &dyn datafusion::arrow::array::Array,
    ) -> Result<ColumnarValue> {
        if array.len() != format_array.len() {
            return exec_err!(
                "spark_unix_timestamp value and format arrays must have the same length"
            );
        }
        let mut cache = HashMap::<String, DateTimeFormat>::new();
        let array = match format_array.data_type() {
            DataType::Utf8 => {
                let formats = as_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache)?
            }
            DataType::LargeUtf8 => {
                let formats = as_large_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache)?
            }
            DataType::Utf8View => {
                let formats = as_string_view_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache)?
            }
            _ => return exec_err!("spark_unix_timestamp format argument must be a string array"),
        };
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn parse_array_with_formats<'f>(
        &self,
        array: &dyn datafusion::arrow::array::Array,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
    ) -> Result<PrimitiveArray<datafusion::arrow::datatypes::Int64Type>> {
        match array.data_type() {
            DataType::Utf8 => {
                self.parse_values_with_formats(as_string_array(array)?.iter(), formats, cache)
            }
            DataType::LargeUtf8 => {
                self.parse_values_with_formats(as_large_string_array(array)?.iter(), formats, cache)
            }
            DataType::Utf8View => {
                self.parse_values_with_formats(as_string_view_array(array)?.iter(), formats, cache)
            }
            other => exec_err!(
                "spark_unix_timestamp function unsupported formatted input data type: {other}"
            ),
        }
    }

    fn parse_values_with_formats<'v, 'f>(
        &self,
        values: impl Iterator<Item = Option<&'v str>>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
    ) -> Result<PrimitiveArray<datafusion::arrow::datatypes::Int64Type>> {
        values
            .zip(formats)
            .map(|(value, format)| match (value, format) {
                (Some(value), Some(format)) => {
                    let format = get_or_parse_format(cache, format)?;
                    self.formatted_string_to_seconds(value, format)
                }
                _ => Ok(None),
            })
            .collect::<Result<_>>()
    }

    fn formatted_string_to_seconds(
        &self,
        value: &str,
        format: &DateTimeFormat,
    ) -> Result<Option<i64>> {
        let parsed = format.parse_datetime_value(value)?;
        let timestamp = if let Some(offset) = parsed.offset {
            parsed
                .datetime
                .and_local_timezone(offset)
                .single()
                .map(|x| x.to_utc().timestamp())
                .ok_or_else(|| exec_datafusion_err!("cannot apply parsed offset"))?
        } else {
            let timezone: Tz = parsed
                .timezone
                .as_deref()
                .unwrap_or(&self.timezone)
                .parse()?;
            localize_with_fallback(&timezone, &parsed.datetime)?.timestamp()
        };
        Ok(Some(timestamp))
    }
}

fn parse_format_scalar(scalar: &ScalarValue) -> Result<DateTimeFormat> {
    scalar
        .try_as_str()
        .flatten()
        .map(DateTimeFormat::parse)
        .transpose()?
        .ok_or_else(|| exec_datafusion_err!("spark_unix_timestamp format cannot be null"))
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
