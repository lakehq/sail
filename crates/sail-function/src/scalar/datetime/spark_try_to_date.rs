use std::sync::Arc;

use arrow::array::types::Date32Type;
use arrow::array::{Array, Date32Array};
use arrow::compute::cast_with_options;
use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::*;
use datafusion::functions::datetime::to_date::ToDateFunc;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignature;

use arrow::compute::CastOptions;

const TRY_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToDate {
    signature: Signature,
}

impl Default for SparkTryToDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }

    fn try_to_date_strings(
        &self,
        args: &[ColumnarValue],
        invoke_args: &ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        if args.len() < 2 {
            return self.try_to_date_unformatted(&args[0]);
        }
        self.try_to_date_formatted(args, invoke_args)
    }

    fn try_to_date_unformatted(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        match arg {
            ColumnarValue::Array(array) => {
                let date_array = match array.data_type() {
                    Utf8 => {
                        let a = as_string_array(array)?;
                        let mut builder = Date32Array::builder(a.len());
                        for value in a.iter() {
                            append_parsed_date(&mut builder, value, Date32Type::parse);
                        }
                        builder.finish()
                    }
                    LargeUtf8 => {
                        let a = as_large_string_array(array)?;
                        let mut builder = Date32Array::builder(a.len());
                        for value in a.iter() {
                            append_parsed_date(&mut builder, value, Date32Type::parse);
                        }
                        builder.finish()
                    }
                    Utf8View => {
                        let a = as_string_view_array(array)?;
                        let mut builder = Date32Array::builder(a.len());
                        for value in a.iter() {
                            append_parsed_date(&mut builder, value, Date32Type::parse);
                        }
                        builder.finish()
                    }
                    other => {
                        return exec_err!(
                            "Unsupported data type {other:?} for function try_to_date"
                        );
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(date_array)))
            }
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
                Some(value) => {
                    let date = value.and_then(|s| Date32Type::parse(s));
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(date)))
                }
                _ => exec_err!("Unsupported data type {scalar:?} for function try_to_date"),
            },
        }
    }

    fn try_to_date_formatted(
        &self,
        args: &[ColumnarValue],
        invoke_args: &ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let format_args = &args[1..];
        match &args[0] {
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut builder = Date32Array::builder(len);
                for row in 0..len {
                    let value = string_value_at(&args[0], row)?;
                    let date = value
                        .and_then(|s| try_to_date_formatted_row(s, row, format_args, invoke_args));
                    match date {
                        Some(days) => builder.append_value(days),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
                Some(value) => {
                    let date = value
                        .and_then(|s| try_to_date_formatted_scalar(s, format_args, invoke_args));
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(date)))
                }
                _ => exec_err!("Unsupported data type {scalar:?} for function try_to_date"),
            },
        }
    }
}

impl ScalarUDFImpl for SparkTryToDate {
    fn name(&self) -> &str {
        "spark_try_to_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columnar_args = &args.args;
        if columnar_args.is_empty() {
            return exec_err!("try_to_date function requires 1 or more arguments, got 0");
        }

        if columnar_args.len() > 1 {
            validate_format_args(columnar_args, "try_to_date")?;
        }

        match columnar_args[0].data_type() {
            Null | Int32 | Int64 | Date32 | Date64 | Timestamp(_, _) => {
                columnar_args[0].cast_to(&Date32, Some(&TRY_CAST_OPTIONS))
            }
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 => match &columnar_args[0] {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_with_options(
                    &cast_with_options(array, &Int32, &TRY_CAST_OPTIONS)?,
                    &Date32,
                    &TRY_CAST_OPTIONS,
                )?)),
                ColumnarValue::Scalar(scalar) => {
                    let int32 = scalar.cast_to_with_options(&Int32, &TRY_CAST_OPTIONS)?;
                    Ok(ColumnarValue::Scalar(
                        int32.cast_to_with_options(&Date32, &TRY_CAST_OPTIONS)?,
                    ))
                }
            },
            Float16
            | Float32
            | Float64
            | Decimal32(_, _)
            | Decimal64(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _) => columnar_args[0]
                .cast_to(&Int64, Some(&TRY_CAST_OPTIONS))?
                .cast_to(&Date32, Some(&TRY_CAST_OPTIONS)),
            Utf8View | LargeUtf8 | Utf8 => self.try_to_date_strings(columnar_args, &args),
            other => exec_err!("Unsupported data type {other} for function try_to_date"),
        }
    }
}

fn validate_format_args(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, arg) in args.iter().skip(1).enumerate() {
        match arg.data_type() {
            Utf8View | LargeUtf8 | Utf8 => {}
            other => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {other}",
                    idx + 1
                );
            }
        }
    }
    Ok(())
}

fn append_parsed_date<F>(builder: &mut arrow::array::Date32Builder, value: Option<&str>, parse: F)
where
    F: Fn(&str) -> Option<i32>,
{
    match value {
        Some(s) => match parse(s) {
            Some(days) => builder.append_value(days),
            None => builder.append_null(),
        },
        None => builder.append_null(),
    }
}

fn string_value_at(arg: &ColumnarValue, row: usize) -> Result<Option<&str>> {
    match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            Utf8 => {
                let a = as_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            LargeUtf8 => {
                let a = as_large_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            Utf8View => {
                let a = as_string_view_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            other => exec_err!("Unexpected type encountered '{other}'"),
        },
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(value) => Ok(value),
            None => exec_err!("Unexpected scalar type encountered '{scalar}'"),
        },
    }
}

fn format_value_at(arg: &ColumnarValue, row: usize) -> Result<Option<&str>> {
    string_value_at(arg, row)
}

fn try_to_date_formatted_scalar(
    value: &str,
    format_args: &[ColumnarValue],
    invoke_args: &ScalarFunctionArgs,
) -> Option<i32> {
    try_to_date_formatted_row(value, 0, format_args, invoke_args)
}

fn try_to_date_formatted_row(
    value: &str,
    row: usize,
    format_args: &[ColumnarValue],
    invoke_args: &ScalarFunctionArgs,
) -> Option<i32> {
    for format_arg in format_args {
        let Some(format) = format_value_at(format_arg, row).ok().flatten() else {
            continue;
        };
        if let Some(days) = try_to_date_with_to_date_func(value, format, invoke_args) {
            return Some(days);
        }
    }
    None
}

fn try_to_date_with_to_date_func(
    value: &str,
    format: &str,
    invoke_args: &ScalarFunctionArgs,
) -> Option<i32> {
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
        ],
        arg_fields: invoke_args.arg_fields.clone(),
        number_rows: 1,
        return_field: invoke_args.return_field.clone(),
        config_options: Arc::clone(&invoke_args.config_options),
    };
    match ToDateFunc::new().invoke_with_args(args) {
        Ok(ColumnarValue::Scalar(ScalarValue::Date32(days))) => days,
        _ => None,
    }
}
