use std::sync::Arc;

use arrow::array::{Array, Date32Array};
use arrow::datatypes::DataType;
use datafusion::functions::datetime::to_date::ToDateFunc;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignature;

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
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
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
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columnar_args = &args.args;
        if columnar_args.len() != 2 {
            return exec_err!(
                "try_to_date formatted function requires 2 arguments, got {}",
                columnar_args.len()
            );
        }

        validate_format_args(columnar_args, "try_to_date")?;
        self.try_to_date_formatted(columnar_args, &args)
    }
}

fn validate_format_args(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, arg) in args.iter().skip(1).enumerate() {
        match arg.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {}
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

fn string_value_at(arg: &ColumnarValue, row: usize) -> Result<Option<&str>> {
    match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let a = as_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            DataType::LargeUtf8 => {
                let a = as_large_string_array(array)?;
                Ok((!a.is_null(row)).then(|| a.value(row)))
            }
            DataType::Utf8View => {
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
    // Invoke DataFusion per row so one parse failure becomes one NULL, not a batch error.
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
