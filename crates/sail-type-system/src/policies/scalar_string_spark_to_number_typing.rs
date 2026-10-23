//! Shared function type policies.
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::number_format::{NumberComponents, RegexSpec};

pub fn spark_to_number_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_to_number_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let ReturnFieldArgs {
        scalar_arguments, ..
    } = args;
    let format: &str = match scalar_arguments.get(1) {
        Some(Some(format)) => match format {
            ScalarValue::Utf8(Some(format))
            | ScalarValue::LargeUtf8(Some(format))
            | ScalarValue::Utf8View(Some(format)) => Ok(format.as_str()),
            // NULL format: safe mode returns all-NULL, strict mode errors at runtime
            ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::Null => {
                return Ok(Arc::new(Field::new(
                    name,
                    DataType::Decimal256(38, 18),
                    true,
                )));
            }
            _ => internal_err!("Expected UTF-8 format string"),
        }?,
        Some(None) | None => {
            return Ok(Arc::new(Field::new(
                name,
                DataType::Decimal256(38, 18),
                true,
            )));
        }
    };

    let format_spec: RegexSpec = RegexSpec::try_from(format)?;
    let NumberComponents {
        precision, scale, ..
    } = NumberComponents::try_from(&format_spec)?;
    let return_type = DataType::Decimal256(precision, scale);
    Ok(Arc::new(Field::new(name, return_type, true)))
}

pub fn spark_to_number_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let func_name = name;
    if arg_types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            func_name,
            (2, 2),
            arg_types.len(),
        ));
    }
    for arg_type in arg_types {
        match arg_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
            _ => {
                return Err(unsupported_data_type_exec_err(
                    func_name, "STRING", arg_type,
                ));
            }
        }
    }
    Ok(vec![DataType::Utf8, DataType::Utf8])
}
