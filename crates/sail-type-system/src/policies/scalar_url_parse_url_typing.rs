//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn parse_url_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn parse_url_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = infer_url_type(name, &arg_types)?;
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn parse_url_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types.len() {
        2 | 3
            if arg_types
                .iter()
                .all(|dt| is_string_type(dt) || dt == &DataType::Null) =>
        {
            Ok(arg_types
                .iter()
                .map(|dt| {
                    if matches!(dt, DataType::Null) {
                        DataType::Utf8
                    } else {
                        dt.clone()
                    }
                })
                .collect())
        }
        2 | 3 => plan_err!("`{}` expects STRING arguments, got {:?}", &name, arg_types),
        _ => plan_err!(
            "`{}` expects 2 or 3 arguments, got {}",
            &name,
            arg_types.len()
        ),
    }
}

pub fn infer_url_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() < 2 || arg_types.len() > 3 {
        return plan_err!(
            "{name} expects 2 or 3 arguments, but got {}",
            arg_types.len()
        );
    }
    // The return type should match the largest size datatype
    match arg_types.len() {
        2 | 3 if arg_types.iter().all(is_string_type) => {
            if arg_types
                .iter()
                .any(|arg| matches!(arg, DataType::LargeUtf8))
            {
                Ok(DataType::LargeUtf8)
            } else {
                Ok(DataType::Utf8)
            }
        }
        2 | 3 => plan_err!("`{name}` expects STRING arguments, got {arg_types:?}"),
        _ => plan_err!("`{name}` expects 2 or 3 arguments, got {}", arg_types.len()),
    }
}

pub fn is_string_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}
