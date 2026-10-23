//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn array_min_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn array_min_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_type =
        element_return_type("ArrayMin", args.arg_fields.first().map(|f| f.data_type()))?;
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn array_max_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn array_max_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_type =
        element_return_type("ArrayMax", args.arg_fields.first().map(|f| f.data_type()))?;
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn element_return_type(name: &str, data_type: Option<&DataType>) -> Result<DataType> {
    match data_type {
        Some(DataType::List(field) | DataType::LargeList(field)) => Ok(field.data_type().clone()),
        Some(DataType::Null) => Ok(DataType::Null),
        _ => plan_err!("{name} can only accept List or LargeList."),
    }
}
