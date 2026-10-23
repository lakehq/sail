//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(
        "xpath",
        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
        true,
    )))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return plan_err!(
            "`xpath` function requires 2 arguments, got {}",
            arg_types.len()
        );
    }
    arg_types
        .iter()
        .map(|data_type| match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {
                Ok(DataType::Utf8)
            }
            other => {
                plan_err!("The `xpath` function can only accept strings, but got {other:?}.")
            }
        })
        .collect()
}
