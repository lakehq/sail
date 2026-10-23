//! Shared function type policies.
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_to_csv_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_to_csv_return_field_from_args(name: &str, _args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, DataType::Utf8, true)))
}
