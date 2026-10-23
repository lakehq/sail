//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use super::scalar_url_parse_url_typing::infer_url_type;

pub fn spark_try_parse_url_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_try_parse_url_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = infer_url_type(name, &arg_types)?;
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn spark_try_parse_url_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    super::scalar_url_parse_url_typing::parse_url_coerce_types("parse_url", arg_types)
}
