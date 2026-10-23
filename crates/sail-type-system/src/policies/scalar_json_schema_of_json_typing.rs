//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;

pub fn spark_schema_of_json_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

pub fn spark_schema_of_json_return_field_from_args(
    name: &str,
    _args: ReturnFieldArgs,
) -> Result<FieldRef> {
    // `schema_of_json` only accepts a foldable, non-null input and always
    // produces a DDL string, so the result is never null. Spark marks the
    // output of a successful foldable call as non-nullable.
    Ok(Arc::new(Field::new(name, DataType::Utf8, false)))
}
