//! Configured function type policy.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    datafusion_common::internal_err!(
        "{}: `return_type` should not be called; `return_field_from_args` is used instead",
        "struct"
    )
}

pub fn return_field_from_args(field_names: &[String], args: ReturnFieldArgs) -> Result<FieldRef> {
    let fields: Vec<Field> = args
        .arg_fields
        .iter()
        .zip(field_names.iter())
        .map(|(arg_field, field_name)| {
            Field::new(
                field_name.clone(),
                arg_field.data_type().clone(),
                arg_field.is_nullable(),
            )
            .with_metadata(arg_field.metadata().clone())
        })
        .collect();

    // The struct value itself is never NULL — only child fields may be.
    // StructArray::from() creates no null bitmap, and Spark's struct()
    // always returns non-nullable.
    let struct_type = DataType::Struct(Fields::from(fields));
    Ok(Arc::new(Field::new("struct", struct_type, false)))
}
