//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_sequence_return_type(arg_types: &[DataType]) -> Result<DataType> {
    let start_type = match arg_types {
        [start, _] | [start, _, _] => start.clone(),
        _ => return invalid_sequence_arity(arg_types.len()),
    };

    Ok(DataType::List(Arc::new(Field::new_list_field(
        start_type, false,
    ))))
}

pub fn spark_sequence_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = spark_sequence_return_type(&arg_types)?;
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());

    Ok(Arc::new(Field::new(name, data_type, nullable)))
}

pub fn invalid_sequence_arity<T>(arity: usize) -> Result<T> {
    exec_err!("Spark `sequence` function requires 2 or 3 arguments, got {arity}")
}
