//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!(
        "{}: `return_type` should not be called; `return_field_from_args` is used instead",
        "spark_concat_ws"
    )
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let nullable = args.arg_fields.first().is_none_or(|sep| sep.is_nullable());
    Ok(Arc::new(Field::new(
        "spark_concat_ws",
        DataType::Utf8,
        nullable,
    )))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    // Spark errors on `concat_ws()` with no separator (WRONG_NUM_ARGS).
    if arg_types.is_empty() {
        return Err(invalid_arg_count_exec_err("concat_ws", (1, i32::MAX), 0));
    }
    arg_types
        .iter()
        .map(|arg_type| match arg_type {
            DataType::Null => Ok(DataType::Null),
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::FixedSizeList(field, _) => Ok(DataType::List(Arc::clone(field))),
            DataType::LargeList(field) | DataType::LargeListView(field) => {
                Ok(DataType::LargeList(Arc::clone(field)))
            }
            // Spark casts any other scalar (numbers, booleans, dates, binary, …) to STRING.
            other => {
                if other.is_nested() {
                    Ok(other.clone())
                } else {
                    Ok(DataType::Utf8)
                }
            }
        })
        .collect()
}
