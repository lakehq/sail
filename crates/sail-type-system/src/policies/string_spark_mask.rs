//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    if arg_types.is_empty() || arg_types.len() > 5 {
        return exec_err!(
            "Spark `mask` function requires 1 to 5 arguments, got {}",
            arg_types.len()
        );
    }
    let data_type = match arg_types[0] {
        DataType::Utf8 | DataType::Utf8View | DataType::Null => DataType::Utf8,
        DataType::LargeUtf8 => DataType::LargeUtf8,
        _ => {
            return exec_err!(
                "Spark `mask` function: first arg must be string, got {}",
                arg_types[0]
            );
        }
    };
    Ok(Arc::new(Field::new("spark_mask", data_type, true)))
}
