//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int32)
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(
        "spark_cast_string_to_int32",
        DataType::Int32,
        true,
    )))
}
