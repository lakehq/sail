//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_type = match args.arg_fields.first().map(|f| f.data_type()) {
        Some(DataType::LargeUtf8) => DataType::LargeUtf8,
        _ => DataType::Utf8,
    };
    Ok(Arc::new(Field::new("quote", data_type, true)))
}
