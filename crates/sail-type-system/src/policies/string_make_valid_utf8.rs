//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_type = match args.arg_fields.first().map(|f| f.data_type()) {
        Some(data_type) => match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => data_type.clone(),
            DataType::Binary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
                DataType::Utf8
            }
            DataType::LargeBinary => DataType::LargeUtf8,
            _ => return exec_err!("expected string array for `make_valid_utf8`"),
        },
        None => return exec_err!("expected single argument for `make_valid_utf8`"),
    };
    Ok(Arc::new(Field::new("make_valid_utf8", data_type, true)))
}
