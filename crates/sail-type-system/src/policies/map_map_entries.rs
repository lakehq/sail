//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let [map_field] = args.arg_fields else {
        return exec_err!("map_entries: expected one argument");
    };

    let DataType::Map(entries_field, _) = map_field.data_type() else {
        return exec_err!(
            "map_entries: expected map argument, got {:?}",
            map_field.data_type()
        );
    };

    Ok(Arc::new(Field::new(
        "map_entries",
        DataType::List(Arc::new(Field::new_list_field(
            entries_field.data_type().clone(),
            entries_field.is_nullable(),
        ))),
        map_field.is_nullable(),
    )))
}
