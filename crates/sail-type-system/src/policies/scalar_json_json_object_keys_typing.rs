//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

use crate::policies::json_common_typing::return_type_check;

pub fn json_object_keys_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    return_type_check(
        arg_types,
        name,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    )
}
