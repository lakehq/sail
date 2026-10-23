//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::policies::json_common_typing::return_type_check;

pub fn json_as_text_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    return_type_check(arg_types, name, DataType::Utf8)
}
