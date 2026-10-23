//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::policies::map_typing::map_type_from_key_value_types;

pub fn str_to_map_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(map_type_from_key_value_types(
        &DataType::Utf8,
        &DataType::Utf8,
    ))
}
