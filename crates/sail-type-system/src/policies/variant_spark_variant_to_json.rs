//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8View)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.is_empty() || arg_types.len() > 2 {
        return Err(invalid_arg_count_exec_err(
            "variant_to_json",
            (1, 2),
            arg_types.len(),
        ));
    }

    // Accept the variant type as-is (it's a Struct with extension type)
    // If there's a second argument (options MAP), accept it as-is too
    Ok(arg_types.to_vec())
}
