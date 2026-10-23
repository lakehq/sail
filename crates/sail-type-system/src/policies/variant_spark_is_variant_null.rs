//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Boolean)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err(
            "is_variant_null",
            (1, 1),
            arg_types.len(),
        ));
    }

    // Accept the variant type as-is (it's a Struct with extension type)
    Ok(vec![arg_types[0].clone()])
}
