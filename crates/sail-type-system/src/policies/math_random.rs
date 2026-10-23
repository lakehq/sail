//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Float64)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.is_empty() {
        Ok(vec![])
    } else if arg_types.len() == 1 {
        if arg_types[0].is_signed_integer() {
            Ok(vec![DataType::Int64])
        } else if arg_types[0].is_unsigned_integer() {
            Ok(vec![DataType::UInt64])
        } else if arg_types[0].is_null() {
            Ok(vec![DataType::Null])
        } else {
            Err(unsupported_data_types_exec_err(
                "random",
                "Integer Type for seed",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(
            "random",
            (0, 1),
            arg_types.len(),
        ))
    }
}
