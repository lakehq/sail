//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int64)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        let lambda_type = if arg_types[0].is_floating() {
            Ok(DataType::Float64)
        } else {
            Err(unsupported_data_types_exec_err(
                "random_poisson",
                "Floating Type for lambda",
                arg_types,
            ))
        }?;
        Ok(vec![lambda_type])
    } else if arg_types.len() == 2 {
        if arg_types[0].is_floating() && arg_types[1].is_numeric() {
            let seed_type = if arg_types[1].is_signed_integer() {
                Ok(DataType::Int64)
            } else if arg_types[1].is_unsigned_integer() {
                Ok(DataType::UInt64)
            } else if arg_types[1].is_null() {
                Ok(DataType::Null)
            } else {
                Err(unsupported_data_types_exec_err(
                    "random_poisson",
                    "Integer Type for seed",
                    arg_types,
                ))
            }?;
            let lambda_type = if arg_types[0].is_floating() {
                Ok(DataType::Float64)
            } else {
                Err(unsupported_data_types_exec_err(
                    "random_poisson",
                    "Floating Type for lambda",
                    arg_types,
                ))
            }?;
            Ok(vec![lambda_type, seed_type])
        } else {
            Err(unsupported_data_types_exec_err(
                "random_poisson",
                "Integer Type for seed",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(
            "random_poisson",
            (1, 2),
            arg_types.len(),
        ))
    }
}
