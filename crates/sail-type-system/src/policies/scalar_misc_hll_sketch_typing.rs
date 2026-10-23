//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

pub fn hll_sketch_estimate_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 1 {
        return exec_err!(
            "hll_sketch_estimate requires 1 argument, got {}",
            arg_types.len()
        );
    }
    match arg_types[0] {
        DataType::Binary | DataType::Null => Ok(DataType::Int64),
        ref data_type => {
            exec_err!("hll_sketch_estimate requires a binary argument, got {data_type}")
        }
    }
}

pub fn hll_union_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_hll_union_types("hll_union", arg_types)?;
    Ok(DataType::Binary)
}

pub fn validate_hll_union_types(function_name: &str, arg_types: &[DataType]) -> Result<()> {
    if !(2..=3).contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} received invalid argument count: {}",
            arg_types.len()
        );
    }
    for data_type in &arg_types[0..2] {
        if !matches!(data_type, DataType::Binary | DataType::Null) {
            return exec_err!("{function_name} requires binary sketch arguments, got {data_type}");
        }
    }
    if arg_types.len() == 3 && !matches!(arg_types[2], DataType::Boolean | DataType::Null) {
        return exec_err!(
            "{function_name} requires a boolean allowDifferentLgConfigK argument, got {}",
            arg_types[2]
        );
    }
    Ok(())
}
