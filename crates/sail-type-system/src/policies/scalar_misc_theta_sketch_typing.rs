//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

pub fn theta_sketch_estimate_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 1 {
        return exec_err!(
            "theta_sketch_estimate requires 1 argument, got {}",
            arg_types.len()
        );
    }
    match arg_types[0] {
        DataType::Binary | DataType::Null => Ok(DataType::Int64),
        ref data_type => {
            exec_err!("theta_sketch_estimate requires a binary argument, got {data_type}")
        }
    }
}

pub fn theta_union_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_binary_set_op_types("theta_union", arg_types, true)?;
    Ok(DataType::Binary)
}

pub fn theta_intersection_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_binary_set_op_types("theta_intersection", arg_types, false)?;
    Ok(DataType::Binary)
}

pub fn theta_difference_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_binary_set_op_types("theta_difference", arg_types, false)?;
    Ok(DataType::Binary)
}

pub fn validate_binary_set_op_types(
    function_name: &str,
    arg_types: &[DataType],
    has_lg_nom_entries: bool,
) -> Result<()> {
    let expected = if has_lg_nom_entries { 2..=3 } else { 2..=2 };
    if !expected.contains(&arg_types.len()) {
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
    if arg_types.len() == 3 && !matches!(arg_types[2], DataType::Int32 | DataType::Null) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[2]
        );
    }
    Ok(())
}
