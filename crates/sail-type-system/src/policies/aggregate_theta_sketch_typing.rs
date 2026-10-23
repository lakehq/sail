//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

pub fn theta_sketch_agg_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_theta_sketch_agg_types("theta_sketch_agg", arg_types)?;
    Ok(DataType::Binary)
}

pub fn theta_union_agg_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_binary_agg_types("theta_union_agg", arg_types, true)?;
    Ok(DataType::Binary)
}

pub fn theta_intersection_agg_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_binary_agg_types("theta_intersection_agg", arg_types, false)?;
    Ok(DataType::Binary)
}

pub fn validate_theta_sketch_agg_types(function_name: &str, arg_types: &[DataType]) -> Result<()> {
    if !(1..=2).contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} requires 1 or 2 arguments, got {}",
            arg_types.len()
        );
    }
    match &arg_types[0] {
        DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        DataType::List(field) | DataType::LargeList(field)
            if matches!(field.data_type(), DataType::Int32 | DataType::Int64) => {}
        data_type => {
            return exec_err!("{function_name} does not support input type {data_type}");
        }
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}

pub fn validate_binary_agg_types(
    function_name: &str,
    arg_types: &[DataType],
    has_lg_nom_entries: bool,
) -> Result<()> {
    let expected = if has_lg_nom_entries { 1..=2 } else { 1..=1 };
    if !expected.contains(&arg_types.len()) {
        return exec_err!(
            "{function_name} received invalid argument count: {}",
            arg_types.len()
        );
    }
    if !matches!(arg_types[0], DataType::Binary | DataType::Null) {
        return exec_err!(
            "{function_name} requires a binary sketch argument, got {}",
            arg_types[0]
        );
    }
    if arg_types.len() == 2 && !matches!(arg_types[1], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "{function_name} requires an integer lgNomEntries argument, got {}",
            arg_types[1]
        );
    }
    Ok(())
}
