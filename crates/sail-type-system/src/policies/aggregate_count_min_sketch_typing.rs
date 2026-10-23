//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn count_min_sketch_function_return_type(arg_types: &[DataType]) -> Result<DataType> {
    validate_count_min_sketch_types(arg_types)?;
    Ok(DataType::Binary)
}

pub fn validate_count_min_sketch_types(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 4 {
        return Err(invalid_arg_count_exec_err(
            "count_min_sketch",
            (4, 4),
            arg_types.len(),
        ));
    }
    match &arg_types[0] {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        data_type => {
            return Err(unsupported_data_type_exec_err(
                "count_min_sketch",
                "TINYINT, SMALLINT, INT, BIGINT, STRING or BINARY",
                data_type,
            ));
        }
    }
    if !matches!(arg_types[1], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double eps argument, got {}",
            arg_types[1]
        );
    }
    if !matches!(arg_types[2], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double confidence argument, got {}",
            arg_types[2]
        );
    }
    if !matches!(arg_types[3], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "count_min_sketch requires an integer seed argument, got {}",
            arg_types[3]
        );
    }
    Ok(())
}
