//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

const BIN_SUPPORTED_TYPES: &str = "numeric or string";

pub fn spark_bin_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

pub fn spark_bin_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err("bin", (1, 1), arg_types.len()));
    }
    // Spark's `bin` accepts numeric and string types only. Anything else
    // (BOOLEAN, BINARY, DATE, TIMESTAMP, ARRAY, MAP, STRUCT, …) is rejected
    // at analysis time with DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE.
    match &arg_types[0] {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(vec![arg_types[0].clone()]),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Null => Ok(vec![DataType::Int64]),
        // Keep floats as DOUBLE: DataFusion's Float->Int64 cast errors on
        // NaN/Infinity, so `bin` applies Spark's saturating cast itself.
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(vec![DataType::Float64]),
        other => Err(unsupported_data_type_exec_err(
            "bin",
            BIN_SUPPORTED_TYPES,
            other,
        )),
    }
}
