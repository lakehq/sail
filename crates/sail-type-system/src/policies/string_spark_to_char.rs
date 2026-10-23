//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let func_name = "spark_to_char";
    if arg_types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            func_name,
            (2, 2),
            arg_types.len(),
        ));
    }
    // Mirror Spark's implicit cast of the input to `DecimalType`:
    // integers convert exactly, floats keep Spark's `DecimalType.forType` semantics
    // (handled natively at runtime to match the JVM's shortest decimal representation),
    // and strings are parsed as `DecimalType.SYSTEM_DEFAULT`, i.e. DECIMAL(38, 18).
    let value_type = match &arg_types[0] {
        DataType::Null => DataType::Null,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::UInt64,
        DataType::Float16 | DataType::Float32 => DataType::Float32,
        DataType::Float64 => DataType::Float64,
        DataType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
        DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        other => {
            return Err(unsupported_data_type_exec_err(
                func_name,
                "NUMERIC or STRING",
                other,
            ));
        }
    };
    match &arg_types[1] {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
        other => {
            return Err(unsupported_data_type_exec_err(func_name, "STRING", other));
        }
    }
    Ok(vec![value_type, DataType::Utf8])
}
