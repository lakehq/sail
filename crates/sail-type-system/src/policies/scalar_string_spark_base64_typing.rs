//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

pub fn spark_base64_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 1 {
        return plan_err!("{} expects 1 argument, but got {}", name, arg_types.len());
    }
    match arg_types[0] {
        DataType::Utf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Ok(DataType::Utf8),
        DataType::LargeUtf8 | DataType::LargeBinary => Ok(DataType::LargeUtf8),
        DataType::Null => Ok(DataType::Utf8),
        _ => plan_err!(
            "1st argument should be String or Binary, got {}",
            arg_types[0]
        ),
    }
}

pub fn spark_unbase64_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    let [arg_type] = arg_types else {
        return plan_err!("{} expects 1 argument, but got {}", name, arg_types.len());
    };
    match arg_type {
        DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
        DataType::LargeUtf8 => Ok(DataType::LargeBinary),
        DataType::Null => Ok(DataType::Binary),
        _ => plan_err!("1st argument should be String, got {}", arg_types[0]),
    }
}
