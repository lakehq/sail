//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.first() {
        Some(DataType::Utf8) => Ok(DataType::Utf8),
        Some(DataType::Utf8View) => Ok(DataType::Utf8View),
        Some(DataType::LargeUtf8) => Ok(DataType::LargeUtf8),
        _ => Ok(DataType::Utf8),
    }
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
    let [input_type, from_base_type, to_base_type] = types else {
        return Err(invalid_arg_count_exec_err(
            "spark_conv",
            (3, 3),
            types.len(),
        ));
    };

    let valid_string: bool = matches!(
        input_type,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Int32
    );
    let valid_from: bool = matches!(from_base_type, DataType::Int32);
    let valid_to: bool = matches!(to_base_type, DataType::Int32);

    if valid_string && valid_from && valid_to {
        Ok(vec![
            input_type.clone(),
            from_base_type.clone(),
            to_base_type.clone(),
        ])
    } else {
        Err(unsupported_data_types_exec_err(
            "spark_conv",
            "Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32",
            types,
        ))
    }
}
