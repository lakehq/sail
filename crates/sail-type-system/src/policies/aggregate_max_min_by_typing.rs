//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::DataFusionError;

pub fn max_by_function_return_type(arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
    Ok(arg_types[0].to_owned())
}

pub fn max_by_function_coerce_types(
    arg_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    get_min_max_by_result_type(arg_types)
}

pub fn min_by_function_return_type(arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
    Ok(arg_types[0].to_owned())
}

pub fn min_by_function_coerce_types(
    arg_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    get_min_max_by_result_type(arg_types)
}

pub fn get_min_max_by_result_type(
    input_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.as_ref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}
