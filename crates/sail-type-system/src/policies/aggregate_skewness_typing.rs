//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::Result;

pub fn skewness_func_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Float64)
}
