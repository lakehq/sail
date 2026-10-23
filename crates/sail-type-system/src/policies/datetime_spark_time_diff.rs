//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::{Result, exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int64)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 3 {
        return exec_err!(
            "Spark `time_diff` function requires 3 arguments, got {}",
            arg_types.len()
        );
    }
    Ok(vec![
        DataType::Utf8,
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Microsecond),
    ])
}
