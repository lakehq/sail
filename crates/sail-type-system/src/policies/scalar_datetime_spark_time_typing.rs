//! Shared function type policies.

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn spark_time_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Time64(TimeUnit::Microsecond))
}

pub fn spark_time_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if !matches!(arg_types.len(), 1 | 2) {
        return Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()));
    }
    match &arg_types[0] {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Null => {}
        other => {
            return Err(unsupported_data_type_exec_err(
                name,
                "STRING, TIME, TIMESTAMP or NULL",
                other,
            ));
        }
    }
    let mut coerced = arg_types.to_vec();
    if let Some(format) = arg_types.get(1) {
        match format {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
            // A NULL format yields a NULL result; coerce it to a Utf8 null so
            // the kernel's per-row None handling produces NULL instead of
            // erroring on the `Null` type.
            DataType::Null => coerced[1] = DataType::Utf8,
            other => {
                return Err(unsupported_data_type_exec_err(name, "STRING", other));
            }
        }
    }
    Ok(coerced)
}
