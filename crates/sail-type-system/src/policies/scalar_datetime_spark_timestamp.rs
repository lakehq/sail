//! Configured function type policy.

use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::Result;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn return_type(timezone: &Option<Arc<str>>, _arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
}

pub fn coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if !matches!(arg_types.len(), 1 | 2) {
        return Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()));
    }
    match &arg_types[0] {
        // String-only, matching the kernel (which parses strings) and the
        // sibling parsers `SparkDate`/`SparkTime`. The planner casts/handles
        // DATE/TIMESTAMP inputs directly, so they never reach this UDF.
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
        other => {
            return Err(unsupported_data_type_exec_err(
                name,
                "STRING or NULL",
                other,
            ));
        }
    }
    let mut coerced = arg_types.to_vec();
    if let Some(format) = arg_types.get(1) {
        match format {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
            // A NULL format yields a NULL result; coerce it to a Utf8 null.
            DataType::Null => coerced[1] = DataType::Utf8,
            other => {
                return Err(unsupported_data_type_exec_err(name, "STRING", other));
            }
        }
    }
    Ok(coerced)
}
