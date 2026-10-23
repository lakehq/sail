//! Configured function type policy.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int64)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if !matches!(arg_types.len(), 1 | 2) {
        return Err(invalid_arg_count_exec_err(
            "spark_unix_timestamp",
            (1, 2),
            arg_types.len(),
        ));
    }
    match &arg_types[0] {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Null => {}
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_unix_timestamp",
                "STRING, DATE, TIMESTAMP or NULL",
                other,
            ));
        }
    }

    let mut coerced = arg_types.to_vec();
    if let Some(format) = arg_types.get(1) {
        match format {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
            DataType::Null => coerced[1] = DataType::Utf8,
            other => {
                return Err(unsupported_data_type_exec_err(
                    "spark_unix_timestamp",
                    "STRING",
                    other,
                ));
            }
        }
    }
    Ok(coerced)
}

pub fn return_field_from_args(ansi_mode: bool, args: ReturnFieldArgs) -> Result<FieldRef> {
    let nullable = !ansi_mode || args.arg_fields.iter().any(|field| field.is_nullable());
    Ok(Arc::new(Field::new(
        "spark_unix_timestamp",
        DataType::Int64,
        nullable,
    )))
}
