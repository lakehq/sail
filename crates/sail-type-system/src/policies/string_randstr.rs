//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.is_empty() || arg_types.len() > 2 {
        return exec_err!(
            "`randstr` requires 1 or 2 arguments, got {}",
            arg_types.len()
        );
    }

    let mut result = Vec::with_capacity(arg_types.len());

    if arg_types[0].is_integer() {
        result.push(DataType::Int32);
    } else {
        return exec_err!(
            "`randstr` expects integer type for length, got {:?}",
            arg_types[0]
        );
    }

    if arg_types.len() == 2 {
        if arg_types[1].is_integer() || arg_types[1].is_null() {
            result.push(DataType::Int64);
        } else {
            return exec_err!(
                "`randstr` expects integer type for seed, got {:?}",
                arg_types[1]
            );
        }
    }

    Ok(result)
}
