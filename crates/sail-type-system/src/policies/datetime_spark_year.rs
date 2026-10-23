//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, plan_err};

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int32)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err("year", (1, 1), arg_types.len()));
    }
    let native: NativeType = (&arg_types[0]).into();
    if matches!(
        native,
        NativeType::Date | NativeType::String | NativeType::Null | NativeType::Timestamp(_, _)
    ) {
        Ok(vec![DataType::Date32])
    } else {
        plan_err!(
            "The argument of the Spark `year` function can only be a date, string or timestamp, but got {}",
            &arg_types[0]
        )
    }
}
