//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, plan_err};

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Date32)
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err(
            "last_day",
            (1, 1),
            arg_types.len(),
        ));
    }

    // Spark implicitly casts Timestamp/Timestamp_NTZ to Date before
    // applying `last_day`. Arrow's `Timestamp → Date32` cast truncates
    // the time component, matching Spark's behaviour.
    let current_native_type: NativeType = (&arg_types[0]).into();
    if matches!(current_native_type, NativeType::Date)
        || matches!(current_native_type, NativeType::String)
        || matches!(current_native_type, NativeType::Null)
        || matches!(current_native_type, NativeType::Timestamp(_, _))
    {
        Ok(vec![DataType::Date32])
    } else {
        plan_err!(
            "The first argument of the Spark `last_day` function can only be a date, string or timestamp, but got {}",
            &arg_types[0]
        )
    }
}
