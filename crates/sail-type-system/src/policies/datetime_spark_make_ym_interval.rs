//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::{DataType, IntervalUnit};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, plan_err};

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Interval(IntervalUnit::YearMonth))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() > 2 {
        return Err(invalid_arg_count_exec_err(
            "make_ym_interval",
            (0, 2),
            arg_types.len(),
        ));
    }
    for arg_type in arg_types {
        let native: NativeType = arg_type.into();
        if !(native.is_integer() || matches!(native, NativeType::String | NativeType::Null)) {
            return plan_err!(
                "The arguments of Spark `make_ym_interval` must be integer or string or null"
            );
        }
    }
    Ok(vec![DataType::Int32; arg_types.len()])
}
