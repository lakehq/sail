//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(
        "spark_next_day",
        DataType::Date32,
        true,
    )))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            "next_day",
            (2, 2),
            arg_types.len(),
        ));
    }

    // Spark implicitly casts Timestamp/Timestamp_NTZ to Date before
    // applying `next_day`. Arrow's `Timestamp → Date32` cast truncates
    // the time component, matching Spark's behaviour.
    let current_native_type: NativeType = (&arg_types[0]).into();
    if matches!(current_native_type, NativeType::Date)
        || matches!(current_native_type, NativeType::String)
        || matches!(current_native_type, NativeType::Null)
        || matches!(current_native_type, NativeType::Timestamp(_, _))
    {
        if matches!(
            &arg_types[1],
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            let day_type = if arg_types[1] == DataType::Null {
                DataType::Utf8
            } else {
                arg_types[1].clone()
            };
            Ok(vec![DataType::Date32, day_type])
        } else {
            plan_err!(
                "The second argument of the Spark `next_day` function must be a string, but got {}",
                &arg_types[1]
            )
        }
    } else {
        plan_err!(
            "The first argument of the Spark `next_day` function can only be a date, string or timestamp, but got {}",
            &arg_types[0]
        )
    }
}
