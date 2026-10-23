//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, IntervalUnit};
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::ReturnFieldArgs;

pub fn year_month_interval_months_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int32)
}

pub fn year_month_interval_months_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let [field] = take_function_args(name, args.arg_fields)?;
    // The numeric output must not retain interval qualifier metadata.
    Ok(Arc::new(Field::new(
        name,
        DataType::Int32,
        field.is_nullable(),
    )))
}

pub fn spark_day_time_interval_to_calendar_interval_return_type(
    _arg_types: &[DataType],
) -> Result<DataType> {
    Ok(DataType::Interval(IntervalUnit::MonthDayNano))
}
