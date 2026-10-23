//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, exec_err, plan_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 2 {
        return Ok(vec![
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
        ]);
    }

    if let [years, months, days, hours, mins, secs] = arg_types {
        let years: NativeType = years.into();
        let months: NativeType = months.into();
        let days: NativeType = days.into();
        let hours: NativeType = hours.into();
        let mins: NativeType = mins.into();
        let secs: NativeType = secs.into();
        if (years.is_integer()
            || matches!(years, NativeType::String)
            || matches!(years, NativeType::Null))
            && (months.is_integer()
                || matches!(months, NativeType::String)
                || matches!(months, NativeType::Null))
            && (days.is_integer()
                || matches!(days, NativeType::String)
                || matches!(days, NativeType::Null))
            && (hours.is_integer()
                || matches!(hours, NativeType::String)
                || matches!(hours, NativeType::Null))
            && (mins.is_integer()
                || matches!(mins, NativeType::String)
                || matches!(mins, NativeType::Null))
            && (secs.is_numeric()
                || matches!(secs, NativeType::String)
                || matches!(secs, NativeType::Null))
        {
            return Ok(vec![
                DataType::Int32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::Float64,
            ]);
        } else {
            return plan_err!(
                "The arguments of Spark `make_timestamp_ntz` must be integer or string or null"
            );
        }
    }

    exec_err!(
        "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
        arg_types.len()
    )
}
