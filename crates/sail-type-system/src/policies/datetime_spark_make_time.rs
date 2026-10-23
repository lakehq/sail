//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, exec_err, plan_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Time64(TimeUnit::Microsecond))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 3 {
        return exec_err!(
            "Spark `make_time` function requires 3 arguments, got {}",
            arg_types.len()
        );
    }

    let hour: NativeType = (&arg_types[0]).into();
    let minute: NativeType = (&arg_types[1]).into();
    let second: NativeType = (&arg_types[2]).into();

    if (hour.is_integer() || matches!(hour, NativeType::String | NativeType::Null))
        && (minute.is_integer() || matches!(minute, NativeType::String | NativeType::Null))
        && (second.is_numeric() || matches!(second, NativeType::String | NativeType::Null))
    {
        // Use Decimal128(16, 6) for seconds, matching Spark's DecimalType(16, 6).
        // This avoids f64 rounding errors (e.g. 59.9999999 rounding up to 60 seconds).
        Ok(vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Decimal128(SECONDS_PRECISION, SECONDS_SCALE),
        ])
    } else {
        plan_err!("The arguments of Spark `make_time` must be (integer, integer, numeric)")
    }
}

pub const SECONDS_PRECISION: u8 = 16;
pub const SECONDS_SCALE: i8 = 6;
