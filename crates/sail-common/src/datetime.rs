use arrow::datatypes::TimeUnit;
use iana_time_zone::get_timezone;

use crate::error::{CommonError, CommonResult};

pub fn get_system_timezone() -> CommonResult<String> {
    // TODO: This does not work in some Amazon Linux environments.
    get_timezone().map_err(|e| CommonError::invalid(format!("failed to get system time zone: {e}")))
}

pub fn time_unit_to_multiplier(time_unit: &TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Second => 1i64,
        TimeUnit::Millisecond => 1000i64,
        TimeUnit::Microsecond => 1_000_000i64,
        TimeUnit::Nanosecond => 1_000_000_000i64,
    }
}
