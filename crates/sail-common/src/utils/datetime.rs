use arrow::datatypes::TimeUnit;
use iana_time_zone::get_timezone;

use crate::error::{CommonError, CommonResult};

pub const MICROS_PER_SECOND: i64 = 1_000_000;

pub const MICROS_PER_MINUTE: i64 = 60 * 1_000_000;

pub const MICROS_PER_HOUR: i64 = 3_600 * 1_000_000;

pub const MICROS_PER_DAY: i64 = 24 * 3_600 * 1_000_000;

// The largest absolute offset that `SET TIME ZONE INTERVAL ...` accepts.
pub const MAX_TIME_ZONE_OFFSET_MICROS: u64 = (18 * MICROS_PER_HOUR) as u64;

pub fn get_system_timezone() -> CommonResult<String> {
    // TODO: This does not work in some Amazon Linux environments.
    get_timezone().map_err(|e| CommonError::invalid(format!("failed to get system time zone: {e}")))
}

pub const fn time_unit_to_multiplier(time_unit: &TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Second => 1i64,
        TimeUnit::Millisecond => 1000i64,
        TimeUnit::Microsecond => 1_000_000i64,
        TimeUnit::Nanosecond => 1_000_000_000i64,
    }
}
