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

// Zones whose local microseconds equal UTC, including outside Chrono's date range.
pub fn is_fixed_zero_offset_timezone(timezone: &str) -> bool {
    matches!(
        timezone,
        "Z" | "UT" | "+00" | "-00" | "+0000" | "-0000" | "+00:00" | "-00:00"
    ) || matches!(
        timezone.strip_prefix("Etc/").unwrap_or(timezone),
        "GMT" | "GMT+0" | "GMT-0" | "GMT0" | "Greenwich" | "UCT" | "UTC" | "Universal" | "Zulu"
    )
}

// Convert an epoch day in the timestamp range to its proleptic Gregorian date.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = shifted_month + if shifted_month < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}
