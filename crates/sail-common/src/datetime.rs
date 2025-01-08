use chrono_tz::Tz;
use iana_time_zone::get_timezone;
use log::warn;

use crate::error::{CommonError, CommonResult};

pub fn get_system_timezone() -> CommonResult<String> {
    let timezone_str = get_timezone()
        .map_err(|e| CommonError::invalid(format!("Failed to get system timezone: {e}")))?;
    Ok(timezone_str)
}

pub fn warn_if_spark_session_timezone_mismatches_local_timezone(
    configured_session_timezone: &str,
    configured_system_timezone: &str,
) -> CommonResult<()> {
    let session_timezone: Tz = configured_session_timezone
        .parse()
        .map_err(|e| CommonError::invalid(format!("{e}")))?;
    let system_timezone: Tz = configured_system_timezone
        .parse()
        .map_err(|e| CommonError::invalid(format!("{e}")))?;
    if system_timezone != session_timezone {
        warn!(
            "Local server timezone does not match session timezone. \
            Local server timezone: {system_timezone:?}. \
            Configured session timezone: {configured_session_timezone:?}, \
            Parsed session timezone: {session_timezone:?}, \
            The Spark client applies the local client timezone to TimestampLtz values before \
            sending them to the server. If these timezones do not match, the server may rely on \
            the local server timezone, which may lead to unintended interpretation of values. \
            To avoid this issue, make sure the local timezone matches the session timezone."
        );
    }
    Ok(())
}
