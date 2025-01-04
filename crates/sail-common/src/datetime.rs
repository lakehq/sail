use chrono::{DateTime, FixedOffset, Local, Offset, TimeZone};
use chrono_tz::Tz;
use log::warn;

use crate::error::{CommonError, CommonResult};

pub fn get_local_datetime_offset() -> FixedOffset {
    // CHECK HERE BEFORE MERGING IN
    // There is a bug in the spark client. Commented out code should be the actual log,
    // but spark client uses the current timezone offset to adjust the timestamp which does not account for DST
    //  let local_datetime: DateTime<Local> = datetime.with_timezone(&Local);
    //  let local_offset: FixedOffset = local_datetime.offset().fix();
    let local_datetime: DateTime<Local> = Local::now();
    local_datetime.offset().fix()
}

pub fn warn_if_spark_session_timezone_mismatches_local_timezone(
    session_timezone: &str,
) -> CommonResult<()> {
    let local_datetime: DateTime<Local> = Local::now();
    let local_offset: FixedOffset = local_datetime.offset().fix();
    let session_tz: Tz = session_timezone.parse().map_err(|_| {
        CommonError::invalid(format!("Invalid configured timezone: {session_timezone}"))
    })?;
    let session_tz_offset: FixedOffset = session_tz
        .offset_from_utc_datetime(&local_datetime.naive_utc())
        .fix();
    if session_tz_offset != local_offset {
        warn!(
            "Local timezone does not match session timezone. \
            Raw Session timezone: {session_timezone}, \
            Parsed Session timezone: {session_tz}, \
            Session timezone offset: {session_tz_offset}, and \
            Local timezone offset: {local_offset}. \
            The Spark client applies the local client timezone to TimestampLtz values before \
            sending them to the server. If these timezones do not match, the server may rely on \
            the local server timezone, which may lead to unintended interpretation of values. \
            To avoid this issue, make sure the local timezone matches the session timezone."
        );
    }
    Ok(())
}
