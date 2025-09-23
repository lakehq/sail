use chrono::{DateTime, Days, NaiveDateTime, TimeZone, Utc};
use datafusion_common::{exec_datafusion_err, Result};

/// Localize the naive datetime to the time zone.
/// 1. If the datatime is mapped to exactly one datetime in the time zone, the local datetime
///    is returned.
/// 1. If the datetime is ambiguous in the time zone, the earliest local datetime is returned.
/// 1. If the datetime is invalid in the time zone, the next valid local datetime is returned.
pub fn localize_with_fallback<Tz: TimeZone>(
    tz: &Tz,
    datetime: &NaiveDateTime,
) -> Result<DateTime<Utc>> {
    let localize = |x: &NaiveDateTime| tz.from_local_datetime(x).map(|x| x.to_utc());
    match localize(datetime).earliest() {
        Some(x) => Ok(x),
        // FIXME: The logic here may not work in all cases.
        //   The correct solution requires access to offset transition rules for the time zone.
        None => datetime
            .checked_sub_days(Days::new(1))
            .and_then(|x| localize(&x).earliest())
            .and_then(|x| x.checked_add_days(Days::new(1)))
            .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}")),
    }
}
