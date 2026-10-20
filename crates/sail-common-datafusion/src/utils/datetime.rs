use std::fmt::Display;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Offset, TimeZone, Timelike, Utc};
use datafusion_common::{Result, exec_datafusion_err};
use jiff::tz::AmbiguousOffset;
use sail_common::utils::datetime::civil_from_days;

fn timezone<Tz: TimeZone + Display>(tz: &Tz) -> Result<jiff::tz::TimeZone> {
    let name = tz.to_string();
    if name.starts_with(['+', '-']) {
        let offset = tz
            .offset_from_utc_datetime(&DateTime::UNIX_EPOCH.naive_utc())
            .fix()
            .local_minus_utc();
        return jiff::tz::Offset::from_seconds(offset)
            .map(jiff::tz::TimeZone::fixed)
            .map_err(|error| exec_datafusion_err!("{error}"));
    }
    jiff::tz::TimeZoneDatabase::bundled()
        .get(&name)
        .map_err(|error| exec_datafusion_err!("{error}"))
}

fn lookup_year(year: i64) -> i16 {
    // Outside Jiff's calendar, initial historical offsets are constant and future rules repeat.
    if year < -9999 {
        (-9600 + year.rem_euclid(400)) as i16
    } else if year > 9999 {
        (2400 + year.rem_euclid(400)) as i16
    } else {
        year as i16
    }
}

fn local_lookup(datetime: &NaiveDateTime) -> Result<jiff::civil::DateTime> {
    // Chrono's extra nanoseconds for a leap second do not change its offset lookup.
    jiff::civil::DateTime::new(
        lookup_year(i64::from(datetime.year())),
        datetime.month() as i8,
        datetime.day() as i8,
        datetime.hour() as i8,
        datetime.minute() as i8,
        datetime.second() as i8,
        datetime.nanosecond().min(999_999_999) as i32,
    )
    .map_err(|error| exec_datafusion_err!("{error}"))
}

pub fn timezone_offset<Tz: TimeZone + Display>(tz: &Tz, seconds: i64) -> Result<FixedOffset> {
    let instant = match jiff::Timestamp::from_second(seconds) {
        Ok(instant) => instant,
        Err(_) => {
            let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
            let time = seconds.rem_euclid(86_400);
            // Jiff's first and final civil days can also exceed its instant range.
            let year = if year >= 9999 {
                (2400 + year.rem_euclid(400)) as i16
            } else if year <= -9999 {
                (-9600 + year.rem_euclid(400)) as i16
            } else {
                lookup_year(year)
            };
            let datetime = jiff::civil::DateTime::new(
                year,
                month as i8,
                day as i8,
                (time / 3600) as i8,
                (time / 60 % 60) as i8,
                (time % 60) as i8,
                0,
            )
            .map_err(|error| exec_datafusion_err!("{error}"))?;
            jiff::tz::Offset::UTC
                .to_timestamp(datetime)
                .map_err(|error| exec_datafusion_err!("{error}"))?
        }
    };
    FixedOffset::east_opt(timezone(tz)?.to_offset(instant).seconds())
        .ok_or_else(|| exec_datafusion_err!("invalid time zone offset"))
}

pub fn to_local_datetime<Tz: TimeZone + Display>(
    tz: &Tz,
    datetime: &DateTime<Utc>,
) -> Result<DateTime<FixedOffset>> {
    let offset = timezone_offset(tz, datetime.timestamp())?;
    datetime
        .naive_utc()
        .checked_add_offset(offset)
        .ok_or_else(|| exec_datafusion_err!("local datetime out of range: {datetime}"))?;
    Ok(datetime.with_timezone(&offset))
}

pub fn local_timezone_offsets<Tz: TimeZone + Display>(
    tz: &Tz,
    datetime: &NaiveDateTime,
) -> Result<AmbiguousOffset> {
    Ok(timezone(tz)?
        .to_ambiguous_timestamp(local_lookup(datetime)?)
        .offset())
}

/// Choose the earlier overlap and shift a gap forward, as Java's `atZone` does.
pub fn localize_with_fallback<Tz: TimeZone + Display>(
    tz: &Tz,
    datetime: &NaiveDateTime,
) -> Result<DateTime<Utc>> {
    let offset = match local_timezone_offsets(tz, datetime)? {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Fold { before, .. } | AmbiguousOffset::Gap { before, .. } => before,
    };
    FixedOffset::east_opt(offset.seconds())
        .and_then(|offset| datetime.and_local_timezone(offset).single())
        .map(|datetime| datetime.to_utc())
        .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}"))
}

/// Calendar boundaries inside a gap use its end, rather than shifting midnight.
pub fn localize_calendar_boundary<Tz: TimeZone + Display>(
    tz: &Tz,
    datetime: &NaiveDateTime,
) -> Result<DateTime<Utc>> {
    let zone = timezone(tz)?;
    let lookup = local_lookup(datetime)?;
    let ambiguous = zone.to_ambiguous_timestamp(lookup);
    if matches!(ambiguous.offset(), AmbiguousOffset::Gap { .. }) {
        let earlier = ambiguous
            .earlier()
            .map_err(|error| exec_datafusion_err!("{error}"))?;
        let transition = zone
            .following(earlier)
            .next()
            .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}"))?;
        let lookup_seconds = jiff::tz::Offset::UTC
            .to_timestamp(lookup)
            .map_err(|error| exec_datafusion_err!("{error}"))?
            .as_second();
        let seconds =
            datetime.and_utc().timestamp() + (transition.timestamp().as_second() - lookup_seconds);
        DateTime::from_timestamp(seconds, 0)
            .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}"))
    } else {
        localize_with_fallback(tz, datetime)
    }
}
