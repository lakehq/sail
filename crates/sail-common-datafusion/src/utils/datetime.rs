use std::borrow::Cow;

use chrono::{DateTime, Datelike, Days, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use sail_common::utils::datetime::timezone_alias_to_timezone;

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

/// A time zone as Spark accepts it.
#[derive(Debug)]
pub enum SparkZone {
    Fixed(FixedOffset),
    Named(Tz),
}

impl SparkZone {
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            return exec_err!("invalid empty time zone");
        }
        if value == "Z" {
            return Self::fixed(0);
        }

        if let Some(suffix) = ["UTC", "GMT", "UT"]
            .into_iter()
            .find_map(|prefix| value.strip_prefix(prefix))
        {
            if suffix.is_empty() {
                return Self::fixed(0);
            }
            if suffix.starts_with(['+', '-']) {
                let offset = parse_zone_offset(suffix)
                    .ok_or_else(|| exec_datafusion_err!("invalid time zone: {value}"))?;
                return Ok(Self::Fixed(offset));
            }
        }

        let zone_id = timezone_alias_to_timezone(value);

        if zone_id.starts_with(['+', '-']) {
            let offset = parse_zone_offset(zone_id)
                .ok_or_else(|| exec_datafusion_err!("invalid time zone: {value}"))?;
            return Ok(Self::Fixed(offset));
        }

        let timezone = zone_id
            .parse::<Tz>()
            .map_err(|_| exec_datafusion_err!("invalid time zone: {value}"))?;
        Ok(Self::Named(timezone))
    }

    fn fixed(seconds: i32) -> Result<Self> {
        FixedOffset::east_opt(seconds)
            .map(Self::Fixed)
            .ok_or_else(|| exec_datafusion_err!("invalid time-zone offset: {seconds}"))
    }

    pub fn current_date(&self) -> (i64, u32, u32) {
        let now = Utc::now();
        let date = match self {
            Self::Fixed(zone) => now.with_timezone(zone).date_naive(),
            Self::Named(zone) => now.with_timezone(zone).date_naive(),
        };
        (i64::from(date.year()), date.month(), date.day())
    }

    #[expect(clippy::too_many_arguments)]
    pub fn to_utc_micros(
        &self,
        year: i64,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micros: u32,
        local_micros: i128,
    ) -> Result<i64> {
        let offset_micros = match self {
            Self::Fixed(offset) => i128::from(offset.local_minus_utc()) * 1_000_000,
            Self::Named(zone) => {
                let representative_year = i32::try_from(year)
                    .ok()
                    .filter(|year| NaiveDate::from_ymd_opt(*year, month, day).is_some())
                    .map(Ok)
                    .unwrap_or_else(|| {
                        let year = if year >= 0 {
                            2400 + year.rem_euclid(400)
                        } else {
                            -200_000 + year.rem_euclid(400)
                        };
                        i32::try_from(year)
                            .map_err(|_| exec_datafusion_err!("invalid timestamp year: {year}"))
                    })?;
                let date = NaiveDate::from_ymd_opt(representative_year, month, day)
                    .ok_or_else(|| exec_datafusion_err!("invalid timestamp date"))?;
                let datetime = date
                    .and_hms_micro_opt(hour, minute, second, micros)
                    .ok_or_else(|| exec_datafusion_err!("invalid timestamp time"))?;
                let instant = localize_with_fallback(zone, &datetime)?;
                i128::from(datetime.and_utc().timestamp_micros())
                    - i128::from(instant.timestamp_micros())
            }
        };
        i64::try_from(local_micros - offset_micros)
            .map_err(|_| exec_datafusion_err!("timestamp is outside the microsecond range"))
    }
}

fn parse_digits(value: &str, min: usize, max: usize) -> Option<u32> {
    if !(min..=max).contains(&value.len()) || !value.bytes().all(|value| value.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn parse_zone_offset(value: &str) -> Option<FixedOffset> {
    let (sign, body) = match value.as_bytes().first() {
        Some(b'+') => (1_i32, &value[1..]),
        Some(b'-') => (-1_i32, &value[1..]),
        _ => return None,
    };
    if !body.is_ascii() {
        return None;
    }
    let (hours, minutes, seconds) = if body.contains(':') {
        let parts = body.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            [hours, minutes] => (parse_digits(hours, 1, 2)?, parse_digits(minutes, 1, 2)?, 0),
            [hours, minutes, seconds] => (
                parse_digits(hours, 1, 2)?,
                parse_digits(minutes, 2, 2)?,
                parse_digits(seconds, 2, 2)?,
            ),
            _ => return None,
        }
    } else {
        match body.len() {
            1 => (parse_digits(body, 1, 1)?, 0, 0),
            2 => (parse_digits(body, 2, 2)?, 0, 0),
            4 => (
                parse_digits(&body[..2], 2, 2)?,
                parse_digits(&body[2..], 2, 2)?,
                0,
            ),
            6 => (
                parse_digits(&body[..2], 2, 2)?,
                parse_digits(&body[2..4], 2, 2)?,
                parse_digits(&body[4..], 2, 2)?,
            ),
            _ => return None,
        }
    };
    if hours > 18 || minutes >= 60 || seconds >= 60 {
        return None;
    }
    if hours == 18 && (minutes != 0 || seconds != 0) {
        return None;
    }
    let seconds = i32::try_from(hours * 3600 + minutes * 60 + seconds).ok()?;
    FixedOffset::east_opt(sign * seconds)
}

/// The Arrow time zone label of a Spark session time zone. Arrow does not parse ids such as
/// `Z`, `UT`, `GMT+8`, `PST` or `+5:30`, which Spark accepts, so they are spelled as the same
/// zone, except that an offset with seconds is labelled UTC. An id that Arrow parses, or that
/// Spark does not accept, stays as it is.
pub fn arrow_timezone_label(value: &str) -> Cow<'_, str> {
    if value.parse::<Tz>().is_ok() {
        return Cow::Borrowed(value);
    }
    match SparkZone::parse(value) {
        Ok(SparkZone::Fixed(offset)) if offset.local_minus_utc() % 60 == 0 => {
            Cow::Owned(offset.to_string())
        }
        // TODO: Label values with an offset with seconds, which Arrow does not parse. Until then
        //  they are labelled with UTC, in which Sail compared them before it used the session
        //  time zone.
        Ok(SparkZone::Fixed(_)) => Cow::Borrowed("UTC"),
        Ok(SparkZone::Named(_)) => Cow::Borrowed(timezone_alias_to_timezone(value)),
        Err(_) => Cow::Borrowed(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_timezone_label_spells_spark_zones_for_arrow() {
        for (spark, arrow) in [
            ("Z", "+00:00"),
            ("UT", "+00:00"),
            ("GMT+8", "+08:00"),
            ("UTC+08:00", "+08:00"),
            ("GMT-03:30", "-03:30"),
            ("PST", "America/Los_Angeles"),
            ("+5:30", "+05:30"),
            ("+05:30:00", "+05:30"),
            ("+053000", "+05:30"),
            ("+05:30:45", "UTC"),
            ("+053045", "UTC"),
            ("Mars/Olympus", "Mars/Olympus"),
            ("UTC", "UTC"),
            ("+0530", "+0530"),
            ("-08", "-08"),
            ("Asia/Kolkata", "Asia/Kolkata"),
        ] {
            assert_eq!(arrow_timezone_label(spark), arrow, "{spark}");
        }
    }
}
