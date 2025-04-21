use std::str::FromStr;

use chrono::format::{parse_and_remainder, Fixed, Item, Numeric, Pad, Parsed};
use chrono::{
    DateTime, Days, FixedOffset, MappedLocalTime, NaiveDateTime, Offset, ParseError, TimeZone, Utc,
};
use chrono_tz::Tz;
use datafusion_common::{exec_datafusion_err, exec_err, Result};

#[derive(Debug)]
pub struct TimestampValue {
    pub datetime: NaiveDateTime,
    pub timezone: Option<TimeZoneValue>,
}

#[derive(Debug)]
pub enum TimeZoneValue {
    Fixed(FixedOffset),
    Tz(Tz),
}

impl TimeZoneValue {
    #[allow(clippy::wrong_self_convention)]
    pub fn from_utc(&self, datetime: &DateTime<Utc>) -> NaiveDateTime {
        match self {
            TimeZoneValue::Fixed(offset) => datetime.with_timezone(offset).naive_local(),
            TimeZoneValue::Tz(tz) => datetime.with_timezone(tz).naive_local(),
        }
    }

    pub fn localize(&self, datetime: &NaiveDateTime) -> MappedLocalTime<DateTime<Utc>> {
        match self {
            TimeZoneValue::Fixed(offset) => {
                offset.from_local_datetime(datetime).map(|x| x.to_utc())
            }
            TimeZoneValue::Tz(tz) => tz.from_local_datetime(datetime).map(|x| x.to_utc()),
        }
    }

    /// Localize the naive datetime to the time zone.
    /// 1. If the datatime is mapped to exactly one datetime in the time zone, the local datetime
    ///    is returned.
    /// 1. If the datetime is ambiguous in the time zone, the earliest local datetime is returned.
    /// 1. If the datetime is invalid in the time zone, the next valid local datetime is returned.
    pub fn localize_with_fallback(&self, datetime: &NaiveDateTime) -> Result<DateTime<Utc>> {
        match self.localize(datetime).earliest() {
            Some(x) => Ok(x),
            // FIXME: The logic here may not work in all cases.
            //   The correct solution requires access to offset transition rules for the time zone.
            None => datetime
                .checked_sub_days(Days::new(1))
                .and_then(|x| self.localize(&x).earliest())
                .and_then(|x| x.checked_add_days(Days::new(1)))
                .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}")),
        }
    }
}

pub fn parse_timezone(s: &str) -> Result<TimeZoneValue> {
    if let Ok(offset) = FixedOffset::from_str(s) {
        Ok(TimeZoneValue::Fixed(offset))
    } else {
        let tz = Tz::from_str(s).map_err(|_| exec_datafusion_err!("invalid time zone: {s}"))?;
        Ok(TimeZoneValue::Tz(tz))
    }
}

pub fn parse_timestamp(s: &str) -> Result<TimestampValue> {
    const DATE_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
    ];
    const TIME_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Hour, Pad::Zero),
        Item::Space(""),
        Item::Literal(":"),
        Item::Numeric(Numeric::Minute, Pad::Zero),
        Item::Space(""),
        Item::Literal(":"),
        Item::Numeric(Numeric::Second, Pad::Zero),
        Item::Fixed(Fixed::Nanosecond),
        Item::Space(""),
    ];

    let error = |e: ParseError| exec_datafusion_err!("invalid timestamp: {e}: {s}");
    let mut parsed = Parsed::new();
    let suffix = parse_and_remainder(&mut parsed, s, DATE_ITEMS.iter()).map_err(error)?;
    let suffix = match suffix.as_bytes().first() {
        Some(b' ' | b'T' | b't') => &suffix[1..],
        Some(_) => return exec_err!("invalid time part in timestamp: {s}"),
        None => return exec_err!("missing time part in timestamp: {s}"),
    };
    let suffix = parse_and_remainder(&mut parsed, suffix, TIME_ITEMS.iter()).map_err(error)?;
    let timezone = if suffix.is_empty() {
        None
    } else if suffix.eq_ignore_ascii_case("Z") {
        Some(TimeZoneValue::Fixed(Utc.fix()))
    } else {
        Some(parse_timezone(suffix)?)
    };
    let datetime = parsed.to_naive_datetime_with_offset(0).map_err(error)?;
    Ok(TimestampValue { datetime, timezone })
}
