use std::str::FromStr;

use chrono::{
    DateTime, FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc,
};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, exec_datafusion_err};
use sail_common::error::CommonError;
use sail_common::utils::datetime::parse_spark_timezone_id;

use crate::formatter::{
    TimestampMicrosecondFormatter, TimestampMillisecondFormatter, TimestampNanosecondFormatter,
    TimestampSecondFormatter,
};

/// Localize the naive datetime to the time zone.
/// 1. If the datetime is mapped to exactly one datetime in the time zone, the local datetime
///    is returned.
/// 1. If the datetime is ambiguous in the time zone, the earliest local datetime is returned.
/// 1. If the datetime is in a time-zone gap, it is shifted forward by the length of the gap.
pub fn localize_with_fallback(
    timezone: &SparkTimeZone,
    datetime: &NaiveDateTime,
) -> Result<DateTime<Utc>> {
    match timezone.from_local_datetime(datetime).earliest() {
        Some(datetime) => Ok(datetime.to_utc()),
        None => match timezone {
            SparkTimeZone::Named(timezone) => GapInfo::new(datetime, timezone)
                .and_then(|gap| gap.begin)
                .and_then(|(_, offset)| datetime.checked_sub_offset(offset.fix()))
                .map(|datetime| datetime.and_utc()),
            SparkTimeZone::Fixed(_) => None,
        }
        .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SparkTimeZone {
    Named(Tz),
    Fixed(FixedOffset),
}

#[derive(Debug, Clone, Copy)]
pub struct SparkTimeZoneOffset {
    timezone: SparkTimeZone,
    offset: FixedOffset,
}

impl std::fmt::Display for SparkTimeZoneOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.offset, f)
    }
}

impl Offset for SparkTimeZoneOffset {
    fn fix(&self) -> FixedOffset {
        self.offset
    }
}

impl SparkTimeZone {
    fn with_offset(&self, offset: FixedOffset) -> SparkTimeZoneOffset {
        SparkTimeZoneOffset {
            timezone: *self,
            offset,
        }
    }
}

impl TimeZone for SparkTimeZone {
    type Offset = SparkTimeZoneOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        offset.timezone
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> MappedLocalTime<Self::Offset> {
        match self {
            Self::Named(timezone) => timezone
                .offset_from_local_date(local)
                .map(|offset| self.with_offset(offset.fix())),
            Self::Fixed(offset) => MappedLocalTime::Single(self.with_offset(*offset)),
        }
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> MappedLocalTime<Self::Offset> {
        match self {
            Self::Named(timezone) => timezone
                .offset_from_local_datetime(local)
                .map(|offset| self.with_offset(offset.fix())),
            Self::Fixed(offset) => MappedLocalTime::Single(self.with_offset(*offset)),
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        match self {
            Self::Named(timezone) => self.with_offset(timezone.offset_from_utc_date(utc).fix()),
            Self::Fixed(offset) => self.with_offset(*offset),
        }
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        match self {
            Self::Named(timezone) => self.with_offset(timezone.offset_from_utc_datetime(utc).fix()),
            Self::Fixed(offset) => self.with_offset(*offset),
        }
    }
}

/// Mirrors the two rewrites in
/// `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#getZoneId`, which support the
/// pre-Spark-3.0 `(+|-)h:mm` and `(+|-)hh:m` forms. Java applies them with unanchored
/// `Matcher#replaceFirst`, so they also pad prefixed IDs such as `GMT+8:30`.
fn normalize_spark_zone_id(value: &str) -> String {
    let mut value = value.to_string();

    // `(\+|\-)(\d):` -> `$10$2:`
    let bytes = value.as_bytes();
    let single_hour = (0..bytes.len().saturating_sub(2)).find(|&index| {
        matches!(bytes[index], b'+' | b'-')
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2] == b':'
    });
    if let Some(index) = single_hour {
        value.insert(index + 1, '0');
    }

    // `(\+|\-)(\d\d):(\d)$` -> `$1$2:0$3`
    let bytes = value.as_bytes();
    let single_minute = bytes.len() >= 5 && {
        let index = bytes.len() - 5;
        matches!(bytes[index], b'+' | b'-')
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3] == b':'
            && bytes[index + 4].is_ascii_digit()
    };
    if single_minute {
        value.insert(value.len() - 1, '0');
    }

    value
}

/// Mirrors `java.time.ZoneOffset#of`, whose parser dispatches purely on the length of the
/// offset ID: `+h`, `+hh`, `+hhmm`, `+hh:mm`, `+hhmmss` and `+hh:mm:ss`. Colons are never
/// optional — they are implied by the length.
fn parse_spark_fixed_offset(value: &str) -> Option<FixedOffset> {
    if !value.is_ascii() {
        return None;
    }

    let bytes = value.as_bytes();
    let sign = match bytes.first().copied() {
        Some(b'+') => 1_i32,
        Some(b'-') => -1_i32,
        _ => return None,
    };

    let component = |value: &str| {
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| value.parse::<i32>())
            .and_then(Result::ok)
    };

    let (hours, minutes, seconds) = match bytes.len() {
        2 => (component(&value[1..2])?, 0, 0),
        3 => (component(&value[1..3])?, 0, 0),
        5 => (component(&value[1..3])?, component(&value[3..5])?, 0),
        6 if bytes[3] == b':' => (component(&value[1..3])?, component(&value[4..6])?, 0),
        7 => (
            component(&value[1..3])?,
            component(&value[3..5])?,
            component(&value[5..7])?,
        ),
        9 if bytes[3] == b':' && bytes[6] == b':' => (
            component(&value[1..3])?,
            component(&value[4..6])?,
            component(&value[7..9])?,
        ),
        _ => return None,
    };

    if minutes > 59 || seconds > 59 || hours > 18 || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }

    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60 + seconds))
}

impl FromStr for SparkTimeZone {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let normalized = normalize_spark_zone_id(value);

        if normalized == "Z" {
            return FixedOffset::east_opt(0).map(Self::Fixed).ok_or(());
        }

        if let Some(offset) = parse_spark_fixed_offset(&normalized) {
            return Ok(Self::Fixed(offset));
        }

        for prefix in ["UTC", "GMT", "UT"] {
            if let Some(suffix) = normalized.strip_prefix(prefix) {
                // `java.time.ZoneId#of` resolves a bare `UTC`, `GMT` or `UT` to `+00:00`.
                if suffix.is_empty() {
                    return FixedOffset::east_opt(0).map(Self::Fixed).ok_or(());
                }
                if let Some(offset) = parse_spark_fixed_offset(suffix) {
                    return Ok(Self::Fixed(offset));
                }
            }
        }

        normalized.parse::<Tz>().map(Self::Named).map_err(|_| ())
    }
}

pub fn parse_spark_timezone(value: &str) -> Result<SparkTimeZone> {
    parse_spark_timezone_id::<SparkTimeZone>(value).map_err(|error| match error {
        CommonError::InvalidArgument(message) => DataFusionError::Execution(message),
        error => DataFusionError::External(Box::new(error)),
    })
}

pub fn validate_spark_timezone(value: &str) -> Result<()> {
    parse_spark_timezone(value).map(|_| ())
}

pub fn local_datetime_to_micros(datetime: &NaiveDateTime, timezone: &str) -> Result<i64> {
    let timezone = parse_spark_timezone(timezone)?;
    Ok(localize_with_fallback(&timezone, datetime)?.timestamp_micros())
}

pub fn format_timestamp_literal(
    value: i64,
    unit: TimeUnit,
    timezone: Option<&str>,
) -> Result<String> {
    let timezone = timezone.map(parse_spark_timezone).transpose()?;
    Ok(match unit {
        TimeUnit::Second => TimestampSecondFormatter(value, timezone.as_ref()).to_string(),
        TimeUnit::Millisecond => {
            TimestampMillisecondFormatter(value, timezone.as_ref()).to_string()
        }
        TimeUnit::Microsecond => {
            TimestampMicrosecondFormatter(value, timezone.as_ref()).to_string()
        }
        TimeUnit::Nanosecond => TimestampNanosecondFormatter(value, timezone.as_ref()).to_string(),
    })
}
