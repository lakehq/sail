use std::str::FromStr;

use chrono::{FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, exec_datafusion_err};
use sail_common::error::CommonError;
use sail_common::utils::datetime::spark_timezone_parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SparkTimeZone {
    Named(Tz),
    Fixed(FixedOffset),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SparkTimeZoneOffset {
    timezone: SparkTimeZone,
    offset: FixedOffset,
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

fn normalize_spark_zone_id(value: &str) -> String {
    let mut value = value.to_string();

    let add_hour_zero = {
        let bytes = value.as_bytes();
        bytes.len() >= 3
            && matches!(bytes.first().copied(), Some(b'+' | b'-'))
            && bytes[1].is_ascii_digit()
            && bytes[2] == b':'
    };
    if add_hour_zero {
        value.insert(1, '0');
    }

    let add_minute_zero = {
        let bytes = value.as_bytes();
        bytes.len() == 5
            && matches!(bytes.first().copied(), Some(b'+' | b'-'))
            && bytes[1].is_ascii_digit()
            && bytes[2].is_ascii_digit()
            && bytes[3] == b':'
            && bytes[4].is_ascii_digit()
    };
    if add_minute_zero {
        value.insert(4, '0');
    }

    value
}

fn parse_spark_fixed_offset(value: &str) -> Option<FixedOffset> {
    let (sign, value) = match value.as_bytes().first().copied() {
        Some(b'+') => (1_i32, &value[1..]),
        Some(b'-') => (-1_i32, &value[1..]),
        _ => return None,
    };

    let parse_component = |value: &str| {
        if value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_digit()) {
            value.parse::<u32>().ok()
        } else {
            None
        }
    };

    let parts = value.split(':').collect::<Vec<_>>();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [hours] => (parse_component(hours)?, 0, 0),
        [hours, minutes] => (parse_component(hours)?, parse_component(minutes)?, 0),
        [hours, minutes, seconds] => (
            parse_component(hours)?,
            parse_component(minutes)?,
            parse_component(seconds)?,
        ),
        _ => return None,
    };

    if minutes >= 60
        || seconds >= 60
        || hours > 18
        || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }

    let seconds = hours * 3_600 + minutes * 60 + seconds;
    FixedOffset::east_opt(sign * seconds as i32)
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
            if let Some(suffix) = normalized.strip_prefix(prefix)
                && !suffix.is_empty()
                && let Some(offset) = parse_spark_fixed_offset(suffix)
            {
                return Ok(Self::Fixed(offset));
            }
        }

        normalized.parse::<Tz>().map(Self::Named).map_err(|_| ())
    }
}

pub(crate) fn parse_spark_timezone(value: &str) -> Result<SparkTimeZone> {
    spark_timezone_parser::<SparkTimeZone>()(Some(value))
        .map_err(|error| match error {
            CommonError::InvalidArgument(message) => DataFusionError::Execution(message),
            error => DataFusionError::External(Box::new(error)),
        })?
        .ok_or_else(|| exec_datafusion_err!("cannot parse timezone {value:?}"))
}
