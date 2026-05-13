//! Shared Spark interval qualifier kind and value formatting.
//!
//! The qualifier kind (e.g. `YEAR`, `YEAR TO MONTH`, `DAY TO SECOND`) is the
//! pair `(start_field, end_field)` Spark attaches to typed interval columns.
//! It does not live inside the underlying Arrow value (an `i32` of months or
//! an `i64` of microseconds), so any code that needs to render an interval
//! as Spark's canonical literal text — both the SQL analyzer (when naming a
//! literal column) and the `show_string` runtime path (when rendering a row
//! value) — needs to combine the raw value with the kind.

use std::fmt::{self, Display, Formatter};

use serde::Deserialize;

/// One of Spark's 13 ANSI interval qualifier kinds.
///
/// Numeric encoding used by Spark / our Arrow extension metadata:
/// year-month uses `YEAR=0, MONTH=1`; day-time uses
/// `DAY=0, HOUR=1, MINUTE=2, SECOND=3`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SparkIntervalKind {
    Year,
    YearToMonth,
    Month,
    Day,
    DayToHour,
    DayToMinute,
    DayToSecond,
    Hour,
    HourToMinute,
    HourToSecond,
    Minute,
    MinuteToSecond,
    Second,
}

impl Display for SparkIntervalKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SparkIntervalKind::Year => "YEAR",
            SparkIntervalKind::YearToMonth => "YEAR TO MONTH",
            SparkIntervalKind::Month => "MONTH",
            SparkIntervalKind::Day => "DAY",
            SparkIntervalKind::DayToHour => "DAY TO HOUR",
            SparkIntervalKind::DayToMinute => "DAY TO MINUTE",
            SparkIntervalKind::DayToSecond => "DAY TO SECOND",
            SparkIntervalKind::Hour => "HOUR",
            SparkIntervalKind::HourToMinute => "HOUR TO MINUTE",
            SparkIntervalKind::HourToSecond => "HOUR TO SECOND",
            SparkIntervalKind::Minute => "MINUTE",
            SparkIntervalKind::MinuteToSecond => "MINUTE TO SECOND",
            SparkIntervalKind::Second => "SECOND",
        })
    }
}

impl SparkIntervalKind {
    /// Map a year-month `(start_field, end_field)` pair to a kind.
    pub fn from_year_month_fields(start: i32, end: i32) -> Option<Self> {
        match (start, end) {
            (0, 0) => Some(Self::Year),
            (0, 1) => Some(Self::YearToMonth),
            (1, 1) => Some(Self::Month),
            _ => None,
        }
    }

    /// Map a day-time `(start_field, end_field)` pair to a kind.
    pub fn from_day_time_fields(start: i32, end: i32) -> Option<Self> {
        match (start, end) {
            (0, 0) => Some(Self::Day),
            (0, 1) => Some(Self::DayToHour),
            (0, 2) => Some(Self::DayToMinute),
            (0, 3) => Some(Self::DayToSecond),
            (1, 1) => Some(Self::Hour),
            (1, 2) => Some(Self::HourToMinute),
            (1, 3) => Some(Self::HourToSecond),
            (2, 2) => Some(Self::Minute),
            (2, 3) => Some(Self::MinuteToSecond),
            (3, 3) => Some(Self::Second),
            _ => None,
        }
    }
}

/// Deserialization shape for the JSON payload Sail writes to
/// `ARROW:extension:metadata` for `SAIL_INTERVAL_EXTENSION_NAME` columns.
#[derive(Debug, Default, Deserialize)]
pub struct IntervalQualifierMetadata {
    pub start_field: Option<i32>,
    pub end_field: Option<i32>,
}

/// Format a year-month interval value (`months`) into Spark's canonical
/// literal form, e.g. `"INTERVAL '10' YEAR"`. Returns `None` if `kind` is a
/// day-time kind.
pub fn format_year_month_interval(months: i32, kind: SparkIntervalKind) -> Option<String> {
    let body = match kind {
        SparkIntervalKind::Year => format!("{}", months / 12),
        SparkIntervalKind::Month => format!("{months}"),
        SparkIntervalKind::YearToMonth => {
            let sign = if months < 0 { "-" } else { "" };
            let abs_months = months.unsigned_abs();
            format!("{sign}{}-{}", abs_months / 12, abs_months % 12)
        }
        _ => return None,
    };
    Some(format!("INTERVAL '{body}' {kind}"))
}

/// Format a day-time interval value (`microseconds`) into Spark's canonical
/// literal form, e.g. `"INTERVAL '1 02:03:04.123456' DAY TO SECOND"`. Larger
/// fields are unbounded; smaller fields (hour, minute, second) are
/// zero-padded to two digits. Fractional seconds use up to six digits with
/// trailing zeros stripped. Returns `None` if `kind` is a year-month kind.
pub fn format_day_time_interval(microseconds: i64, kind: SparkIntervalKind) -> Option<String> {
    const US_PER_SECOND: u64 = 1_000_000;
    const US_PER_MINUTE: u64 = 60 * US_PER_SECOND;
    const US_PER_HOUR: u64 = 60 * US_PER_MINUTE;
    const US_PER_DAY: u64 = 24 * US_PER_HOUR;
    let sign = if microseconds < 0 { "-" } else { "" };
    let abs_us = microseconds.unsigned_abs();
    let fraction = |us: u64| -> String {
        let f = us % US_PER_SECOND;
        if f == 0 {
            String::new()
        } else {
            format!(".{}", format!("{f:06}").trim_end_matches('0'))
        }
    };
    let body = match kind {
        SparkIntervalKind::Day => format!("{sign}{}", abs_us / US_PER_DAY),
        SparkIntervalKind::DayToHour => {
            let d = abs_us / US_PER_DAY;
            let h = (abs_us % US_PER_DAY) / US_PER_HOUR;
            format!("{sign}{d} {h:02}")
        }
        SparkIntervalKind::DayToMinute => {
            let d = abs_us / US_PER_DAY;
            let rem = abs_us % US_PER_DAY;
            let h = rem / US_PER_HOUR;
            let m = (rem % US_PER_HOUR) / US_PER_MINUTE;
            format!("{sign}{d} {h:02}:{m:02}")
        }
        SparkIntervalKind::DayToSecond => {
            let d = abs_us / US_PER_DAY;
            let rem = abs_us % US_PER_DAY;
            let h = rem / US_PER_HOUR;
            let rem = rem % US_PER_HOUR;
            let m = rem / US_PER_MINUTE;
            let rem = rem % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{d} {h:02}:{m:02}:{s:02}{}", fraction(abs_us))
        }
        SparkIntervalKind::Hour => format!("{sign}{:02}", abs_us / US_PER_HOUR),
        SparkIntervalKind::HourToMinute => {
            let h = abs_us / US_PER_HOUR;
            let m = (abs_us % US_PER_HOUR) / US_PER_MINUTE;
            format!("{sign}{h:02}:{m:02}")
        }
        SparkIntervalKind::HourToSecond => {
            let h = abs_us / US_PER_HOUR;
            let rem = abs_us % US_PER_HOUR;
            let m = rem / US_PER_MINUTE;
            let rem = rem % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{h:02}:{m:02}:{s:02}{}", fraction(abs_us))
        }
        SparkIntervalKind::Minute => format!("{sign}{:02}", abs_us / US_PER_MINUTE),
        SparkIntervalKind::MinuteToSecond => {
            let m = abs_us / US_PER_MINUTE;
            let rem = abs_us % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{m:02}:{s:02}{}", fraction(abs_us))
        }
        SparkIntervalKind::Second => {
            let s = abs_us / US_PER_SECOND;
            format!("{sign}{s:02}{}", fraction(abs_us))
        }
        _ => return None,
    };
    Some(format!("INTERVAL '{body}' {kind}"))
}
