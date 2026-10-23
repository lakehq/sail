use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::prelude::{any, choice, end, just, one_of};

use crate::error::{SqlError, SqlResult};

#[derive(Debug, Clone)]
pub struct DateValue {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl TryFrom<DateValue> for NaiveDate {
    type Error = SqlError;

    fn try_from(value: DateValue) -> SqlResult<Self> {
        NaiveDate::from_ymd_opt(value.year, value.month as u32, value.day as u32)
            .ok_or_else(|| SqlError::invalid(format!("{value:?}")))
    }
}

impl DateValue {
    /// Days since 1970-01-01 in the proleptic Gregorian calendar. `None` when the count does
    /// not fit an `Int`, as in Spark's `localDateToDays` (`Math.toIntExact`). It does not go
    /// through `NaiveDate`, which stops at year 262143 while Spark reads seven-digit years.
    pub fn to_epoch_days(&self) -> Option<i32> {
        let month = i64::from(self.month);
        let year = i64::from(self.year) - i64::from(month <= 2);
        let era = year.div_euclid(400);
        let year_of_era = year.rem_euclid(400);
        let day_of_year =
            (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(self.day) - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        i32::try_from(era * 146_097 + day_of_era - 719_468).ok()
    }
}

/// Spark's `SparkDateTimeUtils.stringToDate` (SparkDateTimeUtils.scala:395-458), byte by byte.
///
/// The input is trimmed of whitespace and ISO control characters (`UTF8String
/// .isWhitespaceOrISOControl` on a byte: 0x00-0x20 and 0x7F; a byte of a multi-byte character
/// is negative in Java and is never trimmed). Then `[+-]yyyy*[-[m]m[-[d]d]]` is read: a year of
/// four to seven digits, a month and a day of one or two. Once the day has been read, the first
/// ' ' or 'T' ends the date and everything after it is ignored; after only the year, or the
/// year and month, the whole input has to be consumed. Any other character gives `None`, a tab
/// or a newline included.
pub fn parse_spark_date(s: &str) -> Option<DateValue> {
    let is_trimmed = |b: &u8| *b <= 0x20 || *b == 0x7F;
    let bytes = s.as_bytes();
    let start = bytes.iter().position(|b| !is_trimmed(b))?;
    let end = bytes.iter().rposition(|b| !is_trimmed(b))? + 1;
    let bytes = &bytes[start..end];
    let (sign, mut position) = match bytes.first()? {
        b'-' => (-1_i32, 1),
        b'+' => (1_i32, 1),
        _ => (1_i32, 0),
    };
    let valid_digits = |segment: usize, digits: usize| {
        if segment == 0 {
            (4..=7).contains(&digits)
        } else {
            (1..=2).contains(&digits)
        }
    };
    // Year, month and day; an absent month or day is 1.
    let mut segments = [1_i32; 3];
    let (mut segment, mut value, mut digits) = (0_usize, 0_i32, 0_usize);
    while position < bytes.len() && !matches!(bytes[position], b' ' | b'T') {
        let b = bytes[position];
        if segment < 2 && b == b'-' {
            if !valid_digits(segment, digits) {
                return None;
            }
            segments[segment] = value;
            (value, digits) = (0, 0);
            segment += 1;
        } else if b.is_ascii_digit() {
            // More than seven digits is invalid in every segment.
            digits += 1;
            if digits > 7 {
                return None;
            }
            value = value * 10 + i32::from(b - b'0');
        } else {
            return None;
        }
        position += 1;
    }
    if !valid_digits(segment, digits) {
        return None;
    }
    if segment < 2 && position < bytes.len() {
        return None;
    }
    segments[segment] = value;
    let year = sign * segments[0];
    let (month, day) = (segments[1], segments[2]);
    let is_leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=days_in_month).contains(&day) {
        return None;
    }
    Some(DateValue {
        year,
        month: u8::try_from(month).ok()?,
        day: u8::try_from(day).ok()?,
    })
}

#[derive(Debug, Clone, Default)]
pub struct TimeValue {
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanoseconds: u32,
}

impl TryFrom<TimeValue> for NaiveTime {
    type Error = SqlError;

    fn try_from(value: TimeValue) -> SqlResult<Self> {
        let second = if value.second == 60 {
            if value.hour != 23 || value.minute != 59 {
                return Err(SqlError::invalid("leap second must be at 23:59:60"));
            }
            59
        } else {
            value.second
        };

        let time = NaiveTime::from_hms_nano_opt(
            value.hour as u32,
            value.minute as u32,
            second as u32,
            value.nanoseconds,
        )
        .ok_or_else(|| SqlError::invalid(format!("{value:?}")))?;

        Ok(time)
    }
}

#[derive(Debug, Clone)]
pub struct TimestampValue<'a> {
    pub date: DateValue,
    pub time: TimeValue,
    pub timezone: &'a str,
}

impl<'a> TimestampValue<'a> {
    pub fn into_naive(self) -> SqlResult<(NaiveDateTime, &'a str)> {
        let Self {
            date,
            time,
            timezone,
        } = self;

        let is_leap_second = time.second == 60;
        let date = NaiveDate::try_from(date)?;
        let time = NaiveTime::try_from(time)?;
        let datetime = date.and_time(time);

        let datetime = if is_leap_second {
            datetime + chrono::Duration::seconds(1)
        } else {
            datetime
        };

        Ok((datetime, timezone))
    }
}

#[expect(clippy::unwrap_used)]
fn signed<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of(['-', '+'])
        .or_not()
        .then(
            one_of('0'..='9')
                .repeated()
                .at_least(min_digits)
                .at_most(max_digits),
        )
        .to_slice()
        .map(|s: &str| s.parse::<T>().unwrap())
}

#[expect(clippy::unwrap_used)]
fn unsigned<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of('0'..='9')
        .repeated()
        .at_least(min_digits)
        .at_most(max_digits)
        .to_slice()
        .map(|s: &str| s.parse::<T>().unwrap())
}

/// Parses fractional seconds with configurable digit range.
///
/// The parser accepts up to 9 digits (nanoseconds) for compatibility with Spark SQL syntax,
/// but Spark stores timestamps with microsecond precision (6 digits). Nanosecond part is
/// truncated during conversion to match Spark behavior.
#[expect(clippy::unwrap_used)]
fn fraction<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of('0'..='9')
        .repeated()
        .at_least(min_digits)
        .at_most(max_digits)
        .to_slice()
        .map(move |s: &str| {
            s.chars()
                .chain(std::iter::repeat('0'))
                .take(max_digits)
                .collect::<String>()
                .parse::<T>()
                .unwrap()
        })
}

fn date<'a, E>() -> impl Parser<'a, &'a str, DateValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    let year = signed(4, 7);
    let month = unsigned(1, 2);
    let day = unsigned(1, 2);

    year.then(just('-').ignore_then(month).or_not())
        .then(just('-').ignore_then(day).or_not())
        .map(|((year, month), day)| DateValue {
            year,
            month: month.unwrap_or(1),
            day: day.unwrap_or(1),
        })
}

fn time<'a, E>() -> impl Parser<'a, &'a str, TimeValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    let hour = unsigned(1, 2);
    let minute = unsigned(1, 2);
    let second = unsigned(1, 2);
    let nanoseconds = fraction(0, 9);

    hour.then(just(':').ignore_then(minute).or_not())
        .then(just(':').ignore_then(second).or_not())
        .then(just('.').ignore_then(nanoseconds).or_not())
        .map(|(((hour, minute), second), nanoseconds)| TimeValue {
            hour,
            minute: minute.unwrap_or(0),
            second: second.unwrap_or(0),
            nanoseconds: nanoseconds.unwrap_or(0),
        })
}

fn timezone<'a, E>() -> impl Parser<'a, &'a str, &'a str, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    any().repeated().to_slice().map(|s: &str| {
        let s = s.trim_matches(' ');
        if s.eq_ignore_ascii_case("Z") {
            "UTC"
        } else if s.starts_with("UTC+") || s.starts_with("UTC-") {
            // Convert "UTC+xx:xx" to "+xx:xx" and "UTC-xx:xx" to "-xx:xx"
            &s[3..]
        } else {
            s
        }
    })
}

fn timestamp<'a, E>() -> impl Parser<'a, &'a str, TimestampValue<'a>, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    date()
        .then(choice((
            just('T')
                .or(just('t'))
                .padded_by(just(' ').repeated())
                .ignored()
                .or(just(' ').repeated().at_least(1))
                .ignore_then(time().then(timezone()).map(Some)),
            just(' ').repeated().map(|_| None),
        )))
        .map(|(date, time_and_timezone)| {
            let (time, timezone) = match time_and_timezone {
                None => (Default::default(), ""),
                Some((t, tz)) => (t, tz),
            };
            TimestampValue {
                date,
                time,
                timezone,
            }
        })
}

pub fn create_date_parser<'a, E>() -> impl Parser<'a, &'a str, DateValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    // A date value is allowed to be followed by an arbitrary string which is ignored.
    date()
        .then_ignore(
            any()
                .filter(|c: &char| c.is_ascii_whitespace())
                .then(any().repeated())
                .or_not(),
        )
        .then_ignore(end())
}

pub fn create_timestamp_parser<'a, E>() -> impl Parser<'a, &'a str, TimestampValue<'a>, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    timestamp().then_ignore(end())
}

pub fn create_time_parser<'a, E>() -> impl Parser<'a, &'a str, TimeValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    time().then_ignore(end())
}

#[cfg(test)]
mod tests {
    use super::parse_spark_date;

    fn epoch_days(s: &str) -> Option<i32> {
        parse_spark_date(s).and_then(|date| date.to_epoch_days())
    }

    #[test]
    fn test_epoch_days_across_the_whole_range() {
        assert_eq!(epoch_days("1970-01-01"), Some(0));
        assert_eq!(epoch_days("1969-12-31"), Some(-1));
        assert_eq!(epoch_days("2000-01-01"), Some(10_957));
        assert_eq!(epoch_days("2024-02-29"), Some(19_782));
        assert_eq!(epoch_days("0000-01-31"), Some(-719_528 + 30));
        // Years beyond `chrono`'s 262142, up to the last one an `Int` of days holds.
        assert_eq!(epoch_days("5874897-12-31"), Some(2_145_042_905));
        assert_eq!(epoch_days("5874898-01-01"), Some(2_145_042_906));
        assert_eq!(epoch_days("-1234567-01-31"), Some(-451_635_835));
    }

    #[test]
    fn test_a_day_count_that_does_not_fit_an_int_is_none() {
        assert_eq!(epoch_days("9999999-12-31"), None);
        assert_eq!(epoch_days("-9999999-01-01"), None);
    }

    #[test]
    fn test_the_terminator_after_the_day_is_a_space_or_t_only() {
        for accepted in [
            "2023-01-31 x",
            "2023-01-31T",
            "2023-01-31T13:45:00",
            "2023-1-5T10:00",
        ] {
            assert!(parse_spark_date(accepted).is_some(), "{accepted}");
        }
        for rejected in [
            "2023-01-31\t10",
            "2023-01-31\n10",
            "2023-01-31t10",
            "2023-01-31Z",
            "2023-01 x",
            "2023 x",
            "2023-01T",
            "2023-02-30T00:00:00",
        ] {
            assert!(parse_spark_date(rejected).is_none(), "{rejected:?}");
        }
    }

    #[test]
    fn test_trimming_covers_whitespace_and_iso_control_bytes_only() {
        for accepted in [
            " 2023-01-31",
            "\t2023-01-31",
            "\u{1}2023-01-31",
            "2023-01-31\u{1}",
            "\u{7f}2023-01-31",
        ] {
            assert!(parse_spark_date(accepted).is_some(), "{accepted:?}");
        }
        for rejected in ["\u{a0}2023-01-31", "2023-01-31\u{a0}", "", " ", "\t"] {
            assert!(parse_spark_date(rejected).is_none(), "{rejected:?}");
        }
    }
}
