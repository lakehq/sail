use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use chumsky::extra::ParserExtra;
use chumsky::prelude::{any, choice, end, just, one_of};
use chumsky::Parser;

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
        NaiveTime::from_hms_nano_opt(
            value.hour as u32,
            value.minute as u32,
            value.second as u32,
            value.nanoseconds,
        )
        .ok_or_else(|| SqlError::invalid(format!("{value:?}")))
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
        let date = NaiveDate::try_from(date)?;
        let time = NaiveTime::try_from(time)?;
        let datetime = date.and_time(time);
        Ok((datetime, timezone))
    }
}

#[allow(clippy::unwrap_used)]
fn signed<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    just('-')
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

#[allow(clippy::unwrap_used)]
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

#[allow(clippy::unwrap_used)]
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
