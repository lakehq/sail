use std::iter::once;
use std::str::FromStr;

use chrono::{self, TimeDelta};
use lazy_static::lazy_static;
use regex::Regex;
use sail_common::spec;
use sail_sql_parser::ast::data_type::{IntervalDayTimeUnit, IntervalYearMonthUnit};
use sail_sql_parser::ast::expression::{
    Expr, IntervalExpr, IntervalLiteral, IntervalQualifier, IntervalUnit, IntervalValueWithUnit,
};

use crate::error::{SqlError, SqlResult};
use crate::literal::utils::{extract_fraction_match, extract_match, parse_signed_value, Signed};
use crate::parser::parse_interval_literal;
use crate::value::from_ast_string;

fn create_regex(regex: Result<Regex, regex::Error>) -> Regex {
    #[expect(clippy::unwrap_used)]
    regex.unwrap()
}

lazy_static! {
    static ref INTERVAL_YEAR_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<year>\d+)\s*$"));
    static ref INTERVAL_YEAR_TO_MONTH_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<year>\d+)-(?P<month>\d+)\s*$"
    ));
    static ref INTERVAL_MONTH_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<month>\d+)\s*$"));
    static ref INTERVAL_DAY_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s*$"));
    static ref INTERVAL_DAY_TO_HOUR_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+)\s*$"
    ));
    static ref INTERVAL_DAY_TO_MINUTE_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+)\s*$"
    ));
    static ref INTERVAL_DAY_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_HOUR_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<hour>\d+)\s*$"));
    static ref INTERVAL_HOUR_TO_MINUTE_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+)\s*$"
    ));
    static ref INTERVAL_HOUR_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_MINUTE_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<minute>\d+)\s*$"));
    static ref INTERVAL_MINUTE_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IntervalValue {
    YearMonth {
        months: i32,
    },
    Microsecond {
        microseconds: i64,
    },
    MonthDayNanosecond {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
}

impl From<IntervalValue> for spec::Literal {
    fn from(value: IntervalValue) -> Self {
        match value {
            IntervalValue::YearMonth { months } => spec::Literal::IntervalYearMonth {
                months: Some(months),
            },
            IntervalValue::Microsecond { microseconds } => spec::Literal::DurationMicrosecond {
                microseconds: Some(microseconds),
            },
            IntervalValue::MonthDayNanosecond {
                months,
                days,
                nanoseconds,
            } => spec::Literal::IntervalMonthDayNano {
                value: Some(spec::IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                }),
            },
        }
    }
}

/// Format an `IntervalValue` as Spark's canonical SQL literal column name
/// (e.g. `"INTERVAL '7' DAY"`, `"INTERVAL '01:02:03.456789' HOUR TO SECOND"`).
/// Returns `None` if the value cannot be expressed under the requested kind
/// (e.g. a mixed `MonthDayNanosecond` with no single ANSI qualifier).
fn format_interval_column_name(
    value: &IntervalValue,
    kind: &StandardIntervalKind,
) -> Option<String> {
    use StandardIntervalKind as K;
    let body = match (value, kind) {
        (IntervalValue::YearMonth { months }, K::Year) => format!("{}", months / 12),
        (IntervalValue::YearMonth { months }, K::Month) => format!("{months}"),
        (IntervalValue::YearMonth { months }, K::YearToMonth) => {
            let sign = if *months < 0 { "-" } else { "" };
            let abs_months = months.unsigned_abs();
            format!("{sign}{}-{}", abs_months / 12, abs_months % 12)
        }
        (IntervalValue::Microsecond { microseconds }, kind) => {
            format_day_time(*microseconds, kind)?
        }
        _ => return None,
    };
    Some(format!("INTERVAL '{body}' {kind}"))
}

/// Format a day-time interval (signed microseconds) under a specific ANSI
/// `StandardIntervalKind`. Larger fields are unbounded; smaller fields are
/// zero-padded to 2 digits. Fractional seconds get up to 6 digits with
/// trailing zeros stripped. Returns `None` if `kind` is not a day-time kind.
fn format_day_time(microseconds: i64, kind: &StandardIntervalKind) -> Option<String> {
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
    use StandardIntervalKind as K;
    Some(match kind {
        K::Day => format!("{sign}{}", abs_us / US_PER_DAY),
        K::DayToHour => {
            let d = abs_us / US_PER_DAY;
            let h = (abs_us % US_PER_DAY) / US_PER_HOUR;
            format!("{sign}{d} {h:02}")
        }
        K::DayToMinute => {
            let d = abs_us / US_PER_DAY;
            let rem = abs_us % US_PER_DAY;
            let h = rem / US_PER_HOUR;
            let m = (rem % US_PER_HOUR) / US_PER_MINUTE;
            format!("{sign}{d} {h:02}:{m:02}")
        }
        K::DayToSecond => {
            let d = abs_us / US_PER_DAY;
            let rem = abs_us % US_PER_DAY;
            let h = rem / US_PER_HOUR;
            let rem = rem % US_PER_HOUR;
            let m = rem / US_PER_MINUTE;
            let rem = rem % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{d} {h:02}:{m:02}:{s:02}{}", fraction(abs_us))
        }
        K::Hour => format!("{sign}{:02}", abs_us / US_PER_HOUR),
        K::HourToMinute => {
            let h = abs_us / US_PER_HOUR;
            let m = (abs_us % US_PER_HOUR) / US_PER_MINUTE;
            format!("{sign}{h:02}:{m:02}")
        }
        K::HourToSecond => {
            let h = abs_us / US_PER_HOUR;
            let rem = abs_us % US_PER_HOUR;
            let m = rem / US_PER_MINUTE;
            let rem = rem % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{h:02}:{m:02}:{s:02}{}", fraction(abs_us))
        }
        K::Minute => format!("{sign}{:02}", abs_us / US_PER_MINUTE),
        K::MinuteToSecond => {
            let m = abs_us / US_PER_MINUTE;
            let rem = abs_us % US_PER_MINUTE;
            let s = rem / US_PER_SECOND;
            format!("{sign}{m:02}:{s:02}{}", fraction(abs_us))
        }
        K::Second => {
            let s = abs_us / US_PER_SECOND;
            format!("{sign}{s:02}{}", fraction(abs_us))
        }
        // Not a day-time kind.
        K::Year | K::YearToMonth | K::Month => return None,
    })
}

fn standard_interval_kind_to_spec_type(kind: &StandardIntervalKind) -> spec::DataType {
    use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};
    use spec::IntervalUnit::{DayTime, YearMonth};
    let (interval_unit, start, end) = match kind {
        StandardIntervalKind::Year => (YearMonth, Year, Year),
        StandardIntervalKind::YearToMonth => (YearMonth, Year, Month),
        StandardIntervalKind::Month => (YearMonth, Month, Month),
        StandardIntervalKind::Day => (DayTime, Day, Day),
        StandardIntervalKind::DayToHour => (DayTime, Day, Hour),
        StandardIntervalKind::DayToMinute => (DayTime, Day, Minute),
        StandardIntervalKind::DayToSecond => (DayTime, Day, Second),
        StandardIntervalKind::Hour => (DayTime, Hour, Hour),
        StandardIntervalKind::HourToMinute => (DayTime, Hour, Minute),
        StandardIntervalKind::HourToSecond => (DayTime, Hour, Second),
        StandardIntervalKind::Minute => (DayTime, Minute, Minute),
        StandardIntervalKind::MinuteToSecond => (DayTime, Minute, Second),
        StandardIntervalKind::Second => (DayTime, Second, Second),
    };
    spec::DataType::Interval {
        interval_unit,
        start_field: Some(start),
        end_field: Some(end),
    }
}

pub fn from_ast_signed_interval(
    value: Signed<IntervalExpr>,
) -> SqlResult<(IntervalValue, Option<spec::DataType>, Option<String>)> {
    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`
    let negated = value.is_negative();
    let interval = value.into_inner();
    match interval.clone() {
        IntervalExpr::Standard { value, qualifier } => {
            let kind = from_ast_interval_qualifier(qualifier)?;
            let ty = standard_interval_kind_to_spec_type(&kind);
            // Preserve the original SQL form (e.g. "INTERVAL '10' YEAR") as the
            // column name so unaliased literals match Spark's output.
            let value_text: Signed<String> = parse_signed_value(value.clone())?;
            let column_name = format!("INTERVAL '{}' {}", value_text.into_inner(), kind);
            Ok((
                from_ast_standard_interval(value, kind, negated)?,
                Some(ty),
                Some(column_name),
            ))
        }
        IntervalExpr::MultiUnit { head, tail } => {
            if tail.is_empty() {
                match head.unit {
                    IntervalUnit::Year(_) | IntervalUnit::Years(_) => {
                        let kind = StandardIntervalKind::Year;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    IntervalUnit::Month(_) | IntervalUnit::Months(_) => {
                        let kind = StandardIntervalKind::Month;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    IntervalUnit::Day(_) | IntervalUnit::Days(_) => {
                        let kind = StandardIntervalKind::Day;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => {
                        let kind = StandardIntervalKind::Hour;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => {
                        let kind = StandardIntervalKind::Minute;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    IntervalUnit::Second(_) | IntervalUnit::Seconds(_) => {
                        let kind = StandardIntervalKind::Second;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_standard_interval(head.value, kind, negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    // WEEK normalizes to DAY (1 week = 7 days).
                    IntervalUnit::Week(_) | IntervalUnit::Weeks(_) => {
                        let kind = StandardIntervalKind::Day;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_multi_unit_interval(vec![head], negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                    // MILLISECOND / MICROSECOND normalize to fractional SECOND.
                    IntervalUnit::Millisecond(_)
                    | IntervalUnit::Milliseconds(_)
                    | IntervalUnit::Microsecond(_)
                    | IntervalUnit::Microseconds(_) => {
                        let kind = StandardIntervalKind::Second;
                        let ty = standard_interval_kind_to_spec_type(&kind);
                        let value = from_ast_multi_unit_interval(vec![head], negated)?;
                        let name = format_interval_column_name(&value, &kind);
                        Ok((value, Some(ty), name))
                    }
                }
            } else {
                // Walk units to determine the canonical (start, end) qualifier.
                // WEEK collapses to DAY, MILLI/MICROSECOND collapse to SECOND.
                // Mixed year-month + day-time falls through to CalendarInterval (None).
                let values: Vec<IntervalValueWithUnit> = once(head).chain(tail).collect();
                use spec::IntervalFieldType::{Day, Hour, Minute, Second};
                let (mut has_year, mut has_month) = (false, false);
                let (mut has_day, mut has_hour, mut has_minute, mut has_second) =
                    (false, false, false, false);
                for u in &values {
                    match u.unit {
                        IntervalUnit::Year(_) | IntervalUnit::Years(_) => has_year = true,
                        IntervalUnit::Month(_) | IntervalUnit::Months(_) => has_month = true,
                        IntervalUnit::Week(_) | IntervalUnit::Weeks(_) => has_day = true,
                        IntervalUnit::Day(_) | IntervalUnit::Days(_) => has_day = true,
                        IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => has_hour = true,
                        IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => has_minute = true,
                        IntervalUnit::Second(_)
                        | IntervalUnit::Seconds(_)
                        | IntervalUnit::Millisecond(_)
                        | IntervalUnit::Milliseconds(_)
                        | IntervalUnit::Microsecond(_)
                        | IntervalUnit::Microseconds(_) => has_second = true,
                    }
                }
                let is_year_month = has_year || has_month;
                let is_day_time = has_day || has_hour || has_minute || has_second;
                let kind = if is_year_month && is_day_time {
                    None
                } else if is_year_month {
                    match (has_year, has_month) {
                        (true, true) => Some(StandardIntervalKind::YearToMonth),
                        (true, false) => Some(StandardIntervalKind::Year),
                        (false, true) => Some(StandardIntervalKind::Month),
                        (false, false) => None,
                    }
                } else {
                    let start = if has_day {
                        Day
                    } else if has_hour {
                        Hour
                    } else if has_minute {
                        Minute
                    } else {
                        Second
                    };
                    let end = if has_second {
                        Second
                    } else if has_minute {
                        Minute
                    } else if has_hour {
                        Hour
                    } else {
                        Day
                    };
                    Some(match (start, end) {
                        (Day, Day) => StandardIntervalKind::Day,
                        (Day, Hour) => StandardIntervalKind::DayToHour,
                        (Day, Minute) => StandardIntervalKind::DayToMinute,
                        (Day, Second) => StandardIntervalKind::DayToSecond,
                        (Hour, Hour) => StandardIntervalKind::Hour,
                        (Hour, Minute) => StandardIntervalKind::HourToMinute,
                        (Hour, Second) => StandardIntervalKind::HourToSecond,
                        (Minute, Minute) => StandardIntervalKind::Minute,
                        (Minute, Second) => StandardIntervalKind::MinuteToSecond,
                        (Second, Second) => StandardIntervalKind::Second,
                        _ => {
                            return Err(SqlError::InternalError(format!(
                                "interval qualifier inference produced an invalid day-time field pair (start={start:?}, end={end:?}) while resolving a multi-unit interval literal in sail-sql-analyzer; this combination should be impossible — open an issue at https://github.com/lakehq/sail with the SQL query that triggered it"
                            )));
                        }
                    })
                };
                let ty = kind.as_ref().map(standard_interval_kind_to_spec_type);
                let value = from_ast_multi_unit_interval(values, negated)?;
                let name = kind
                    .as_ref()
                    .and_then(|k| format_interval_column_name(&value, k));
                Ok((value, ty, name))
            }
        }
        IntervalExpr::Literal(value) => Ok((
            parse_unqualified_interval_string(&from_ast_string(value)?, negated)?,
            None,
            None,
        )),
    }
}

struct DecimalSecond {
    seconds: u32,
    microseconds: u32,
}

impl FromStr for Signed<DecimalSecond> {
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        let error = || SqlError::invalid(format!("second: {s:?}"));
        let captures = INTERVAL_SECOND_REGEX.captures(s).ok_or_else(error)?;
        let negated = captures.name("sign").map(|s| s.as_str()) == Some("-");
        let seconds: u32 = extract_match(&captures, "second", error)?.unwrap_or(0);
        let microseconds: u32 =
            extract_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
        let value = DecimalSecond {
            seconds,
            microseconds,
        };
        if negated {
            Ok(Signed::Negative(value))
        } else {
            Ok(Signed::Positive(value))
        }
    }
}

fn parse_interval_year_month_string(
    s: &str,
    negated: bool,
    interval_regex: &Regex,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let years: i32 = extract_match(&captures, "year", error)?.unwrap_or(0);
    let months: i32 = extract_match(&captures, "month", error)?.unwrap_or(0);
    let n = years
        .checked_mul(12)
        .ok_or_else(error)?
        .checked_add(months)
        .ok_or_else(error)?;
    let n = if negated {
        n.checked_mul(-1).ok_or_else(error)?
    } else {
        n
    };
    Ok(IntervalValue::YearMonth { months: n })
}

fn parse_interval_day_time_string(
    s: &str,
    negated: bool,
    interval_regex: &Regex,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let days: i64 = extract_match(&captures, "day", error)?.unwrap_or(0);
    let hours: i64 = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minutes: i64 = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let seconds: i64 = extract_match(&captures, "second", error)?.unwrap_or(0);
    let microseconds: i64 = extract_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
    let delta = TimeDelta::try_days(days)
        .ok_or_else(error)?
        .checked_add(&TimeDelta::try_hours(hours).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&TimeDelta::try_minutes(minutes).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&TimeDelta::try_seconds(seconds).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&TimeDelta::microseconds(microseconds))
        .ok_or_else(error)?;
    let microseconds = delta.num_microseconds().ok_or_else(error)?;
    let n = if negated {
        microseconds.checked_mul(-1).ok_or_else(error)?
    } else {
        microseconds
    };
    Ok(IntervalValue::Microsecond { microseconds: n })
}

#[derive(Clone, Copy)]
enum StandardIntervalKind {
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

impl std::fmt::Display for StandardIntervalKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StandardIntervalKind::Year => "YEAR",
            StandardIntervalKind::YearToMonth => "YEAR TO MONTH",
            StandardIntervalKind::Month => "MONTH",
            StandardIntervalKind::Day => "DAY",
            StandardIntervalKind::DayToHour => "DAY TO HOUR",
            StandardIntervalKind::DayToMinute => "DAY TO MINUTE",
            StandardIntervalKind::DayToSecond => "DAY TO SECOND",
            StandardIntervalKind::Hour => "HOUR",
            StandardIntervalKind::HourToMinute => "HOUR TO MINUTE",
            StandardIntervalKind::HourToSecond => "HOUR TO SECOND",
            StandardIntervalKind::Minute => "MINUTE",
            StandardIntervalKind::MinuteToSecond => "MINUTE TO SECOND",
            StandardIntervalKind::Second => "SECOND",
        })
    }
}

fn from_ast_interval_qualifier(qualifier: IntervalQualifier) -> SqlResult<StandardIntervalKind> {
    match qualifier {
        IntervalQualifier::YearMonth(IntervalYearMonthUnit::Year(_), None) => {
            Ok(StandardIntervalKind::Year)
        }
        IntervalQualifier::YearMonth(
            IntervalYearMonthUnit::Year(_),
            Some((_, IntervalYearMonthUnit::Month(_))),
        ) => Ok(StandardIntervalKind::YearToMonth),
        IntervalQualifier::YearMonth(IntervalYearMonthUnit::Month(_), None) => {
            Ok(StandardIntervalKind::Month)
        }
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Day(_), None) => {
            Ok(StandardIntervalKind::Day)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Hour(_))),
        ) => Ok(StandardIntervalKind::DayToHour),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Minute(_))),
        ) => Ok(StandardIntervalKind::DayToMinute),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::DayToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Hour(_), None) => {
            Ok(StandardIntervalKind::Hour)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Hour(_),
            Some((_, IntervalDayTimeUnit::Minute(_))),
        ) => Ok(StandardIntervalKind::HourToMinute),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Hour(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::HourToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Minute(_), None) => {
            Ok(StandardIntervalKind::Minute)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Minute(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::MinuteToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Second(_), None) => {
            Ok(StandardIntervalKind::Second)
        }
        _ => Err(SqlError::invalid("interval qualifier")),
    }
}

fn from_ast_standard_interval(
    value: Expr,
    kind: StandardIntervalKind,
    negated: bool,
) -> SqlResult<IntervalValue> {
    let signed: Signed<String> = parse_signed_value(value)?;
    let negated = signed.is_negative() ^ negated;
    let value = signed.into_inner();
    match kind {
        StandardIntervalKind::Year => {
            parse_interval_year_month_string(&value, negated, &INTERVAL_YEAR_REGEX)
        }
        StandardIntervalKind::YearToMonth => {
            parse_interval_year_month_string(&value, negated, &INTERVAL_YEAR_TO_MONTH_REGEX)
        }
        StandardIntervalKind::Month => {
            parse_interval_year_month_string(&value, negated, &INTERVAL_MONTH_REGEX)
        }
        StandardIntervalKind::Day => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_REGEX)
        }
        StandardIntervalKind::DayToHour => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_TO_HOUR_REGEX)
        }
        StandardIntervalKind::DayToMinute => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_TO_MINUTE_REGEX)
        }
        StandardIntervalKind::DayToSecond => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_TO_SECOND_REGEX)
        }
        StandardIntervalKind::Hour => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_HOUR_REGEX)
        }
        StandardIntervalKind::HourToMinute => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_HOUR_TO_MINUTE_REGEX)
        }
        StandardIntervalKind::HourToSecond => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_HOUR_TO_SECOND_REGEX)
        }
        StandardIntervalKind::Minute => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_MINUTE_REGEX)
        }
        StandardIntervalKind::MinuteToSecond => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_MINUTE_TO_SECOND_REGEX)
        }
        StandardIntervalKind::Second => {
            parse_interval_day_time_string(&value, negated, &INTERVAL_SECOND_REGEX)
        }
    }
}

fn from_ast_multi_unit_interval(
    values: Vec<IntervalValueWithUnit>,
    negated: bool,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut delta = TimeDelta::zero();
    for value in values {
        let IntervalValueWithUnit { value, unit } = value;
        match unit {
            IntervalUnit::Year(_) | IntervalUnit::Years(_) => {
                let value: i32 = parse_signed_value(value)?;
                let m = value.checked_mul(12).ok_or_else(error)?;
                months = months.checked_add(m).ok_or_else(error)?;
            }
            IntervalUnit::Month(_) | IntervalUnit::Months(_) => {
                let value: i32 = parse_signed_value(value)?;
                months = months.checked_add(value).ok_or_else(error)?;
            }
            IntervalUnit::Week(_) | IntervalUnit::Weeks(_) => {
                let value: i64 = parse_signed_value(value)?;
                let weeks = TimeDelta::try_weeks(value).ok_or_else(error)?;
                delta = delta.checked_add(&weeks).ok_or_else(error)?;
            }
            IntervalUnit::Day(_) | IntervalUnit::Days(_) => {
                let value: i64 = parse_signed_value(value)?;
                let days = TimeDelta::try_days(value).ok_or_else(error)?;
                delta = delta.checked_add(&days).ok_or_else(error)?;
            }
            IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => {
                let value: i64 = parse_signed_value(value)?;
                let hours = TimeDelta::try_hours(value).ok_or_else(error)?;
                delta = delta.checked_add(&hours).ok_or_else(error)?;
            }
            IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => {
                let value: i64 = parse_signed_value(value)?;
                let minutes = TimeDelta::try_minutes(value).ok_or_else(error)?;
                delta = delta.checked_add(&minutes).ok_or_else(error)?;
            }
            IntervalUnit::Second(_) | IntervalUnit::Seconds(_) => {
                let value: Signed<DecimalSecond> = parse_signed_value(value)?;
                let negated = value.is_negative();
                let value = value.into_inner();
                let seconds = TimeDelta::seconds(value.seconds as i64);
                let microseconds = TimeDelta::microseconds(value.microseconds as i64);
                if negated {
                    delta = delta.checked_sub(&seconds).ok_or_else(error)?;
                    delta = delta.checked_sub(&microseconds).ok_or_else(error)?;
                } else {
                    delta = delta.checked_add(&seconds).ok_or_else(error)?;
                    delta = delta.checked_add(&microseconds).ok_or_else(error)?;
                }
            }
            IntervalUnit::Millisecond(_) | IntervalUnit::Milliseconds(_) => {
                let value: i64 = parse_signed_value(value)?;
                let milliseconds = TimeDelta::try_milliseconds(value).ok_or_else(error)?;
                delta = delta.checked_add(&milliseconds).ok_or_else(error)?;
            }
            IntervalUnit::Microsecond(_) | IntervalUnit::Microseconds(_) => {
                let value: i64 = parse_signed_value(value)?;
                let microseconds = TimeDelta::microseconds(value);
                delta = delta.checked_add(&microseconds).ok_or_else(error)?;
            }
        }
    }
    match (months != 0, delta != TimeDelta::zero()) {
        (true, false) => {
            let n = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            Ok(IntervalValue::YearMonth { months: n })
        }
        (true, true) => {
            let days = delta.num_days();
            let remainder = delta - chrono::Duration::days(days);
            let microseconds = remainder.num_microseconds().ok_or_else(error)?;

            let months = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            let days = if negated {
                days.checked_mul(-1).ok_or_else(error)?
            } else {
                days
            };
            let days = i32::try_from(days).map_err(|_| {
                SqlError::invalid(format!("Days value out of range for i32: {days}"))
            })?;
            let microseconds = if negated {
                microseconds.checked_mul(-1).ok_or_else(error)?
            } else {
                microseconds
            };
            let nanoseconds = microseconds * 1_000;

            Ok(IntervalValue::MonthDayNanosecond {
                months,
                days,
                nanoseconds,
            })
        }
        (false, _) => {
            let microseconds = delta.num_microseconds().ok_or_else(error)?;
            let n = if negated {
                microseconds.checked_mul(-1).ok_or_else(error)?
            } else {
                microseconds
            };
            Ok(IntervalValue::Microsecond { microseconds: n })
        }
    }
}

pub(crate) fn parse_unqualified_interval_string(
    s: &str,
    negated: bool,
) -> SqlResult<IntervalValue> {
    let IntervalLiteral {
        interval: _,
        value: interval,
    } = parse_interval_literal(s)?;
    let value = if negated {
        Signed::Negative(interval)
    } else {
        Signed::Positive(interval)
    };
    // Unqualified strings (e.g. `INTERVAL '1 month'`) carry no qualifier by
    // definition, and all external callers (`parse_interval`, runtime UDFs,
    // delta property parsing) only need the numeric value — so we drop the
    // qualifier type.
    from_ast_signed_interval(value).map(|(v, _, _)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval() -> SqlResult<()> {
        let parse = parse_unqualified_interval_string;

        assert!(parse("178956970 year 7 month", false).is_ok());
        assert!(parse("178956970 year 7 month", true).is_ok());
        assert!(parse("178956970 year 8 month", false).is_err());
        assert!(parse("178956970 year 8 month", true).is_err());
        assert!(parse("-178956970 year -8 month", false).is_ok());
        assert!(parse("-178956970 year -8 month", true).is_err());
        assert!(parse("-178956970 year -9 month", false).is_err());
        assert!(parse("-178956970 year -9 month", true).is_err());

        assert!(parse("'178956970-7' year to month", false).is_ok());
        assert!(parse("'178956970-7' year to month", true).is_ok());
        assert!(parse("'178956970-8' year to month", false).is_err());
        assert!(parse("'178956970-8' year to month", true).is_err());
        assert!(parse("-'178956970-8' year to month", false).is_err());
        assert!(parse("-'178956970-8' year to month", true).is_err());
        assert!(parse("-'178956970-9' year to month", false).is_err());
        assert!(parse("-'178956970-9' year to month", true).is_err());

        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("'2-1' year to month", true)?
        );
        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("-'2-1' year to month", false)?
        );
        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("-2 year -1 month", false)?
        );

        assert!(parse("106751991 day 14454775807 microsecond", false).is_ok());
        assert!(parse("106751991 day 14454775807 microsecond", true).is_ok());
        assert!(parse("106751991 day 14454775808 microsecond", false).is_err());
        assert!(parse("106751991 day 14454775808 microsecond", true).is_err());
        assert!(parse("-106751991 day -14454775808 microsecond", false).is_ok());
        assert!(parse("-106751991 day -14454775808 microsecond", true).is_err());
        assert!(parse("-106751991 day -14454775809 microsecond", false).is_err());
        assert!(parse("-106751991 day -14454775809 microsecond", true).is_err());

        assert!(parse("'106751991 04:00:54.775807' day to second", false).is_ok());
        assert!(parse("'106751991 04:00:54.775807' day to second", true).is_ok());
        assert!(parse("'106751991 04:00:54.775808' day to second", false).is_err());
        assert!(parse("'106751991 04:00:54.775808' day to second", true).is_err());
        assert!(parse("-'106751991 04:00:54.775808' day to second", false).is_err());
        assert!(parse("-'106751991 04:00:54.775808' day to second", true).is_err());
        assert!(parse("-'106751991 04:00:54.775809' day to second", false).is_err());
        assert!(parse("-'106751991 04:00:54.775809' day to second", true).is_err());

        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse("'1 2:3:4.567890' day to second", true)?
        );
        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse("-'1 2:3:4.567890' day to second", false)?
        );
        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse(
                "-1 day -2 hour -3 minute -4 second -567 millisecond -890 microsecond",
                false
            )?
        );
        Ok(())
    }

    #[test]
    fn test_parse_unqualified_interval_string() -> SqlResult<()> {
        assert!(parse_unqualified_interval_string("1", false).is_err());
        assert!(parse_unqualified_interval_string("1 month", false).is_ok());
        assert_eq!(
            parse_unqualified_interval_string("1 month", true)?,
            parse_unqualified_interval_string("-1 month", false)?
        );
        assert_eq!(
            parse_unqualified_interval_string("1 hour 2 seconds", false)?,
            parse_unqualified_interval_string("-1 hour -2 seconds", true)?
        );
        Ok(())
    }
}
