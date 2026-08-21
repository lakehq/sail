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
use crate::literal::utils::{Signed, extract_fraction_match, extract_match, parse_signed_value};
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

pub fn from_ast_signed_interval(value: Signed<IntervalExpr>) -> SqlResult<IntervalValue> {
    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`
    let negated = value.is_negative();
    let interval = value.into_inner();
    match interval.clone() {
        IntervalExpr::Standard { value, qualifier } => {
            let kind = from_ast_interval_qualifier(qualifier)?;
            from_ast_standard_interval(value, kind, negated)
        }
        IntervalExpr::MultiUnit { head, tail } => {
            if tail.is_empty() {
                match head.unit {
                    IntervalUnit::Year(_) | IntervalUnit::Years(_) => {
                        from_ast_standard_interval(head.value, StandardIntervalKind::Year, negated)
                    }
                    IntervalUnit::Month(_) | IntervalUnit::Months(_) => {
                        from_ast_standard_interval(head.value, StandardIntervalKind::Month, negated)
                    }
                    IntervalUnit::Day(_) | IntervalUnit::Days(_) => {
                        from_ast_standard_interval(head.value, StandardIntervalKind::Day, negated)
                    }
                    IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => {
                        from_ast_standard_interval(head.value, StandardIntervalKind::Hour, negated)
                    }
                    IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => {
                        from_ast_standard_interval(
                            head.value,
                            StandardIntervalKind::Minute,
                            negated,
                        )
                    }
                    IntervalUnit::Second(_) | IntervalUnit::Seconds(_) => {
                        from_ast_standard_interval(
                            head.value,
                            StandardIntervalKind::Second,
                            negated,
                        )
                    }
                    _ => from_ast_multi_unit_interval(vec![head], negated),
                }
            } else {
                let values = once(head).chain(tail).collect();
                from_ast_multi_unit_interval(values, negated)
            }
        }
        IntervalExpr::Literal(value) => {
            parse_unqualified_interval_string(&from_ast_string(value)?, negated)
        }
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
    // The full parser rebuilds its combinator graph on every call — too costly
    // for per-row use. Common strings take the fast path; everything else falls
    // through, so accepted syntax and errors are unchanged.
    if let Some(value) = parse_unqualified_interval_string_fast(s, negated) {
        return Ok(value);
    }
    parse_unqualified_interval_string_full(s, negated)
}

fn parse_unqualified_interval_string_full(s: &str, negated: bool) -> SqlResult<IntervalValue> {
    let IntervalLiteral {
        interval: _,
        value: interval,
    } = parse_interval_literal(s)?;
    let value = if negated {
        Signed::Negative(interval)
    } else {
        Signed::Positive(interval)
    };
    from_ast_signed_interval(value)
}

/// A calendar interval with Spark `stringToInterval` bucketing: the unit the
/// user wrote decides the bucket (year/month → `months`, week/day → `days`,
/// sub-day units → `microseconds`), and nothing is rebucketed across the day
/// boundary. This is the semantics `session_window`/`window` gaps need — a
/// `'1 day'` gap spans a calendar day across a DST transition while a
/// `'25 hours'` gap spans 25 absolute hours. The typed SQL literal paths keep
/// the legacy [IntervalValue] shapes from [parse_unqualified_interval_string].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalendarInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

pub fn parse_calendar_interval_string(s: &str) -> SqlResult<CalendarInterval> {
    if let Some(value) = parse_calendar_interval_string_fast(s) {
        return Ok(value);
    }
    let IntervalLiteral {
        interval: _,
        value: interval,
    } = parse_interval_literal(s)?;
    match interval {
        IntervalExpr::MultiUnit { head, tail } => {
            from_ast_multi_unit_calendar(std::iter::once(head).chain(tail))
        }
        // Shapes Spark's `stringToInterval` does not accept (qualified forms
        // like `'1 2:03:04' day to second`); keep accepting them with the
        // legacy conversion, whose day-time part is absolute microseconds.
        other => match from_ast_signed_interval(Signed::Positive(other))? {
            IntervalValue::YearMonth { months } => Ok(CalendarInterval {
                months,
                days: 0,
                microseconds: 0,
            }),
            IntervalValue::Microsecond { microseconds } => Ok(CalendarInterval {
                months: 0,
                days: 0,
                microseconds,
            }),
            IntervalValue::MonthDayNanosecond {
                months,
                days,
                nanoseconds,
            } => Ok(CalendarInterval {
                months,
                days,
                microseconds: nanoseconds / 1_000,
            }),
        },
    }
}

/// Per-unit bucketing over the same term scanner as the fast interval parser;
/// declines (`None`) anything the scanner does not recognize.
fn parse_calendar_interval_string_fast(s: &str) -> Option<CalendarInterval> {
    let mut words = s.split_ascii_whitespace().peekable();
    if words
        .peek()
        .is_some_and(|w| w.eq_ignore_ascii_case("interval"))
    {
        words.next();
    }
    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut delta = TimeDelta::zero();
    let mut seen = false;
    while let Some(value_word) = words.next() {
        let (neg, int_part, fraction) = parse_value_word(value_word)?;
        let unit = parse_unit_word(words.next()?)?;
        if fraction.is_some() && unit != Unit::Second {
            return None;
        }
        seen = true;
        use Unit::*;
        match unit {
            Year | Month => {
                let mut value: i32 = int_part.parse().ok()?;
                if neg {
                    value = value.checked_neg()?;
                }
                if matches!(unit, Year) {
                    value = value.checked_mul(12)?;
                }
                months = months.checked_add(value)?;
            }
            Week | Day => {
                let mut value: i32 = int_part.parse().ok()?;
                if neg {
                    value = value.checked_neg()?;
                }
                if matches!(unit, Week) {
                    value = value.checked_mul(7)?;
                }
                days = days.checked_add(value)?;
            }
            Hour | Minute | Second | Millisecond | Microsecond => {
                let mut value: i64 = int_part.parse().ok()?;
                if neg {
                    value = value.checked_neg()?;
                }
                let part = match unit {
                    Hour => TimeDelta::try_hours(value)?,
                    Minute => TimeDelta::try_minutes(value)?,
                    Second => TimeDelta::try_seconds(value)?.checked_add(
                        &TimeDelta::microseconds(if neg {
                            fraction_microseconds(fraction)?.checked_neg()?
                        } else {
                            fraction_microseconds(fraction)?
                        }),
                    )?,
                    Millisecond => TimeDelta::try_milliseconds(value)?,
                    _ => TimeDelta::microseconds(value),
                };
                delta = delta.checked_add(&part)?;
            }
        }
    }
    if !seen {
        return None;
    }
    Some(CalendarInterval {
        months,
        days,
        microseconds: delta.num_microseconds()?,
    })
}

/// AST-side counterpart of [parse_calendar_interval_string_fast] for strings
/// the fast scanner declines.
fn from_ast_multi_unit_calendar(
    values: impl Iterator<Item = IntervalValueWithUnit>,
) -> SqlResult<CalendarInterval> {
    let error = || SqlError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut days = 0i32;
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
                let value: i32 = parse_signed_value(value)?;
                let d = value.checked_mul(7).ok_or_else(error)?;
                days = days.checked_add(d).ok_or_else(error)?;
            }
            IntervalUnit::Day(_) | IntervalUnit::Days(_) => {
                let value: i32 = parse_signed_value(value)?;
                days = days.checked_add(value).ok_or_else(error)?;
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
    Ok(CalendarInterval {
        months,
        days,
        microseconds: delta.num_microseconds().ok_or_else(error)?,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Unit {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

fn parse_unit_word(word: &str) -> Option<Unit> {
    use Unit::*;
    for (names, unit) in [
        (["year", "years"], Year),
        (["month", "months"], Month),
        (["week", "weeks"], Week),
        (["day", "days"], Day),
        (["hour", "hours"], Hour),
        (["minute", "minutes"], Minute),
        (["second", "seconds"], Second),
        (["millisecond", "milliseconds"], Millisecond),
        (["microsecond", "microseconds"], Microsecond),
    ] {
        if names.iter().any(|n| word.eq_ignore_ascii_case(n)) {
            return Some(unit);
        }
    }
    None
}

/// Splits a value word into (negated, integer digits, fraction digits).
/// Accepts only `-?digits(.digits)?`; anything else (`+`, a detached sign, a
/// trailing dot) is declined so the full parser decides.
fn parse_value_word(word: &str) -> Option<(bool, &str, Option<&str>)> {
    let (negated, rest) = match word.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, word),
    };
    let (int_part, fraction) = match rest.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (rest, None),
    };
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(f) = fraction
        && (f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some((negated, int_part, fraction))
}

/// Converts fraction digits of a second to microseconds the same way
/// [extract_fraction_match] does: pad with zeros to 6 digits, ignore the rest.
fn fraction_microseconds(fraction: Option<&str>) -> Option<i64> {
    match fraction {
        None => Some(0),
        Some(f) => f
            .chars()
            .chain(std::iter::repeat('0'))
            .take(6)
            .collect::<String>()
            .parse::<i64>()
            .ok(),
    }
}

/// Fast parser for the common `[interval] (value unit)+` strings, e.g.
/// `5 minutes`, `-2 days`, `1.5 seconds`. Anything else (quoted values, `+`,
/// qualifiers like `day to second`) returns `None` and the caller falls back
/// to the full parser. Accepted strings reproduce [from_ast_signed_interval]
/// exactly, quirks included: a single year/month term is year-month even when
/// zero, single-term seconds are `i64` but multi-term `u32`, and overflow is
/// declined so the full parser reports the error.
fn parse_unqualified_interval_string_fast(s: &str, negated: bool) -> Option<IntervalValue> {
    let mut words = s.split_ascii_whitespace().peekable();
    if words
        .peek()
        .is_some_and(|w| w.eq_ignore_ascii_case("interval"))
    {
        words.next();
    }
    let mut terms: Vec<(bool, &str, Option<&str>, Unit)> = Vec::new();
    while let Some(value_word) = words.next() {
        let (neg, int_part, fraction) = parse_value_word(value_word)?;
        let unit = parse_unit_word(words.next()?)?;
        if fraction.is_some() && unit != Unit::Second {
            return None;
        }
        terms.push((neg, int_part, fraction, unit));
    }

    // A single term follows the standard-interval path for its units;
    // everything else accumulates as multi-unit.
    if let [(neg, int_part, fraction, unit)] = terms[..] {
        use Unit::*;
        let negated = neg ^ negated;
        match unit {
            Year | Month => {
                let value: i32 = int_part.parse().ok()?;
                let mut months = match unit {
                    Year => value.checked_mul(12)?,
                    _ => value,
                };
                if negated {
                    months = months.checked_neg()?;
                }
                return Some(IntervalValue::YearMonth { months });
            }
            Day | Hour | Minute | Second => {
                let value: i64 = int_part.parse().ok()?;
                let delta = match unit {
                    Day => TimeDelta::try_days(value)?,
                    Hour => TimeDelta::try_hours(value)?,
                    Minute => TimeDelta::try_minutes(value)?,
                    _ => TimeDelta::try_seconds(value)?
                        .checked_add(&TimeDelta::microseconds(fraction_microseconds(fraction)?))?,
                };
                let mut microseconds = delta.num_microseconds()?;
                if negated {
                    microseconds = microseconds.checked_neg()?;
                }
                return Some(IntervalValue::Microsecond { microseconds });
            }
            Week | Millisecond | Microsecond => {}
        }
    } else if terms.is_empty() {
        return None;
    }

    let mut months: i32 = 0;
    let mut delta = TimeDelta::zero();
    for (neg, int_part, fraction, unit) in &terms {
        use Unit::*;
        match unit {
            Year | Month => {
                let mut value: i32 = int_part.parse().ok()?;
                if *neg {
                    value = value.checked_neg()?;
                }
                if matches!(unit, Year) {
                    value = value.checked_mul(12)?;
                }
                months = months.checked_add(value)?;
            }
            Second => {
                // Multi-unit seconds are `u32` (`DecimalSecond`).
                let seconds: u32 = int_part.parse().ok()?;
                let seconds = TimeDelta::seconds(seconds as i64);
                let microseconds = TimeDelta::microseconds(fraction_microseconds(*fraction)?);
                if *neg {
                    delta = delta.checked_sub(&seconds)?.checked_sub(&microseconds)?;
                } else {
                    delta = delta.checked_add(&seconds)?.checked_add(&microseconds)?;
                }
            }
            _ => {
                let mut value: i64 = int_part.parse().ok()?;
                if *neg {
                    value = value.checked_neg()?;
                }
                let part = match unit {
                    Week => TimeDelta::try_weeks(value)?,
                    Day => TimeDelta::try_days(value)?,
                    Hour => TimeDelta::try_hours(value)?,
                    Minute => TimeDelta::try_minutes(value)?,
                    Millisecond => TimeDelta::try_milliseconds(value)?,
                    _ => TimeDelta::microseconds(value),
                };
                delta = delta.checked_add(&part)?;
            }
        }
    }
    match (months != 0, delta != TimeDelta::zero()) {
        (true, false) => {
            if negated {
                months = months.checked_neg()?;
            }
            Some(IntervalValue::YearMonth { months })
        }
        (true, true) => {
            let mut days = delta.num_days();
            let remainder = delta - chrono::Duration::days(days);
            let mut microseconds = remainder.num_microseconds()?;
            if negated {
                months = months.checked_neg()?;
                days = days.checked_neg()?;
                microseconds = microseconds.checked_neg()?;
            }
            let days = i32::try_from(days).ok()?;
            Some(IntervalValue::MonthDayNanosecond {
                months,
                days,
                nanoseconds: microseconds * 1_000,
            })
        }
        (false, _) => {
            let mut microseconds = delta.num_microseconds()?;
            if negated {
                microseconds = microseconds.checked_neg()?;
            }
            Some(IntervalValue::Microsecond { microseconds })
        }
    }
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
    fn test_fast_path_matches_full_parser() {
        // Fast-path values must equal the full parser's; declined strings
        // reach the full parser anyway. The list covers accepted shapes plus
        // ones that must decline (quotes, `+`, qualifiers, junk, overflow).
        let cases = [
            "5 minutes",
            "2 minutes",
            "1 second",
            "1.5 seconds",
            "-1.5 seconds",
            "1. seconds",
            "0.000001 seconds",
            "1.1234567 seconds",
            "1 month",
            "0 month",
            "-0 month",
            "0 year",
            "3 years",
            "0 day",
            "0 seconds",
            "1 week",
            "-2 weeks",
            "10 milliseconds",
            "7 microseconds",
            "1 month 2 days",
            "1 hour 2 seconds",
            "-1 hour -2 seconds",
            "1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds",
            "1 day -2 hours",
            "interval 5 minutes",
            "INTERVAL 3 HOURS",
            "  5   MINUTES  ",
            "007 days",
            "2147483647 months",
            "-2147483648 month",
            "178956970 year 7 month",
            "178956970 year 8 month",
            "106751991 day 14454775807 microsecond",
            "106751991 day 14454775808 microsecond",
            "5000000000 seconds",
            "1 day 5000000000 seconds",
            "'5' minutes",
            "+5 minutes",
            "- 5 minutes",
            "'1 1' day to hour",
            "'178956970-7' year to month",
            "5",
            "minutes",
            "5 fortnights",
            "1e3 days",
            "",
        ];
        for s in cases {
            for negated in [false, true] {
                let full = parse_unqualified_interval_string_full(s, negated);
                if let Some(fast) = parse_unqualified_interval_string_fast(s, negated) {
                    assert!(
                        full.is_ok(),
                        "fast path accepts {s:?} (negated={negated}) but the full parser errors: {full:?}"
                    );
                    if let Ok(full) = full {
                        assert_eq!(
                            fast, full,
                            "fast path diverges for {s:?} (negated={negated})"
                        );
                    }
                }
            }
        }
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

    /// Spark `stringToInterval` bucketing: the unit written decides the
    /// bucket; sub-day amounts are never rebucketed into days (and days never
    /// collapse into microseconds).
    #[test]
    fn test_calendar_interval_bucketing() -> SqlResult<()> {
        const HOUR: i64 = 3_600_000_000;
        for (s, months, days, micros) in [
            ("1 day", 0, 1, 0),
            ("interval 1 day", 0, 1, 0),
            ("25 hours", 0, 0, 25 * HOUR),
            ("1 day 2 hours", 0, 1, 2 * HOUR),
            ("2 weeks", 0, 14, 0),
            ("-2 days", 0, -2, 0),
            ("1 month -30 days", 1, -30, 0),
            ("1 month 25 hours", 1, 0, 25 * HOUR),
            ("1.5 seconds", 0, 0, 1_500_000),
            ("-1.5 seconds", 0, 0, -1_500_000),
            ("1 year 1 microsecond", 12, 0, 1),
        ] {
            let v = parse_calendar_interval_string(s)?;
            assert_eq!(
                (v.months, v.days, v.microseconds),
                (months, days, micros),
                "{s}"
            );
        }
        assert!(parse_calendar_interval_string("garbage").is_err());
        Ok(())
    }

    /// The fast scanner and the AST fallback agree; exercise the fallback via
    /// a quoted value the scanner declines.
    #[test]
    fn test_calendar_interval_fallback_matches_fast() -> SqlResult<()> {
        let fast = parse_calendar_interval_string("1 day 2 hours")?;
        let full = parse_calendar_interval_string("'1' day '2' hours")?;
        assert_eq!(fast, full);
        Ok(())
    }
}
