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
        start_field: Option<spec::IntervalFieldType>,
        end_field: Option<spec::IntervalFieldType>,
    },
    Microsecond {
        microseconds: i64,
        start_field: Option<spec::IntervalFieldType>,
        end_field: Option<spec::IntervalFieldType>,
    },
    MonthDayNanosecond {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
}

impl IntervalValue {
    /// Records the leading and trailing fields of the interval qualifier that produced this value.
    /// The fields are not part of the value itself, but Spark keeps them in the interval type.
    fn with_fields(self, start: spec::IntervalFieldType, end: spec::IntervalFieldType) -> Self {
        match self {
            IntervalValue::YearMonth { months, .. } => IntervalValue::YearMonth {
                months,
                start_field: Some(start),
                end_field: Some(end),
            },
            IntervalValue::Microsecond { microseconds, .. } => IntervalValue::Microsecond {
                microseconds,
                start_field: Some(start),
                end_field: Some(end),
            },
            x @ IntervalValue::MonthDayNanosecond { .. } => x,
        }
    }
}

impl From<IntervalValue> for spec::Literal {
    fn from(value: IntervalValue) -> Self {
        match value {
            IntervalValue::YearMonth {
                months,
                start_field,
                end_field,
            } => spec::Literal::IntervalYearMonth {
                months: Some(months),
                start_field,
                end_field,
            },
            IntervalValue::Microsecond {
                microseconds,
                start_field,
                end_field,
            } => spec::Literal::DurationMicrosecond {
                microseconds: Some(microseconds),
                start_field,
                end_field,
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

/// Whether a multi-unit interval may mix year-month units with day-time units.
///
/// Spark rejects the mix for an ANSI interval literal, but accepts it for the legacy calendar
/// interval string form, which yields a `CalendarInterval` spanning both families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixedIntervalUnits {
    Allow,
    Reject,
}

pub fn from_ast_signed_interval(
    value: Signed<IntervalExpr>,
    mixed_units: MixedIntervalUnits,
) -> SqlResult<IntervalValue> {
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
                    _ => from_ast_multi_unit_interval(vec![head], negated, mixed_units),
                }
            } else {
                let values = once(head).chain(tail).collect();
                from_ast_multi_unit_interval(values, negated, mixed_units)
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
    Ok(IntervalValue::YearMonth {
        months: n,
        start_field: None,
        end_field: None,
    })
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
    Ok(IntervalValue::Microsecond {
        microseconds: n,
        start_field: None,
        end_field: None,
    })
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

impl StandardIntervalKind {
    /// The leading and trailing fields of the resulting Spark interval type. A qualifier with a
    /// single field spans that field only, matching `YearMonthIntervalType.apply(field)` and
    /// `DayTimeIntervalType.apply(field)` in Spark.
    fn fields(&self) -> (spec::IntervalFieldType, spec::IntervalFieldType) {
        use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};

        match self {
            StandardIntervalKind::Year => (Year, Year),
            StandardIntervalKind::YearToMonth => (Year, Month),
            StandardIntervalKind::Month => (Month, Month),
            StandardIntervalKind::Day => (Day, Day),
            StandardIntervalKind::DayToHour => (Day, Hour),
            StandardIntervalKind::DayToMinute => (Day, Minute),
            StandardIntervalKind::DayToSecond => (Day, Second),
            StandardIntervalKind::Hour => (Hour, Hour),
            StandardIntervalKind::HourToMinute => (Hour, Minute),
            StandardIntervalKind::HourToSecond => (Hour, Second),
            StandardIntervalKind::Minute => (Minute, Minute),
            StandardIntervalKind::MinuteToSecond => (Minute, Second),
            StandardIntervalKind::Second => (Second, Second),
        }
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
    let (start_field, end_field) = kind.fields();
    let interval = match kind {
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
    }?;
    Ok(interval.with_fields(start_field, end_field))
}

/// The Spark interval field that a multi-unit keyword contributes to. Sub-day units below the
/// second and the week both fold into a coarser field, since Spark has no field for them.
fn interval_unit_field(unit: &IntervalUnit) -> spec::IntervalFieldType {
    use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};

    match unit {
        IntervalUnit::Year(_) | IntervalUnit::Years(_) => Year,
        IntervalUnit::Month(_) | IntervalUnit::Months(_) => Month,
        IntervalUnit::Week(_)
        | IntervalUnit::Weeks(_)
        | IntervalUnit::Day(_)
        | IntervalUnit::Days(_) => Day,
        IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => Hour,
        IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => Minute,
        IntervalUnit::Second(_)
        | IntervalUnit::Seconds(_)
        | IntervalUnit::Millisecond(_)
        | IntervalUnit::Milliseconds(_)
        | IntervalUnit::Microsecond(_)
        | IntervalUnit::Microseconds(_) => Second,
    }
}

/// The interval spans from the coarsest to the finest field that its units mention, regardless of
/// the order they are written in.
fn interval_field_span(
    fields: &[spec::IntervalFieldType],
) -> Option<(spec::IntervalFieldType, spec::IntervalFieldType)> {
    Some((*fields.iter().min()?, *fields.iter().max()?))
}

fn from_ast_multi_unit_interval(
    values: Vec<IntervalValueWithUnit>,
    negated: bool,
    mixed_units: MixedIntervalUnits,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut delta = TimeDelta::zero();
    let mut year_month_fields = vec![];
    let mut day_time_fields = vec![];
    for value in values {
        let IntervalValueWithUnit { value, unit } = value;
        let field = interval_unit_field(&unit);
        if matches!(
            field,
            spec::IntervalFieldType::Year | spec::IntervalFieldType::Month
        ) {
            year_month_fields.push(field);
        } else {
            day_time_fields.push(field);
        }
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
    if mixed_units == MixedIntervalUnits::Reject && !year_month_fields.is_empty() {
        // Spark selects the interval family from the units that are written, not from the value
        // they add up to, and rejects an ANSI interval literal that mixes the two families.
        if !day_time_fields.is_empty() {
            return Err(SqlError::invalid(
                "Cannot mix year-month and day-time fields in an interval",
            ));
        }
        let n = if negated {
            months.checked_mul(-1).ok_or_else(error)?
        } else {
            months
        };
        let interval = IntervalValue::YearMonth {
            months: n,
            start_field: None,
            end_field: None,
        };
        return Ok(match interval_field_span(&year_month_fields) {
            Some((start, end)) => interval.with_fields(start, end),
            None => interval,
        });
    }
    match (months != 0, delta != TimeDelta::zero()) {
        (true, false) => {
            let n = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            let interval = IntervalValue::YearMonth {
                months: n,
                start_field: None,
                end_field: None,
            };
            Ok(match interval_field_span(&year_month_fields) {
                Some((start, end)) => interval.with_fields(start, end),
                None => interval,
            })
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
            let interval = IntervalValue::Microsecond {
                microseconds: n,
                start_field: None,
                end_field: None,
            };
            Ok(match interval_field_span(&day_time_fields) {
                Some((start, end)) => interval.with_fields(start, end),
                None => interval,
            })
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
    // The unqualified string form is the legacy calendar interval, which may span both families.
    from_ast_signed_interval(value, MixedIntervalUnits::Allow)
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
