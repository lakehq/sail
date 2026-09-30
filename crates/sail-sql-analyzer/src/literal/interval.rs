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
    // Spark's `IntervalUtils.castStringToDTInterval` tries the compact form
    // (`"1 02:03:04"`) first, then this "literal" form -- the same text a
    // day-time interval's own `CAST(... AS STRING)` produces -- so that
    // casting an interval to STRING and back round-trips.
    static ref INTERVAL_LITERAL_WRAPPER_REGEX: Regex = create_regex(Regex::new(
        r"(?i)^\s*INTERVAL\s+([+-]?)'(.*)'\s+(\w+(?:\s+TO\s+\w+)?)\s*$"
    ));
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IntervalValue {
    YearMonth {
        months: i32,
        start_field: spec::IntervalFieldType,
        end_field: Option<spec::IntervalFieldType>,
    },
    Microsecond {
        microseconds: i64,
        start_field: spec::IntervalFieldType,
        end_field: Option<spec::IntervalFieldType>,
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
            IntervalValue::YearMonth {
                months,
                start_field,
                end_field,
            } => spec::Literal::IntervalYearMonth {
                months: Some(months),
                start_field: Some(start_field),
                end_field,
            },
            IntervalValue::Microsecond {
                microseconds,
                start_field,
                end_field,
            } => spec::Literal::IntervalDayTimeMicrosecond {
                microseconds: Some(microseconds),
                start_field: Some(start_field),
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
    start_field: spec::IntervalFieldType,
    end_field: Option<spec::IntervalFieldType>,
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
        start_field,
        end_field,
    })
}

fn parse_interval_day_time_string(
    s: &str,
    negated: bool,
    interval_regex: &Regex,
    start_field: spec::IntervalFieldType,
    end_field: Option<spec::IntervalFieldType>,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let days: i64 = extract_match(&captures, "day", error)?.unwrap_or(0);
    let hours: i64 = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minutes: i64 = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let seconds: i64 = extract_match(&captures, "second", error)?.unwrap_or(0);
    let microseconds: i64 = extract_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
    // Keep the calculation signed in i128 until the final range check.  The
    // negative endpoint has one more representable value than the positive
    // endpoint: `i64::MIN` has a magnitude of `i64::MAX + 1`.  Building a
    // positive `TimeDelta` and negating it afterwards therefore rejects an
    // otherwise valid negative interval at exactly that endpoint.
    let sign = if negated { -1_i128 } else { 1_i128 };
    let n = [
        (days, 86_400_000_000_i128),
        (hours, 3_600_000_000_i128),
        (minutes, 60_000_000_i128),
        (seconds, 1_000_000_i128),
        (microseconds, 1_i128),
    ]
    .into_iter()
    .try_fold(0_i128, |total, (value, unit)| {
        total.checked_add((value as i128).checked_mul(unit)?)
    })
    .and_then(|total| total.checked_mul(sign))
    .and_then(|total| i64::try_from(total).ok())
    .ok_or_else(error)?;
    Ok(IntervalValue::Microsecond {
        microseconds: n,
        start_field,
        end_field,
    })
}

/// Parses a raw (unquoted, no SQL syntax) day-time interval value string --
/// e.g. `"1 02:03:04"` -- against the exact field range `CAST(string AS
/// INTERVAL <start> TO <end>)` declares, the way Spark's
/// `IntervalUtils.castStringToDTInterval` does. This is the runtime
/// counterpart of the interval *literal* grammar (`INTERVAL '...' <field>
/// [TO <field>]`), which expects SQL syntax (a quoted string token, or `TO`
/// as a keyword) and must not be reused to parse a plain runtime value: it
/// rejects "abc" and "1 02:03:04" alike, but the failure surfaces as a
/// confusing "error in SQL parser" instead of Spark's
/// `INVALID_INTERVAL_FORMAT` on the malformed input.
pub fn parse_day_time_interval_value_string(
    s: &str,
    start_field: spec::IntervalFieldType,
    end_field: spec::IntervalFieldType,
) -> SqlResult<i64> {
    let regex: &Regex = match (start_field, end_field) {
        (spec::IntervalFieldType::Day, spec::IntervalFieldType::Day) => &INTERVAL_DAY_REGEX,
        (spec::IntervalFieldType::Day, spec::IntervalFieldType::Hour) => {
            &INTERVAL_DAY_TO_HOUR_REGEX
        }
        (spec::IntervalFieldType::Day, spec::IntervalFieldType::Minute) => {
            &INTERVAL_DAY_TO_MINUTE_REGEX
        }
        (spec::IntervalFieldType::Day, spec::IntervalFieldType::Second) => {
            &INTERVAL_DAY_TO_SECOND_REGEX
        }
        (spec::IntervalFieldType::Hour, spec::IntervalFieldType::Hour) => &INTERVAL_HOUR_REGEX,
        (spec::IntervalFieldType::Hour, spec::IntervalFieldType::Minute) => {
            &INTERVAL_HOUR_TO_MINUTE_REGEX
        }
        (spec::IntervalFieldType::Hour, spec::IntervalFieldType::Second) => {
            &INTERVAL_HOUR_TO_SECOND_REGEX
        }
        (spec::IntervalFieldType::Minute, spec::IntervalFieldType::Minute) => {
            &INTERVAL_MINUTE_REGEX
        }
        (spec::IntervalFieldType::Minute, spec::IntervalFieldType::Second) => {
            &INTERVAL_MINUTE_TO_SECOND_REGEX
        }
        (spec::IntervalFieldType::Second, spec::IntervalFieldType::Second) => {
            &INTERVAL_SECOND_REGEX
        }
        _ => {
            return Err(SqlError::invalid(format!(
                "invalid day-time interval field range: {start_field:?} to {end_field:?}"
            )));
        }
    };
    let extract_micros = |value: IntervalValue| match value {
        IntervalValue::Microsecond { microseconds, .. } => microseconds,
        IntervalValue::YearMonth { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            unreachable!("day-time regexes only ever produce Microsecond values")
        }
    };
    if let Ok(value) = parse_interval_day_time_string(s, false, regex, start_field, Some(end_field))
    {
        return Ok(extract_micros(value));
    }
    // Fall back to Spark's "literal" form: `INTERVAL [sign] '<compact>' <FIELD> [TO <FIELD>]`,
    // the text produced by casting a day-time interval to STRING.
    if let Some(captures) = INTERVAL_LITERAL_WRAPPER_REGEX.captures(s.trim())
        && let Some(qualifier) = captures.get(3)
        && day_time_qualifier_matches(qualifier.as_str(), start_field, end_field)
        && let Some(inner) = captures.get(2)
    {
        let negated = captures.get(1).map(|m| m.as_str()) == Some("-");
        if let Ok(value) =
            parse_interval_day_time_string(inner.as_str(), negated, regex, start_field, Some(end_field))
        {
            return Ok(extract_micros(value));
        }
    }
    Err(SqlError::invalid(format!("interval: {s}")))
}

fn day_time_field_name(field: spec::IntervalFieldType) -> &'static str {
    match field {
        spec::IntervalFieldType::Day => "DAY",
        spec::IntervalFieldType::Hour => "HOUR",
        spec::IntervalFieldType::Minute => "MINUTE",
        spec::IntervalFieldType::Second => "SECOND",
        spec::IntervalFieldType::Year | spec::IntervalFieldType::Month => {
            unreachable!("year-month fields never appear in a day-time interval qualifier")
        }
    }
}

fn day_time_qualifier_matches(
    text: &str,
    start_field: spec::IntervalFieldType,
    end_field: spec::IntervalFieldType,
) -> bool {
    let words: Vec<&str> = text.split_whitespace().collect();
    let start_name = day_time_field_name(start_field);
    let end_name = day_time_field_name(end_field);
    if start_field == end_field {
        matches!(words.as_slice(), [field] if field.eq_ignore_ascii_case(start_name))
    } else {
        matches!(
            words.as_slice(),
            [field, to, end] if field.eq_ignore_ascii_case(start_name)
                && to.eq_ignore_ascii_case("TO")
                && end.eq_ignore_ascii_case(end_name)
        )
    }
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
        StandardIntervalKind::Year => parse_interval_year_month_string(
            &value,
            negated,
            &INTERVAL_YEAR_REGEX,
            spec::IntervalFieldType::Year,
            None,
        ),
        StandardIntervalKind::YearToMonth => parse_interval_year_month_string(
            &value,
            negated,
            &INTERVAL_YEAR_TO_MONTH_REGEX,
            spec::IntervalFieldType::Year,
            Some(spec::IntervalFieldType::Month),
        ),
        StandardIntervalKind::Month => parse_interval_year_month_string(
            &value,
            negated,
            &INTERVAL_MONTH_REGEX,
            spec::IntervalFieldType::Month,
            None,
        ),
        StandardIntervalKind::Day => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_DAY_REGEX,
            spec::IntervalFieldType::Day,
            None,
        ),
        StandardIntervalKind::DayToHour => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_DAY_TO_HOUR_REGEX,
            spec::IntervalFieldType::Day,
            Some(spec::IntervalFieldType::Hour),
        ),
        StandardIntervalKind::DayToMinute => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_DAY_TO_MINUTE_REGEX,
            spec::IntervalFieldType::Day,
            Some(spec::IntervalFieldType::Minute),
        ),
        StandardIntervalKind::DayToSecond => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_DAY_TO_SECOND_REGEX,
            spec::IntervalFieldType::Day,
            Some(spec::IntervalFieldType::Second),
        ),
        StandardIntervalKind::Hour => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_HOUR_REGEX,
            spec::IntervalFieldType::Hour,
            None,
        ),
        StandardIntervalKind::HourToMinute => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_HOUR_TO_MINUTE_REGEX,
            spec::IntervalFieldType::Hour,
            Some(spec::IntervalFieldType::Minute),
        ),
        StandardIntervalKind::HourToSecond => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_HOUR_TO_SECOND_REGEX,
            spec::IntervalFieldType::Hour,
            Some(spec::IntervalFieldType::Second),
        ),
        StandardIntervalKind::Minute => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_MINUTE_REGEX,
            spec::IntervalFieldType::Minute,
            None,
        ),
        StandardIntervalKind::MinuteToSecond => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_MINUTE_TO_SECOND_REGEX,
            spec::IntervalFieldType::Minute,
            Some(spec::IntervalFieldType::Second),
        ),
        StandardIntervalKind::Second => parse_interval_day_time_string(
            &value,
            negated,
            &INTERVAL_SECOND_REGEX,
            spec::IntervalFieldType::Second,
            None,
        ),
    }
}

fn from_ast_multi_unit_interval(
    values: Vec<IntervalValueWithUnit>,
    negated: bool,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut delta = TimeDelta::zero();
    let mut year_month_fields = (None, None);
    let mut day_time_fields = (None, None);
    for value in values {
        let IntervalValueWithUnit { value, unit } = value;
        match unit {
            IntervalUnit::Year(_) | IntervalUnit::Years(_) => {
                extend_interval_fields(&mut year_month_fields, spec::IntervalFieldType::Year);
                let value: i32 = parse_signed_value(value)?;
                let m = value.checked_mul(12).ok_or_else(error)?;
                months = months.checked_add(m).ok_or_else(error)?;
            }
            IntervalUnit::Month(_) | IntervalUnit::Months(_) => {
                extend_interval_fields(&mut year_month_fields, spec::IntervalFieldType::Month);
                let value: i32 = parse_signed_value(value)?;
                months = months.checked_add(value).ok_or_else(error)?;
            }
            IntervalUnit::Week(_) | IntervalUnit::Weeks(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Day);
                let value: i64 = parse_signed_value(value)?;
                let weeks = TimeDelta::try_weeks(value).ok_or_else(error)?;
                delta = delta.checked_add(&weeks).ok_or_else(error)?;
            }
            IntervalUnit::Day(_) | IntervalUnit::Days(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Day);
                let value: i64 = parse_signed_value(value)?;
                let days = TimeDelta::try_days(value).ok_or_else(error)?;
                delta = delta.checked_add(&days).ok_or_else(error)?;
            }
            IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Hour);
                let value: i64 = parse_signed_value(value)?;
                let hours = TimeDelta::try_hours(value).ok_or_else(error)?;
                delta = delta.checked_add(&hours).ok_or_else(error)?;
            }
            IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Minute);
                let value: i64 = parse_signed_value(value)?;
                let minutes = TimeDelta::try_minutes(value).ok_or_else(error)?;
                delta = delta.checked_add(&minutes).ok_or_else(error)?;
            }
            IntervalUnit::Second(_) | IntervalUnit::Seconds(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Second);
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
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Second);
                let value: i64 = parse_signed_value(value)?;
                let milliseconds = TimeDelta::try_milliseconds(value).ok_or_else(error)?;
                delta = delta.checked_add(&milliseconds).ok_or_else(error)?;
            }
            IntervalUnit::Microsecond(_) | IntervalUnit::Microseconds(_) => {
                extend_interval_fields(&mut day_time_fields, spec::IntervalFieldType::Second);
                let value: i64 = parse_signed_value(value)?;
                let microseconds = TimeDelta::microseconds(value);
                delta = delta.checked_add(&microseconds).ok_or_else(error)?;
            }
        }
    }
    let has_year_month_fields = year_month_fields.0.is_some();
    let has_day_time_fields = day_time_fields.0.is_some();
    match (has_year_month_fields, has_day_time_fields) {
        (true, false) => {
            let n = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            Ok(IntervalValue::YearMonth {
                months: n,
                start_field: year_month_fields
                    .0
                    .unwrap_or(spec::IntervalFieldType::Month),
                end_field: year_month_fields.1,
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
            Ok(IntervalValue::Microsecond {
                microseconds: n,
                start_field: day_time_fields.0.unwrap_or(spec::IntervalFieldType::Second),
                end_field: day_time_fields.1,
            })
        }
    }
}

fn extend_interval_fields(
    fields: &mut (
        Option<spec::IntervalFieldType>,
        Option<spec::IntervalFieldType>,
    ),
    field: spec::IntervalFieldType,
) {
    let previous_end = fields.1.or(fields.0);
    let start = fields.0.map_or(field, |start| start.min(field));
    let end = previous_end.map_or(field, |end| end.max(field));
    fields.0 = Some(start);
    fields.1 = (start != end).then_some(end);
}

/// Parses a runtime year-month interval string produced by e.g. `CAST(s AS INTERVAL YEAR TO MONTH)`
/// where `s` is a plain string value (not a SQL `INTERVAL` literal).
///
/// Spark accepts the bare `[+|-]y-m` form (see `IntervalUtils.castStringToYMInterval` in Spark)
/// in addition to the qualified forms handled by [`parse_unqualified_interval_string`]
/// (e.g. `INTERVAL '1-2' YEAR TO MONTH` or `1 year 2 months`).
pub fn parse_year_month_interval_string(s: &str) -> SqlResult<i32> {
    if let Ok(IntervalValue::YearMonth { months, .. }) = parse_interval_year_month_string(
        s,
        false,
        &INTERVAL_YEAR_TO_MONTH_REGEX,
        spec::IntervalFieldType::Year,
        Some(spec::IntervalFieldType::Month),
    ) {
        return Ok(months);
    }
    match parse_unqualified_interval_string(s, false)? {
        IntervalValue::YearMonth { months, .. } => Ok(months),
        IntervalValue::Microsecond { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            Err(SqlError::invalid(format!("interval: {s}")))
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
    from_ast_signed_interval(value)
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

        assert_eq!(
            parse("'-9223372036854.775808' second", false)?,
            IntervalValue::Microsecond {
                microseconds: i64::MIN,
                start_field: spec::IntervalFieldType::Second,
                end_field: None,
            }
        );
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

    #[test]
    fn test_preserves_interval_fields() -> SqlResult<()> {
        assert_eq!(
            parse_unqualified_interval_string("'2-3' year to month", false)?,
            IntervalValue::YearMonth {
                months: 27,
                start_field: spec::IntervalFieldType::Year,
                end_field: Some(spec::IntervalFieldType::Month),
            }
        );
        assert_eq!(
            parse_unqualified_interval_string("'2 03:04:05.006007' day to second", false)?,
            IntervalValue::Microsecond {
                microseconds: (((2 * 24 + 3) * 60 + 4) * 60 + 5) * 1_000_000 + 6_007,
                start_field: spec::IntervalFieldType::Day,
                end_field: Some(spec::IntervalFieldType::Second),
            }
        );
        assert_eq!(
            parse_unqualified_interval_string("1 hour 2 seconds", false)?,
            IntervalValue::Microsecond {
                microseconds: 3_602_000_000,
                start_field: spec::IntervalFieldType::Hour,
                end_field: Some(spec::IntervalFieldType::Second),
            }
        );
        Ok(())
    }

    #[test]
    fn test_zero_multi_unit_interval_preserves_family() -> SqlResult<()> {
        assert_eq!(
            parse_unqualified_interval_string("0 year 0 month", false)?,
            IntervalValue::YearMonth {
                months: 0,
                start_field: spec::IntervalFieldType::Year,
                end_field: Some(spec::IntervalFieldType::Month),
            }
        );
        assert_eq!(
            parse_unqualified_interval_string("0 day 0 second", false)?,
            IntervalValue::Microsecond {
                microseconds: 0,
                start_field: spec::IntervalFieldType::Day,
                end_field: Some(spec::IntervalFieldType::Second),
            }
        );
        assert_eq!(
            parse_unqualified_interval_string("0 year 0 day", false)?,
            IntervalValue::MonthDayNanosecond {
                months: 0,
                days: 0,
                nanoseconds: 0,
            }
        );
        assert_eq!(
            parse_unqualified_interval_string("1 month 1 hour", false)?,
            IntervalValue::MonthDayNanosecond {
                months: 1,
                days: 0,
                nanoseconds: 3_600_000_000_000,
            }
        );
        Ok(())
    }
}
