use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::expression::literal::{Decimal, LiteralType};
use crate::spark::connect::expression::{ExprType, Literal};
use crate::sql::data_type::{SPARK_DECIMAL_MAX_PRECISION, SPARK_DECIMAL_MAX_SCALE};
use crate::sql::parser::SparkDialect;
use chrono;
use chrono_tz;
use lazy_static::lazy_static;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;
use std::fmt::Debug;
use std::ops::Neg;
use std::str::FromStr;

lazy_static! {
    static ref BINARY_REGEX: regex::Regex =
        regex::Regex::new(r"^[0-9a-fA-F]*$").unwrap();
    static ref DECIMAL_REGEX: regex::Regex =
        regex::Regex::new(r"^(?P<sign>[+-]?)(?P<whole>\d+)[.]?(?P<fraction>\d*)([eE](?P<exponent>[+-]?\d+))?$").unwrap();
    static ref DATE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>\d{4})(-(?P<month>\d{1,2})(-(?P<day>\d{1,2})T?)?)?\s*$")
            .unwrap();
    static ref TIMESTAMP_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>\d{4})(-(?P<month>\d{1,2})(-(?P<day>\d{1,2})((\s+|T)(?P<hour>\d{1,2})(:((?P<minute>\d{1,2})(:((?P<second>\d{1,2})([.]((?P<fraction>\d{1,6})?(?P<tz>.*))?)?)?)?)?)?)?)?)?\s*$")
            .unwrap();
    static ref TIMEZONE_OFFSET_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(UTC|UT|GMT)?(?P<sign>[+-]?)(?P<hour>\d{1,2})(:(?P<minute>\d{1,2})(:(?P<second>\d{1,2}))?)?\s*$")
            .unwrap();
    static ref TIMEZONE_OFFSET_COMPACT_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(UTC|UT|GMT)?(?P<sign>[+-]?)(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})?\s*$")
            .unwrap();
    static ref INTERVAL_YEAR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<year>\d+)\s*$").unwrap();
    static ref INTERVAL_YEAR_TO_MONTH_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<year>\d+)-(?P<month>\d+)\s*$").unwrap();
    static ref INTERVAL_MONTH_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<month>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_HOUR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_HOUR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<hour>\d+)\s*$").unwrap();
    static ref INTERVAL_HOUR_TO_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+)\s*$").unwrap();
    static ref INTERVAL_HOUR_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<minute>\d+)\s*$").unwrap();
    static ref INTERVAL_MINUTE_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<sign>[+-]?)(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
}

#[derive(Debug)]
pub(crate) struct LiteralValue<T>(pub T);

impl<T> TryFrom<LiteralValue<T>> for sc::Expression
where
    LiteralValue<T>: TryInto<LiteralType, Error = SparkError>,
{
    type Error = SparkError;

    fn try_from(literal: LiteralValue<T>) -> SparkResult<sc::Expression> {
        Ok(sc::Expression {
            expr_type: Some(ExprType::Literal(Literal {
                literal_type: Some(literal.try_into()?),
            })),
        })
    }
}

impl TryFrom<LiteralValue<bool>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<bool>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Boolean(literal.0))
    }
}

impl TryFrom<LiteralValue<String>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<String>) -> SparkResult<LiteralType> {
        Ok(LiteralType::String(literal.0))
    }
}

impl TryFrom<LiteralValue<Vec<u8>>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<Vec<u8>>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Binary(literal.0))
    }
}

impl TryFrom<LiteralValue<i8>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<i8>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Byte(literal.0 as i32))
    }
}

impl TryFrom<LiteralValue<i16>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<i16>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Short(literal.0 as i32))
    }
}

impl TryFrom<LiteralValue<i32>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<i32>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Integer(literal.0))
    }
}

impl TryFrom<LiteralValue<i64>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<i64>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Long(literal.0))
    }
}

impl TryFrom<LiteralValue<f32>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<f32>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Float(literal.0))
    }
}

impl TryFrom<LiteralValue<f64>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<f64>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Double(literal.0))
    }
}

impl TryFrom<LiteralValue<Decimal>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<Decimal>) -> SparkResult<LiteralType> {
        Ok(LiteralType::Decimal(literal.0))
    }
}

impl TryFrom<LiteralValue<chrono::NaiveDate>> for LiteralType {
    type Error = SparkError;

    fn try_from(literal: LiteralValue<chrono::NaiveDate>) -> SparkResult<LiteralType> {
        let value = chrono::NaiveDateTime::from(literal.0);
        let days = (value - chrono::NaiveDateTime::UNIX_EPOCH).num_days();
        let days = i32::try_from(days).or_else(|_| {
            Err(SparkError::invalid(format!(
                "date literal: {:?}",
                literal.0
            )))
        })?;
        Ok(LiteralType::Date(days))
    }
}

impl TryFrom<LiteralValue<(chrono::NaiveDateTime, TimeZoneVariant)>> for LiteralType {
    type Error = SparkError;

    fn try_from(
        literal: LiteralValue<(chrono::NaiveDateTime, TimeZoneVariant)>,
    ) -> SparkResult<LiteralType> {
        let (dt, ref tz) = literal.0;
        let (delta, ntz) = match tz {
            TimeZoneVariant::FixedOffset(tz) => {
                (TimeZoneVariant::time_delta_from_unix_epoch(&dt, tz)?, false)
            }
            TimeZoneVariant::Named(tz) => {
                (TimeZoneVariant::time_delta_from_unix_epoch(&dt, tz)?, false)
            }
            TimeZoneVariant::Utc => (dt - chrono::NaiveDateTime::UNIX_EPOCH, false),
            TimeZoneVariant::None => (dt - chrono::NaiveDateTime::UNIX_EPOCH, true),
        };
        let microseconds = delta
            .num_microseconds()
            .ok_or_else(|| SparkError::invalid(format!("datetime literal: {:?}", literal.0)))?;
        if ntz {
            Ok(LiteralType::TimestampNtz(microseconds))
        } else {
            Ok(LiteralType::Timestamp(microseconds))
        }
    }
}

impl TryFrom<LiteralValue<Signed<ast::Interval>>> for LiteralType {
    type Error = SparkError;

    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`

    fn try_from(literal: LiteralValue<Signed<ast::Interval>>) -> SparkResult<LiteralType> {
        use ast::{DateTimeField, Interval, IntervalUnit, IntervalValueWithUnit};

        let Signed(interval, negated) = literal.0;
        let error = || SparkError::invalid(format!("interval: {:?}", interval));
        match interval.clone() {
            Interval::Standard { value, unit } => {
                if unit.leading_precision.is_some() || unit.fractional_seconds_precision.is_some() {
                    return Err(error());
                }
                let signed = LiteralValue::<Signed<String>>::try_from(*value)?.0;
                let value = signed.0;
                let negated = signed.1 ^ negated;
                match (unit.leading_field, unit.last_field) {
                    (Some(DateTimeField::Year | DateTimeField::Years), None) => {
                        parse_year_month_interval_string(&value, negated, &INTERVAL_YEAR_REGEX)
                    }
                    (
                        Some(DateTimeField::Year | DateTimeField::Years),
                        Some(DateTimeField::Month | DateTimeField::Months),
                    ) => parse_year_month_interval_string(
                        &value,
                        negated,
                        &INTERVAL_YEAR_TO_MONTH_REGEX,
                    ),
                    (Some(DateTimeField::Month | DateTimeField::Months), None) => {
                        parse_year_month_interval_string(&value, negated, &INTERVAL_MONTH_REGEX)
                    }
                    (Some(DateTimeField::Day | DateTimeField::Days), None) => {
                        parse_day_time_interval_string(&value, negated, &INTERVAL_DAY_REGEX)
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                    ) => {
                        parse_day_time_interval_string(&value, negated, &INTERVAL_DAY_TO_HOUR_REGEX)
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => parse_day_time_interval_string(
                        &value,
                        negated,
                        &INTERVAL_DAY_TO_MINUTE_REGEX,
                    ),
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_day_time_interval_string(
                        &value,
                        negated,
                        &INTERVAL_DAY_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Hour | DateTimeField::Hours), None) => {
                        parse_day_time_interval_string(&value, negated, &INTERVAL_HOUR_REGEX)
                    }
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => parse_day_time_interval_string(
                        &value,
                        negated,
                        &INTERVAL_HOUR_TO_MINUTE_REGEX,
                    ),
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_day_time_interval_string(
                        &value,
                        negated,
                        &INTERVAL_HOUR_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Minute | DateTimeField::Minutes), None) => {
                        parse_day_time_interval_string(&value, negated, &INTERVAL_MINUTE_REGEX)
                    }
                    (
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_day_time_interval_string(
                        &value,
                        negated,
                        &INTERVAL_MINUTE_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Second | DateTimeField::Seconds), None) => {
                        parse_day_time_interval_string(&value, negated, &INTERVAL_SECOND_REGEX)
                    }
                    (Some(x), None) => {
                        let value = ast::Expr::Value(ast::Value::SingleQuotedString(value));
                        let values = vec![(value.clone(), x.clone())];
                        parse_multi_unit_interval(values, negated)
                    }
                    (None, None) => {
                        // The interval qualifier is not specified, so the interval string itself
                        // is assumed to follow interval syntax. Therefore, we invoke the SQL parser
                        // to parse the content.
                        let value = ast::Expr::Value(ast::Value::SingleQuotedString(value));
                        let v = LiteralValue::<Signed<String>>::try_from(value)?.0;
                        let s = v.0.as_str();
                        let negated = v.1 ^ negated;
                        parse_unqualified_interval_string(s, negated)
                    }
                    _ => return Err(error()),
                }
            }
            Interval::MultiUnit { values } => {
                let values = values
                    .into_iter()
                    .map(|x| {
                        let IntervalValueWithUnit { value, unit } = x;
                        if let IntervalUnit {
                            leading_field: Some(f),
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        } = unit
                        {
                            Ok((value, f))
                        } else {
                            Err(error())
                        }
                    })
                    .collect::<SparkResult<_>>()?;
                parse_multi_unit_interval(values, negated)
            }
        }
    }
}

impl TryFrom<String> for LiteralValue<Vec<u8>> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        if value.len() % 2 != 0 {
            return Err(SparkError::invalid(format!("hex string: {:?}", value)));
        }
        if !BINARY_REGEX.is_match(&value) {
            return Err(SparkError::invalid(format!("hex string: {:?}", value)));
        }
        let bytes = value
            .as_bytes()
            .chunks(2)
            .map(|chunk| {
                let chunk = std::str::from_utf8(chunk)
                    .or_else(|_| Err(SparkError::invalid(format!("hex string: {:?}", value))))?;
                u8::from_str_radix(chunk, 16)
                    .or_else(|_| Err(SparkError::invalid(format!("hex string: {:?}", value))))
            })
            .collect::<SparkResult<_>>()?;
        Ok(LiteralValue(bytes))
    }
}

impl TryFrom<String> for LiteralValue<i8> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<i8>()
            .or_else(|_| Err(SparkError::invalid(format!("tinyint: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<i16> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<i16>()
            .or_else(|_| Err(SparkError::invalid(format!("smallint: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<i32> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<i32>()
            .or_else(|_| Err(SparkError::invalid(format!("int: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<i64> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<i64>()
            .or_else(|_| Err(SparkError::invalid(format!("bigint: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<f32> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<f32>()
            .or_else(|_| Err(SparkError::invalid(format!("float: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<f64> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let value = value
            .parse::<f64>()
            .or_else(|_| Err(SparkError::invalid(format!("double: {:?}", value))))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<String> for LiteralValue<Decimal> {
    type Error = SparkError;

    fn try_from(value: String) -> SparkResult<Self> {
        let error = || SparkError::invalid(format!("decimal: {:?}", value));
        let captures = DECIMAL_REGEX.captures(&value).ok_or_else(error)?;
        let whole: String = extract_match(&captures, "whole", error)?.unwrap_or(Default::default());
        let fraction: String =
            extract_match(&captures, "fraction", error)?.unwrap_or(Default::default());
        let e: i32 = extract_match(&captures, "exponent", error)?.unwrap_or(0);
        let w = whole.len() as i32;
        let w = if w + e > 0 { w + e } else { 0 };
        let f = fraction.len() as i32;
        let f = if f - e > 0 { f - e } else { 0 };
        let (precision, scale) = (w + f, f);
        if precision > SPARK_DECIMAL_MAX_PRECISION {
            return Err(SparkError::invalid(format!("decimal: {:?}", value)));
        }
        if scale > SPARK_DECIMAL_MAX_SCALE {
            return Err(SparkError::invalid(format!("decimal: {:?}", value)));
        }
        let value = Decimal {
            value,
            precision: Some(precision),
            scale: Some(scale),
        };
        Ok(LiteralValue(value))
    }
}

struct DecimalSecond {
    seconds: u32,
    microseconds: u32,
}

impl FromStr for Signed<DecimalSecond> {
    type Err = SparkError;

    fn from_str(s: &str) -> SparkResult<Self> {
        let error = || SparkError::invalid(format!("second: {:?}", s));
        let captures = INTERVAL_SECOND_REGEX.captures(s).ok_or_else(error)?;
        let negated = captures.name("sign").map(|s| s.as_str()) == Some("-");
        let seconds: u32 = extract_match(&captures, "second", error)?.unwrap_or(0);
        let microseconds: u32 =
            extract_second_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
        Ok(Signed(
            DecimalSecond {
                seconds,
                microseconds,
            },
            negated,
        ))
    }
}

pub(crate) type Negated = bool;
pub(crate) struct Signed<T>(pub T, pub Negated);

impl<T> Neg for Signed<T> {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(self.0, !self.1)
    }
}

impl<T: FromStr> FromStr for Signed<T> {
    type Err = SparkError;

    fn from_str(s: &str) -> SparkResult<Self> {
        let v = s.parse::<T>().or_else(|_| Err(SparkError::invalid(s)))?;
        Ok(Signed(v, false))
    }
}

impl<T> TryFrom<ast::Expr> for LiteralValue<T>
where
    T: Neg<Output = T> + FromStr,
{
    type Error = SparkError;

    fn try_from(expr: ast::Expr) -> SparkResult<LiteralValue<T>> {
        use ast::Value;

        let error = || SparkError::invalid(format!("expression: {:?}", expr));
        match &expr {
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                let value = LiteralValue::<T>::try_from(*expr.clone())?;
                Ok(LiteralValue(-value.0))
            }
            ast::Expr::Value(Value::Number(value, None))
            | ast::Expr::Value(Value::SingleQuotedString(value))
            | ast::Expr::Value(Value::DoubleQuotedString(value))
            | ast::Expr::Value(Value::DollarQuotedString(ast::DollarQuotedString {
                value, ..
            }))
            | ast::Expr::Value(Value::TripleSingleQuotedString(value))
            | ast::Expr::Value(Value::TripleDoubleQuotedString(value)) => {
                let value = value.parse::<T>().or_else(|_| Err(error()))?;
                Ok(LiteralValue(value))
            }
            _ => Err(error()),
        }
    }
}

fn extract_match<T>(
    captures: &regex::Captures,
    name: &str,
    error: impl FnOnce() -> SparkError,
) -> SparkResult<Option<T>>
where
    T: FromStr,
{
    captures
        .name(name)
        .map(|x| x.as_str().parse::<T>())
        .transpose()
        .or_else(|_| Err(error()))
}

fn extract_second_fraction_match<T>(
    captures: &regex::Captures,
    name: &str,
    n: usize,
    error: impl FnOnce() -> SparkError,
) -> SparkResult<Option<T>>
where
    T: FromStr,
{
    captures
        .name(name)
        .map(|f| {
            f.as_str()
                .chars()
                .chain(std::iter::repeat('0'))
                .take(n)
                .collect::<String>()
                .parse::<T>()
        })
        .transpose()
        .or_else(|_| Err(error()))
}

pub(crate) fn parse_date_string(s: &str) -> SparkResult<LiteralType> {
    let error = || SparkError::invalid(format!("date: {s}"));
    let captures = DATE_REGEX.captures(s).ok_or_else(error)?;
    let year = extract_match(&captures, "year", error)?.ok_or_else(error)?;
    let month = extract_match(&captures, "month", error)?.unwrap_or(1);
    let day = extract_match(&captures, "day", error)?.unwrap_or(1);
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| SparkError::invalid(format!("date: {s}")))?;
    Ok(LiteralType::try_from(LiteralValue(date))?)
}

pub(crate) fn parse_timestamp_string(s: &str) -> SparkResult<LiteralType> {
    let error = || SparkError::invalid(format!("timestamp: {s}"));
    let captures = TIMESTAMP_REGEX.captures(s).ok_or_else(error)?;
    let year = extract_match(&captures, "year", error)?.ok_or_else(error)?;
    let month = extract_match(&captures, "month", error)?.unwrap_or(1);
    let day = extract_match(&captures, "day", error)?.unwrap_or(1);
    let hour = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minute = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let second = extract_match(&captures, "second", error)?.unwrap_or(0);
    let fraction = extract_second_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
    let tz = captures.name("tz").map(|tz| tz.as_str());
    let tz = parse_timezone_string(tz)?;
    let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|d| d.and_hms_opt(hour, minute, second))
        .and_then(|d| d.checked_add_signed(chrono::Duration::microseconds(fraction)))
        .ok_or_else(error)?;
    Ok(LiteralType::try_from(LiteralValue((dt, tz)))?)
}

#[derive(Debug)]
pub(crate) enum TimeZoneVariant {
    None,
    Utc,
    FixedOffset(chrono::FixedOffset),
    Named(chrono_tz::Tz),
}

impl TimeZoneVariant {
    fn time_delta_from_unix_epoch<Tz, O>(
        dt: &chrono::NaiveDateTime,
        tz: &Tz,
    ) -> SparkResult<chrono::TimeDelta>
    where
        Tz: chrono::TimeZone<Offset = O> + Debug,
        O: chrono::Offset,
    {
        let dt = tz
            .from_local_datetime(&dt)
            .single()
            .ok_or_else(|| SparkError::invalid(format!("datetime: {:?} {:?}", dt, tz)))?;
        Ok(dt - chrono::DateTime::UNIX_EPOCH.with_timezone(tz))
    }
}

pub(crate) fn parse_timezone_string(tz: Option<&str>) -> SparkResult<TimeZoneVariant> {
    match tz {
        None => Ok(TimeZoneVariant::None),
        Some(tz) if tz.trim().is_empty() => Ok(TimeZoneVariant::None),
        Some(tz) if tz.trim() == "Z" => Ok(TimeZoneVariant::Utc),
        Some(tz) => {
            let error = || SparkError::invalid(format!("timezone: {tz}"));
            let captures = TIMEZONE_OFFSET_REGEX
                .captures(tz)
                .or_else(|| TIMEZONE_OFFSET_COMPACT_REGEX.captures(tz));
            if let Some(captures) = captures {
                let negated = captures.name("sign").map(|s| s.as_str()) == Some("-");
                let hour: u32 = extract_match(&captures, "hour", error)?.ok_or_else(error)?;
                let minute: u32 = extract_match(&captures, "minute", error)?.unwrap_or(0);
                let second: u32 = extract_match(&captures, "second", error)?.unwrap_or(0);
                let n = (hour * 3600 + minute * 60 + second) as i32 * if negated { -1 } else { 1 };
                let offset = chrono::FixedOffset::east_opt(n).ok_or_else(error)?;
                Ok(TimeZoneVariant::FixedOffset(offset))
            } else {
                let tz = tz.parse::<chrono_tz::Tz>().or_else(|_| Err(error()))?;
                Ok(TimeZoneVariant::Named(tz))
            }
        }
    }
}

fn parse_year_month_interval_string(
    s: &str,
    negated: Negated,
    interval_regex: &regex::Regex,
) -> SparkResult<LiteralType> {
    let error = || SparkError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let years: i32 = extract_match(&captures, "year", error)?.unwrap_or(0);
    let months: i32 = extract_match(&captures, "month", error)?.unwrap_or(0);
    let n = years
        .checked_mul(12)
        .ok_or_else(error)?
        .checked_add(months)
        .ok_or_else(error)? as i32;
    let n = if negated {
        n.checked_mul(-1).ok_or_else(error)?
    } else {
        n
    };
    Ok(LiteralType::YearMonthInterval(n))
}

fn parse_day_time_interval_string(
    s: &str,
    negated: Negated,
    interval_regex: &regex::Regex,
) -> SparkResult<LiteralType> {
    let error = || SparkError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let days: i64 = extract_match(&captures, "day", error)?.unwrap_or(0);
    let hours: i64 = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minutes: i64 = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let seconds: i64 = extract_match(&captures, "second", error)?.unwrap_or(0);
    let microseconds: i64 =
        extract_second_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
    let delta = chrono::TimeDelta::try_days(days)
        .ok_or_else(error)?
        .checked_add(&chrono::TimeDelta::try_hours(hours).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&chrono::TimeDelta::try_minutes(minutes).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&chrono::TimeDelta::try_seconds(seconds).ok_or_else(error)?)
        .ok_or_else(error)?
        .checked_add(&chrono::TimeDelta::microseconds(microseconds))
        .ok_or_else(error)?;
    let microseconds = delta.num_microseconds().ok_or_else(error)?;
    let n = if negated {
        microseconds.checked_mul(-1).ok_or_else(error)?
    } else {
        microseconds
    };
    Ok(LiteralType::DayTimeInterval(n))
}

fn parse_multi_unit_interval(
    values: Vec<(ast::Expr, ast::DateTimeField)>,
    negated: Negated,
) -> SparkResult<LiteralType> {
    use ast::DateTimeField;

    let error = || SparkError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut delta = chrono::TimeDelta::zero();
    for (expr, field) in values {
        match field {
            DateTimeField::Year | DateTimeField::Years => {
                let value = LiteralValue::<i32>::try_from(expr)?.0;
                let m = value.checked_mul(12).ok_or_else(error)?;
                months = months.checked_add(m).ok_or_else(error)?;
            }
            DateTimeField::Month | DateTimeField::Months => {
                let value = LiteralValue::<i32>::try_from(expr)?.0;
                months = months.checked_add(value).ok_or_else(error)?;
            }
            DateTimeField::Week(None) | DateTimeField::Weeks => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let weeks = chrono::TimeDelta::try_weeks(value).ok_or_else(error)?;
                delta = delta.checked_add(&weeks).ok_or_else(error)?;
            }
            DateTimeField::Day | DateTimeField::Days => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let days = chrono::TimeDelta::try_days(value).ok_or_else(error)?;
                delta = delta.checked_add(&days).ok_or_else(error)?;
            }
            DateTimeField::Hour | DateTimeField::Hours => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let hours = chrono::TimeDelta::try_hours(value).ok_or_else(error)?;
                delta = delta.checked_add(&hours).ok_or_else(error)?;
            }
            DateTimeField::Minute | DateTimeField::Minutes => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let minutes = chrono::TimeDelta::try_minutes(value).ok_or_else(error)?;
                delta = delta.checked_add(&minutes).ok_or_else(error)?;
            }
            DateTimeField::Second | DateTimeField::Seconds => {
                let Signed(value, negated) =
                    LiteralValue::<Signed<DecimalSecond>>::try_from(expr)?.0;
                let seconds = chrono::TimeDelta::seconds(value.seconds as i64);
                let microseconds = chrono::TimeDelta::microseconds(value.microseconds as i64);
                if negated {
                    delta = delta.checked_sub(&seconds).ok_or_else(error)?;
                    delta = delta.checked_sub(&microseconds).ok_or_else(error)?;
                } else {
                    delta = delta.checked_add(&seconds).ok_or_else(error)?;
                    delta = delta.checked_add(&microseconds).ok_or_else(error)?;
                }
            }
            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let milliseconds = chrono::TimeDelta::try_milliseconds(value).ok_or_else(error)?;
                delta = delta.checked_add(&milliseconds).ok_or_else(error)?;
            }
            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let microseconds = chrono::TimeDelta::microseconds(value);
                delta = delta.checked_add(&microseconds).ok_or_else(error)?;
            }
            _ => return Err(error()),
        }
    }
    match (months != 0, delta != chrono::TimeDelta::zero()) {
        (true, false) => {
            let n = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            Ok(LiteralType::YearMonthInterval(n))
        }
        (true, true) => Err(SparkError::invalid(
            "cannot mix year-month and day-time fields in interval",
        )),
        (false, _) => {
            let microseconds = delta.num_microseconds().ok_or_else(error)?;
            let n = if negated {
                microseconds.checked_mul(-1).ok_or_else(error)?
            } else {
                microseconds
            };
            Ok(LiteralType::DayTimeInterval(n))
        }
    }
}

fn parse_unqualified_interval_string(s: &str, negated: Negated) -> SparkResult<LiteralType> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(s)?;
    // consume the `INTERVAL` keyword if any
    let _ = parser.parse_keyword(Keyword::INTERVAL);
    let expr = parser.parse_interval()?;
    if parser.peek_token() != Token::EOF {
        let token = parser.next_token();
        return Err(SparkError::invalid(format!(
            "extra tokens after interval: {token}"
        )));
    }
    let interval = match expr {
        ast::Expr::Interval(interval) => interval,
        _ => return Err(SparkError::invalid(format!("interval: {s}"))),
    };
    if matches!(
        interval,
        ast::Interval::Standard {
            unit: ast::IntervalUnit {
                leading_field: None,
                last_field: None,
                ..
            },
            ..
        }
    ) {
        // The parsed unqualified interval string cannot itself be unqualified,
        // otherwise it could cause infinite recursion when creating the literal.
        return Err(SparkError::invalid(format!("interval: {s}")));
    }
    LiteralType::try_from(LiteralValue(Signed(interval, negated)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval() -> SparkResult<()> {
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
    fn test_parse_unqualified_interval_string() -> SparkResult<()> {
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
