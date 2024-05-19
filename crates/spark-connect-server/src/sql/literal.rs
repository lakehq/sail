use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::expression::literal::{Decimal, LiteralType};
use crate::spark::connect::expression::{ExprType, Literal};
use crate::sql::data_type::{SPARK_DECIMAL_MAX_PRECISION, SPARK_DECIMAL_MAX_SCALE};
use chrono;
use chrono_tz;
use lazy_static::lazy_static;
use sqlparser::ast;
use std::fmt::Debug;

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
        regex::Regex::new(r"^\s*(UTC|UT|GMT)?(?P<hour>[+-]?\d{1,2})(:(?P<minute>\d{1,2})(:(?P<second>\d{1,2}))?)?\s*$")
            .unwrap();
    static ref TIMEZONE_OFFSET_COMPACT_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(UTC|UT|GMT)?(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})?\s*$")
            .unwrap();
    static ref INTERVAL_YEAR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>[+-]?\d+)\s*$").unwrap();
    static ref INTERVAL_YEAR_TO_MONTH_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>[+-]?\d+)-(?P<month>\d+)\s*$").unwrap();
    static ref INTERVAL_MONTH_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<month>[+-]?\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<day>[+-]?\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_HOUR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<day>[+-]?\d+)\s+(?P<hour>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<day>[+-]?\d+)\s+(?P<hour>\d+):(?P<minute>\d+)\s*$").unwrap();
    static ref INTERVAL_DAY_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<day>[+-]?\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_HOUR_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<hour>[+-]?\d+)\s*$").unwrap();
    static ref INTERVAL_HOUR_TO_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<hour>[+-]?\d+):(?P<minute>\d+)\s*$").unwrap();
    static ref INTERVAL_HOUR_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<hour>[+-]?\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_MINUTE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<minute>[+-]?\d+)\s*$").unwrap();
    static ref INTERVAL_MINUTE_TO_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<minute>[+-]?\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
    static ref INTERVAL_SECOND_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<second>[+-]?\d+)[.]?(?P<fraction>\d+)?\s*$").unwrap();
}

#[derive(Debug)]
pub(crate) struct LiteralValue<T>(pub T);

impl<T> From<LiteralValue<T>> for sc::Expression
where
    LiteralValue<T>: Into<LiteralType>,
{
    fn from(literal: LiteralValue<T>) -> sc::Expression {
        sc::Expression {
            expr_type: Some(ExprType::Literal(Literal {
                literal_type: Some(literal.into()),
            })),
        }
    }
}

impl From<LiteralValue<bool>> for LiteralType {
    fn from(literal: LiteralValue<bool>) -> LiteralType {
        LiteralType::Boolean(literal.0)
    }
}

impl From<LiteralValue<String>> for LiteralType {
    fn from(literal: LiteralValue<String>) -> LiteralType {
        LiteralType::String(literal.0)
    }
}

impl From<LiteralValue<Vec<u8>>> for LiteralType {
    fn from(literal: LiteralValue<Vec<u8>>) -> LiteralType {
        LiteralType::Binary(literal.0)
    }
}

impl From<LiteralValue<i8>> for LiteralType {
    fn from(literal: LiteralValue<i8>) -> LiteralType {
        LiteralType::Byte(literal.0 as i32)
    }
}

impl From<LiteralValue<i16>> for LiteralType {
    fn from(literal: LiteralValue<i16>) -> LiteralType {
        LiteralType::Short(literal.0 as i32)
    }
}

impl From<LiteralValue<i32>> for LiteralType {
    fn from(literal: LiteralValue<i32>) -> LiteralType {
        LiteralType::Integer(literal.0)
    }
}

impl From<LiteralValue<i64>> for LiteralType {
    fn from(literal: LiteralValue<i64>) -> LiteralType {
        LiteralType::Long(literal.0)
    }
}

impl From<LiteralValue<f32>> for LiteralType {
    fn from(literal: LiteralValue<f32>) -> LiteralType {
        LiteralType::Float(literal.0)
    }
}

impl From<LiteralValue<f64>> for LiteralType {
    fn from(literal: LiteralValue<f64>) -> LiteralType {
        LiteralType::Double(literal.0)
    }
}

impl From<LiteralValue<Decimal>> for LiteralType {
    fn from(literal: LiteralValue<Decimal>) -> LiteralType {
        LiteralType::Decimal(literal.0)
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

impl TryFrom<LiteralValue<ast::Interval>> for LiteralType {
    type Error = SparkError;

    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`

    fn try_from(literal: LiteralValue<ast::Interval>) -> SparkResult<LiteralType> {
        use ast::{DateTimeField, Interval, IntervalUnit, IntervalValueWithUnit};

        let interval = literal.0;
        let mut multi_unit_values: Vec<(ast::Expr, DateTimeField)> = vec![];
        let out: SparkResult<()> = match interval {
            Interval::Standard {
                ref value,
                ref unit,
            } => {
                if unit.leading_precision.is_some() || unit.fractional_seconds_precision.is_some() {
                    return Err(SparkError::invalid(format!("interval: {:?}", interval)));
                }
                let v = LiteralValue::<(String, Negated)>::try_from(*value.clone())?;
                let s = v.0 .0.as_str();
                let negated = v.0 .1;
                match (&unit.leading_field, &unit.last_field) {
                    (Some(DateTimeField::Year | DateTimeField::Years), None) => {
                        return parse_year_month_interval_string(s, negated, &INTERVAL_YEAR_REGEX);
                    }
                    (
                        Some(DateTimeField::Year | DateTimeField::Years),
                        Some(DateTimeField::Month | DateTimeField::Months),
                    ) => {
                        return parse_year_month_interval_string(
                            s,
                            negated,
                            &INTERVAL_YEAR_TO_MONTH_REGEX,
                        );
                    }
                    (Some(DateTimeField::Month | DateTimeField::Months), None) => {
                        return parse_year_month_interval_string(s, negated, &INTERVAL_MONTH_REGEX);
                    }
                    (Some(DateTimeField::Day | DateTimeField::Days), None) => {
                        return parse_day_time_interval_string(s, negated, &INTERVAL_DAY_REGEX);
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_DAY_TO_HOUR_REGEX,
                        )
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_DAY_TO_MINUTE_REGEX,
                        )
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_DAY_TO_SECOND_REGEX,
                        )
                    }
                    (Some(DateTimeField::Hour | DateTimeField::Hours), None) => {
                        return parse_day_time_interval_string(s, negated, &INTERVAL_HOUR_REGEX);
                    }
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_HOUR_TO_MINUTE_REGEX,
                        )
                    }
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_HOUR_TO_SECOND_REGEX,
                        )
                    }
                    (Some(DateTimeField::Minute | DateTimeField::Minutes), None) => {
                        return parse_day_time_interval_string(s, negated, &INTERVAL_MINUTE_REGEX);
                    }
                    (
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => {
                        return parse_day_time_interval_string(
                            s,
                            negated,
                            &INTERVAL_MINUTE_TO_SECOND_REGEX,
                        );
                    }
                    (Some(DateTimeField::Second | DateTimeField::Seconds), None) => {
                        return parse_day_time_interval_string(s, negated, &INTERVAL_SECOND_REGEX);
                    }
                    (Some(x), None) => {
                        multi_unit_values.push((*value.clone(), x.clone()));
                        Ok(())
                    }
                    _ => return Err(SparkError::invalid(format!("interval: {:?}", interval))),
                }
            }
            Interval::MultiUnit { ref values } => {
                values.iter().try_for_each(|x| {
                    let IntervalValueWithUnit { value, unit } = x;
                    if let IntervalUnit {
                        leading_field: Some(f),
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None,
                    } = unit
                    {
                        multi_unit_values.push((value.clone(), f.clone()));
                        Ok(())
                    } else {
                        Err(SparkError::invalid(format!("interval: {:?}", interval)))
                    }
                })?;
                Ok(())
            }
        };
        out?;
        parse_multi_unit_interval(multi_unit_values)
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
        let captures = DECIMAL_REGEX
            .captures(&value)
            .ok_or_else(|| SparkError::invalid(format!("decimal: {:?}", value)))?;
        let w = captures
            .name("whole")
            .map(|x| i32::try_from(x.as_str().len()))
            .transpose()
            .or_else(|_| Err(SparkError::invalid(format!("decimal: {:?}", value))))?
            .unwrap_or(0);
        let f = captures
            .name("fraction")
            .map(|x| i32::try_from(x.len()))
            .transpose()
            .or_else(|_| Err(SparkError::invalid(format!("decimal: {:?}", value))))?
            .unwrap_or(0);
        let e = captures
            .name("exponent")
            .map(|x| x.as_str().parse::<i32>())
            .transpose()
            .or_else(|_| Err(SparkError::invalid(format!("decimal: {:?}", value))))?
            .unwrap_or(0);
        let w = if w + e > 0 { w + e } else { 0 };
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

type Negated = bool;

impl TryFrom<ast::Expr> for LiteralValue<(String, Negated)> {
    type Error = SparkError;

    fn try_from(expr: ast::Expr) -> SparkResult<LiteralValue<(String, Negated)>> {
        use ast::Value;

        match expr {
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                let value = LiteralValue::<(String, Negated)>::try_from(*expr)?;
                Ok(LiteralValue((value.0 .0, !value.0 .1)))
            }
            ast::Expr::Value(Value::Number(value, None))
            | ast::Expr::Value(Value::SingleQuotedString(value))
            | ast::Expr::Value(Value::DoubleQuotedString(value))
            | ast::Expr::Value(Value::DollarQuotedString(ast::DollarQuotedString {
                value, ..
            }))
            | ast::Expr::Value(Value::TripleSingleQuotedString(value))
            | ast::Expr::Value(Value::TripleDoubleQuotedString(value)) => {
                Ok(LiteralValue((value, false)))
            }
            _ => Err(SparkError::invalid(format!(
                "string expression: {:?}",
                expr
            ))),
        }
    }
}

impl TryFrom<ast::Expr> for LiteralValue<i32> {
    type Error = SparkError;

    fn try_from(expr: ast::Expr) -> SparkResult<LiteralValue<i32>> {
        use ast::Value;

        match expr {
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                let value = LiteralValue::<i32>::try_from(*expr)?;
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
                let value = value
                    .parse::<i32>()
                    .or_else(|_| Err(SparkError::invalid(format!("int: {:?}", value))))?;
                Ok(LiteralValue(value))
            }
            _ => Err(SparkError::invalid(format!("int expression: {:?}", expr))),
        }
    }
}

pub(crate) fn parse_date_string(s: &str) -> SparkResult<LiteralType> {
    let captures = DATE_REGEX
        .captures(s)
        .ok_or_else(|| SparkError::invalid(format!("date: {s}")))?;
    let year = captures
        .name("year")
        .ok_or_else(|| SparkError::invalid(format!("missing year in date: {s}")))?
        .as_str()
        .parse::<i32>()
        .or_else(|_| Err(SparkError::invalid(format!("invalid year in date: {s}"))))?;
    let month = captures
        .name("month")
        .map(|m| m.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| Err(SparkError::invalid(format!("invalid month in date: {s}"))))?
        .unwrap_or(1);
    let day = captures
        .name("day")
        .map(|d| d.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| Err(SparkError::invalid(format!("invalid day in date: {s}"))))?
        .unwrap_or(1);
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| SparkError::invalid(format!("date: {s}")))?;
    Ok(LiteralType::try_from(LiteralValue(date))?)
}

pub(crate) fn parse_timestamp_string(s: &str) -> SparkResult<LiteralType> {
    let captures = TIMESTAMP_REGEX
        .captures(s)
        .ok_or_else(|| SparkError::invalid(format!("timestamp: {s}")))?;
    let year = captures
        .name("year")
        .ok_or_else(|| SparkError::invalid(format!("missing year in timestamp: {s}")))?
        .as_str()
        .parse::<i32>()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid year in timestamp: {s}"
            )))
        })?;
    let month = captures
        .name("month")
        .map(|m| m.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid month in timestamp: {s}"
            )))
        })?
        .unwrap_or(1);
    let day = captures
        .name("day")
        .map(|d| d.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid day in timestamp: {s}"
            )))
        })?
        .unwrap_or(1);
    let hour = captures
        .name("hour")
        .map(|h| h.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid hour in timestamp: {s}"
            )))
        })?
        .unwrap_or(0);
    let minute = captures
        .name("minute")
        .map(|m| m.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid minute in timestamp: {s}"
            )))
        })?
        .unwrap_or(0);
    let second = captures
        .name("second")
        .map(|s| s.as_str().parse::<u32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid second in timestamp: {s}"
            )))
        })?
        .unwrap_or(0);
    let fraction = captures
        .name("fraction")
        .map(|f| {
            f.as_str()
                .chars()
                .chain(std::iter::repeat('0'))
                .take(6)
                .collect::<String>()
                .parse::<u32>()
        })
        .transpose()
        .or_else(|_| Err(SparkError::invalid(format!("invalid fraction: {s}"))))?
        .unwrap_or(0);
    let tz = captures.name("tz").map(|tz| tz.as_str()).unwrap_or("");
    let tz = parse_timezone_string(tz)?;
    let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|d| d.and_hms_opt(hour, minute, second))
        .and_then(|d| d.checked_add_signed(chrono::Duration::microseconds(fraction as i64)))
        .ok_or_else(|| SparkError::invalid(format!("timestamp: {s}")))?;
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

pub(crate) fn parse_timezone_string(tz: &str) -> SparkResult<TimeZoneVariant> {
    if tz.trim().is_empty() {
        return Ok(TimeZoneVariant::None);
    }
    if tz.trim() == "Z" {
        return Ok(TimeZoneVariant::Utc);
    }
    let captures = TIMEZONE_OFFSET_REGEX
        .captures(tz)
        .or_else(|| TIMEZONE_OFFSET_COMPACT_REGEX.captures(tz));
    if let Some(captures) = captures {
        let hour = captures
            .name("hour")
            .ok_or_else(|| SparkError::invalid(format!("missing hour in timezone offset: {tz}")))?
            .as_str()
            .parse::<i32>()
            .or_else(|_| {
                Err(SparkError::invalid(format!(
                    "invalid hour in timezone offset: {tz}"
                )))
            })?;
        let minute = captures
            .name("minute")
            .map(|m| m.as_str().parse::<i32>())
            .transpose()
            .or_else(|_| {
                Err(SparkError::invalid(format!(
                    "invalid minute in timezone offset: {tz}"
                )))
            })?
            .unwrap_or(0);
        let second = captures
            .name("second")
            .map(|s| s.as_str().parse::<i32>())
            .transpose()
            .or_else(|_| {
                Err(SparkError::invalid(format!(
                    "invalid second in timezone offset: {tz}"
                )))
            })?
            .unwrap_or(0);
        let offset = chrono::FixedOffset::east_opt(hour * 3600 + minute * 60 + second)
            .ok_or_else(|| SparkError::invalid(format!("timezone offset: {tz}")))?;
        Ok(TimeZoneVariant::FixedOffset(offset))
    } else {
        let tz = tz
            .parse::<chrono_tz::Tz>()
            .or_else(|_| Err(SparkError::invalid(format!("timezone literal: {tz}"))))?;
        Ok(TimeZoneVariant::Named(tz))
    }
}

fn parse_year_month_interval_string(
    s: &str,
    negated: bool,
    interval_regex: &regex::Regex,
) -> SparkResult<LiteralType> {
    let captures = interval_regex
        .captures(s)
        .ok_or_else(|| SparkError::invalid(format!("interval: {s}")))?;
    let year = captures
        .name("year")
        .map(|y| y.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid year in interval: {s}"
            )))
        })?
        .unwrap_or(0);
    let month = captures
        .name("month")
        .map(|m| m.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid month in interval: {s}"
            )))
        })?
        .unwrap_or(0);
    let months = (year * 12 + month) * (if negated { -1 } else { 1 });
    Ok(LiteralType::YearMonthInterval(months))
}

fn parse_day_time_interval_string(
    s: &str,
    negated: Negated,
    interval_regex: &regex::Regex,
) -> SparkResult<LiteralType> {
    let captures = interval_regex
        .captures(s)
        .ok_or_else(|| SparkError::invalid(format!("interval: {s}")))?;
    let day = captures
        .name("day")
        .map(|d| d.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| Err(SparkError::invalid(format!("invalid day in interval: {s}"))))?
        .unwrap_or(0);
    let hour = captures
        .name("hour")
        .map(|h| h.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid hour in interval: {s}"
            )))
        })?
        .unwrap_or(0);
    let minute = captures
        .name("minute")
        .map(|m| m.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid minute in interval: {s}"
            )))
        })?
        .unwrap_or(0);
    let second = captures
        .name("second")
        .map(|s| s.as_str().parse::<i32>())
        .transpose()
        .or_else(|_| {
            Err(SparkError::invalid(format!(
                "invalid second in interval: {s}"
            )))
        })?
        .unwrap_or(0);
    let fraction = captures
        .name("fraction")
        .map(|f| {
            f.as_str()
                .chars()
                .chain(std::iter::repeat('0'))
                .take(6)
                .collect::<String>()
                .parse::<i32>()
        })
        .transpose()
        .or_else(|_| Err(SparkError::invalid(format!("invalid fraction: {s}"))))?
        .unwrap_or(0);
    let delta = chrono::TimeDelta::days(day as i64)
        + chrono::TimeDelta::hours(hour as i64)
        + chrono::TimeDelta::minutes(minute as i64)
        + chrono::TimeDelta::seconds(second as i64)
        + chrono::TimeDelta::microseconds(fraction as i64);
    let delta = if negated { -delta } else { delta };
    let microseconds = delta
        .num_microseconds()
        .ok_or_else(|| SparkError::invalid(format!("interval: {s}")))?;
    Ok(LiteralType::DayTimeInterval(microseconds))
}

fn parse_multi_unit_interval(
    values: Vec<(ast::Expr, ast::DateTimeField)>,
) -> SparkResult<LiteralType> {
    use ast::DateTimeField;

    let mut months = 0;
    let mut delta = chrono::TimeDelta::zero();
    for (expr, field) in values {
        match field {
            DateTimeField::Year | DateTimeField::Years => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                months += value.0 * 12;
            }
            DateTimeField::Month | DateTimeField::Months => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                months += value.0;
            }
            DateTimeField::Week(None) | DateTimeField::Weeks => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::weeks(value.0 as i64);
            }
            DateTimeField::Day | DateTimeField::Days => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::days(value.0 as i64);
            }
            DateTimeField::Hour | DateTimeField::Hours => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::hours(value.0 as i64);
            }
            DateTimeField::Minute | DateTimeField::Minutes => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::minutes(value.0 as i64);
            }
            DateTimeField::Second | DateTimeField::Seconds => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::seconds(value.0 as i64);
            }
            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::milliseconds(value.0 as i64);
            }
            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                let value = LiteralValue::<i32>::try_from(expr)?;
                delta += chrono::TimeDelta::microseconds(value.0 as i64);
            }
            _ => return Err(SparkError::invalid("interval")),
        }
    }
    match (months != 0, delta != chrono::TimeDelta::zero()) {
        (true, false) => Ok(LiteralType::YearMonthInterval(months)),
        (true, true) => Err(SparkError::invalid(
            "Cannot mix year-month and day-time fields in interval",
        )),
        (false, _) => {
            let microseconds = delta
                .num_microseconds()
                .ok_or_else(|| SparkError::invalid("interval"))?;
            Ok(LiteralType::DayTimeInterval(microseconds))
        }
    }
}
