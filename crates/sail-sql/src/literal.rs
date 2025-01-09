use std::fmt::Debug;
use std::ops::{Add, Neg};
use std::str::FromStr;

use chrono::{TimeDelta, Utc};
use chrono_tz::Tz;
use datafusion::arrow::datatypes::{
    i256, DECIMAL128_MAX_PRECISION as ARROW_DECIMAL128_MAX_PRECISION,
    DECIMAL256_MAX_PRECISION as ARROW_DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE as ARROW_DECIMAL256_MAX_SCALE,
};
use lazy_static::lazy_static;
use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use {chrono, chrono_tz};

use crate::error::{SqlError, SqlResult};
use crate::parser::{fail_on_extra_token, SparkDialect};

lazy_static! {
    static ref BINARY_REGEX: regex::Regex =
        regex::Regex::new(r"^[0-9a-fA-F]*$").unwrap();
    static ref DECIMAL_REGEX: regex::Regex =
        regex::Regex::new(r"^(?P<sign>[+-]?)(?P<whole>\d{1,38})[.]?(?P<fraction>\d{0,38})([eE](?P<exponent>[+-]?\d+))?$").unwrap();
    static ref DECIMAL_FRACTION_REGEX: regex::Regex =
        regex::Regex::new(r"^(?P<sign>[+-]?)[.](?P<fraction>\d{1,38})([eE](?P<exponent>[+-]?\d+))?$").unwrap();
    static ref DATE_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>\d{4})(-(?P<month>\d{1,2})(-(?P<day>\d{1,2})T?)?)?\s*$")
            .unwrap();
    static ref TIMESTAMP_REGEX: regex::Regex =
        regex::Regex::new(r"^\s*(?P<year>\d{4})(-(?P<month>\d{1,2})(-(?P<day>\d{1,2})((\s+|T)(?P<hour>\d{1,2})(:(?P<minute>\d{1,2})(:(?P<second>\d{1,2})(\.(?P<fraction>\d{1,9}))?(?P<tz>.*)?)?)?)?)?)?\s*$")
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

impl<T> TryFrom<LiteralValue<T>> for spec::Expr
where
    LiteralValue<T>: TryInto<spec::Literal, Error = SqlError>,
{
    type Error = SqlError;

    fn try_from(literal: LiteralValue<T>) -> SqlResult<spec::Expr> {
        Ok(spec::Expr::Literal(literal.try_into()?))
    }
}

impl TryFrom<LiteralValue<bool>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<bool>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Boolean {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<String>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<String>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Utf8 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<Vec<u8>>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<Vec<u8>>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Binary {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<i8>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<i8>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Int8 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<i16>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<i16>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Int16 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<i32>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<i32>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Int32 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<i64>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<i64>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Int64 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<f32>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<f32>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Float32 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<f64>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<f64>) -> SqlResult<spec::Literal> {
        Ok(spec::Literal::Float64 {
            value: Some(literal.0),
        })
    }
}

impl TryFrom<LiteralValue<chrono::NaiveDate>> for spec::Literal {
    type Error = SqlError;

    fn try_from(literal: LiteralValue<chrono::NaiveDate>) -> SqlResult<spec::Literal> {
        let value = chrono::NaiveDateTime::from(literal.0);
        let days = (value - chrono::NaiveDateTime::UNIX_EPOCH).num_days();
        let days = i32::try_from(days)
            .map_err(|_| SqlError::invalid(format!("date literal: {:?}", literal.0)))?;
        Ok(spec::Literal::Date32 { days: Some(days) })
    }
}

impl TryFrom<LiteralValue<(chrono::DateTime<Utc>, TimeZoneVariant)>> for spec::Literal {
    type Error = SqlError;

    fn try_from(
        literal: LiteralValue<(chrono::DateTime<Utc>, TimeZoneVariant)>,
    ) -> SqlResult<spec::Literal> {
        let (dt, ref tz) = literal.0;
        let (delta, timezone_info) = match tz {
            TimeZoneVariant::FixedOffset(tz) => (
                TimeZoneVariant::time_delta_from_unix_epoch(&dt, tz)?,
                spec::TimeZoneInfo::TimeZone {
                    timezone: Some("UTC".into()),
                },
            ),
            TimeZoneVariant::Named(tz) => (
                TimeZoneVariant::time_delta_from_unix_epoch(&dt, tz)?,
                spec::TimeZoneInfo::TimeZone {
                    timezone: Some("UTC".into()),
                },
            ),
            TimeZoneVariant::Utc => (
                dt - chrono::DateTime::UNIX_EPOCH,
                spec::TimeZoneInfo::TimeZone {
                    timezone: Some("UTC".into()),
                },
            ),
            TimeZoneVariant::None => (
                // FIXME: See FIXME in `PlanResolver::resolve_timezone` for more details.
                dt - chrono::DateTime::UNIX_EPOCH,
                spec::TimeZoneInfo::SQLConfigured,
            ),
        };
        let microseconds = delta.num_microseconds().ok_or_else(|| {
            SqlError::invalid(format!(
                "TryFrom<LiteralValue<(chrono::NaiveDateTime, TimeZoneVariant)>> {literal:?}"
            ))
        })?;
        Ok(spec::Literal::TimestampMicrosecond {
            microseconds: Some(microseconds),
            timezone_info,
        })
    }
}

impl TryFrom<LiteralValue<Signed<ast::Interval>>> for spec::Literal {
    type Error = SqlError;

    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`

    fn try_from(literal: LiteralValue<Signed<ast::Interval>>) -> SqlResult<spec::Literal> {
        use ast::{DateTimeField, Interval, IntervalUnit, IntervalValueWithUnit};

        let Signed(interval, negated) = literal.0;
        let error = || SqlError::invalid(format!("interval: {:?}", interval));
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
                        parse_interval_year_month_string(&value, negated, &INTERVAL_YEAR_REGEX)
                    }
                    (
                        Some(DateTimeField::Year | DateTimeField::Years),
                        Some(DateTimeField::Month | DateTimeField::Months),
                    ) => parse_interval_year_month_string(
                        &value,
                        negated,
                        &INTERVAL_YEAR_TO_MONTH_REGEX,
                    ),
                    (Some(DateTimeField::Month | DateTimeField::Months), None) => {
                        parse_interval_year_month_string(&value, negated, &INTERVAL_MONTH_REGEX)
                    }
                    (Some(DateTimeField::Day | DateTimeField::Days), None) => {
                        parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_REGEX)
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                    ) => {
                        parse_interval_day_time_string(&value, negated, &INTERVAL_DAY_TO_HOUR_REGEX)
                    }
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => parse_interval_day_time_string(
                        &value,
                        negated,
                        &INTERVAL_DAY_TO_MINUTE_REGEX,
                    ),
                    (
                        Some(DateTimeField::Day | DateTimeField::Days),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_interval_day_time_string(
                        &value,
                        negated,
                        &INTERVAL_DAY_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Hour | DateTimeField::Hours), None) => {
                        parse_interval_day_time_string(&value, negated, &INTERVAL_HOUR_REGEX)
                    }
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                    ) => parse_interval_day_time_string(
                        &value,
                        negated,
                        &INTERVAL_HOUR_TO_MINUTE_REGEX,
                    ),
                    (
                        Some(DateTimeField::Hour | DateTimeField::Hours),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_interval_day_time_string(
                        &value,
                        negated,
                        &INTERVAL_HOUR_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Minute | DateTimeField::Minutes), None) => {
                        parse_interval_day_time_string(&value, negated, &INTERVAL_MINUTE_REGEX)
                    }
                    (
                        Some(DateTimeField::Minute | DateTimeField::Minutes),
                        Some(DateTimeField::Second | DateTimeField::Seconds),
                    ) => parse_interval_day_time_string(
                        &value,
                        negated,
                        &INTERVAL_MINUTE_TO_SECOND_REGEX,
                    ),
                    (Some(DateTimeField::Second | DateTimeField::Seconds), None) => {
                        parse_interval_day_time_string(&value, negated, &INTERVAL_SECOND_REGEX)
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
                    _ => Err(error()),
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
                    .collect::<SqlResult<_>>()?;
                parse_multi_unit_interval(values, negated)
            }
        }
    }
}

impl TryFrom<&str> for LiteralValue<Vec<u8>> {
    type Error = SqlError;

    /// [Credit]: <https://github.com/apache/datafusion/blob/a0a635afe481b7b3cdc89591f9eff209010b911a/datafusion/sql/src/expr/value.rs#L285-L306>
    fn try_from(value: &str) -> SqlResult<Self> {
        if !BINARY_REGEX.is_match(value) {
            return Err(SqlError::invalid(format!("hex string: {value}")));
        }

        let hex_bytes = value.as_bytes();
        let mut decoded_bytes = Vec::with_capacity((hex_bytes.len() + 1) / 2);

        let start_idx = hex_bytes.len() % 2;
        if start_idx > 0 {
            // The first byte is formed of only one char.
            match try_decode_hex_char(hex_bytes[0])? {
                Some(byte) => decoded_bytes.push(byte),
                None => return Err(SqlError::invalid(format!("hex string: {value}"))),
            };
        }

        for i in (start_idx..hex_bytes.len()).step_by(2) {
            match (
                try_decode_hex_char(hex_bytes[i])?,
                try_decode_hex_char(hex_bytes[i + 1])?,
            ) {
                (Some(high), Some(low)) => decoded_bytes.push(high << 4 | low),
                _ => return Err(SqlError::invalid(format!("hex string: {value}"))),
            }
        }

        Ok(LiteralValue(decoded_bytes))
    }
}

impl TryFrom<&str> for LiteralValue<i8> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let value = value
            .parse::<i8>()
            .map_err(|_| SqlError::invalid(format!("tinyint: {:?}", value)))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<&str> for LiteralValue<i16> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let value = value
            .parse::<i16>()
            .map_err(|_| SqlError::invalid(format!("smallint: {:?}", value)))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<&str> for LiteralValue<i32> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let value = value
            .parse::<i32>()
            .map_err(|_| SqlError::invalid(format!("int: {:?}", value)))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<&str> for LiteralValue<i64> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let value = value
            .parse::<i64>()
            .map_err(|_| SqlError::invalid(format!("bigint: {:?}", value)))?;
        Ok(LiteralValue(value))
    }
}

impl TryFrom<&str> for LiteralValue<f32> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let n = value
            .parse::<f32>()
            .map_err(|_| SqlError::invalid(format!("float: {:?}", value)))?;
        if n.is_infinite() || n.is_nan() {
            return Err(SqlError::invalid(format!(
                "out-of-range float: {:?}",
                value
            )));
        }
        Ok(LiteralValue(n))
    }
}

impl TryFrom<&str> for LiteralValue<f64> {
    type Error = SqlError;

    fn try_from(value: &str) -> SqlResult<Self> {
        let n = value
            .parse::<f64>()
            .map_err(|_| SqlError::invalid(format!("double: {:?}", value)))?;
        if n.is_infinite() || n.is_nan() {
            return Err(SqlError::invalid(format!(
                "out-of-range double: {:?}",
                value
            )));
        }
        Ok(LiteralValue(n))
    }
}

struct DecimalSecond {
    seconds: u32,
    microseconds: u32,
}

impl FromStr for Signed<DecimalSecond> {
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        let error = || SqlError::invalid(format!("second: {:?}", s));
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
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        let v = s.parse::<T>().map_err(|_| SqlError::invalid(s))?;
        Ok(Signed(v, false))
    }
}

impl<T> TryFrom<ast::Expr> for LiteralValue<T>
where
    T: Neg<Output = T> + FromStr,
{
    type Error = SqlError;

    fn try_from(expr: ast::Expr) -> SqlResult<LiteralValue<T>> {
        use ast::Value;

        let error = || SqlError::invalid(format!("expression: {:?}", expr));
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
                let value = value.parse::<T>().map_err(|_| error())?;
                Ok(LiteralValue(value))
            }
            _ => Err(error()),
        }
    }
}

fn extract_match<T>(
    captures: &regex::Captures,
    name: &str,
    error: impl FnOnce() -> SqlError,
) -> SqlResult<Option<T>>
where
    T: FromStr,
{
    captures
        .name(name)
        .map(|x| x.as_str().parse::<T>())
        .transpose()
        .map_err(|_| error())
}

fn extract_second_fraction_match<T>(
    captures: &regex::Captures,
    name: &str,
    n: usize,
    error: impl FnOnce() -> SqlError,
) -> SqlResult<Option<T>>
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
        .map_err(|_| error())
}

pub fn parse_decimal_128_string(s: &str) -> SqlResult<spec::Literal> {
    let decimal_literal = parse_decimal_string(s)?;
    match decimal_literal {
        decimal128 @ spec::Literal::Decimal128 { .. } => Ok(decimal128),
        _ => Err(SqlError::invalid(format!(
            "Decimal128: {s} in parse_decimal_128_string"
        ))),
    }
}

pub fn parse_decimal_256_string(s: &str) -> SqlResult<spec::Literal> {
    let decimal_literal = parse_decimal_string(s)?;
    match decimal_literal {
        decimal256 @ spec::Literal::Decimal256 { .. } => Ok(decimal256),
        _ => Err(SqlError::invalid(format!(
            "Decimal256: {s} in parse_decimal_256_string"
        ))),
    }
}

pub fn parse_decimal_string(s: &str) -> SqlResult<spec::Literal> {
    let error = || SqlError::invalid(format!("decimal: {s}"));
    let captures = DECIMAL_REGEX
        .captures(s)
        .or_else(|| DECIMAL_FRACTION_REGEX.captures(s))
        .ok_or_else(error)?;
    let sign = captures.name("sign").map(|s| s.as_str()).unwrap_or("");
    let whole = captures
        .name("whole")
        .map(|s| s.as_str())
        .unwrap_or("")
        .trim_start_matches('0');
    let fraction = captures.name("fraction").map(|s| s.as_str()).unwrap_or("");
    let e: i8 = extract_match(&captures, "exponent", error)?.unwrap_or(0);
    let (whole, w, f) = match (whole, fraction) {
        ("", "") => ("0", 1i8, 0i8),
        (whole, fraction) => (whole, whole.len() as i8, fraction.len() as i8),
    };
    let (scale, padding) = {
        let scale = f.checked_sub(e).ok_or_else(error)?;
        if !(-ARROW_DECIMAL256_MAX_SCALE..=ARROW_DECIMAL256_MAX_SCALE).contains(&scale) {
            return Err(error());
        }
        if scale < 0 {
            // Although the decimal type allows negative scale,
            // we always parse the literal as zero-scale and add padding.
            (0, -scale)
        } else {
            (scale, 0)
        }
    };
    let width = w + f + padding;
    let precision = std::cmp::max(width, scale) as u8;
    let num = format!("{whole}{fraction}");
    let width = width as usize;
    let value = format!("{sign}{num:0<width$}");
    if precision == 0
        || precision > ARROW_DECIMAL256_MAX_PRECISION
        || scale.unsigned_abs() > precision
    {
        Err(error())
    } else if precision > ARROW_DECIMAL128_MAX_PRECISION {
        let value: i256 = value.parse().map_err(|_| error())?;
        Ok(spec::Literal::Decimal256 {
            precision,
            scale,
            value: Some(value),
        })
    } else {
        let value: i128 = value.parse().map_err(|_| error())?;
        Ok(spec::Literal::Decimal128 {
            precision,
            scale,
            value: Some(value),
        })
    }
}

pub fn parse_date_string(s: &str) -> SqlResult<spec::Literal> {
    let error = || SqlError::invalid(format!("date: {s}"));
    let captures = DATE_REGEX.captures(s).ok_or_else(error)?;
    let year = extract_match(&captures, "year", error)?.ok_or_else(error)?;
    let month = extract_match(&captures, "month", error)?.unwrap_or(1);
    let day = extract_match(&captures, "day", error)?.unwrap_or(1);
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| SqlError::invalid(format!("date: {s}")))?;
    spec::Literal::try_from(LiteralValue(date))
}

pub fn parse_timestamp_string(s: &str) -> SqlResult<spec::Literal> {
    let error = |msg: &str| SqlError::invalid(format!("{msg} error when parsing timestamp: {s}"));
    let captures = TIMESTAMP_REGEX
        .captures(s)
        .ok_or_else(|| error("Invalid format"))?;
    let year = extract_match(&captures, "year", || error("Invalid year"))?
        .ok_or_else(|| error("Missing year"))?;
    let month = extract_match(&captures, "month", || error("Invalid month"))?.unwrap_or(1);
    let day = extract_match(&captures, "day", || error("Invalid day"))?.unwrap_or(1);
    let hour = extract_match(&captures, "hour", || error("Invalid hour"))?.unwrap_or(0);
    let minute = extract_match(&captures, "minute", || error("Invalid minute"))?.unwrap_or(0);
    let second = extract_match(&captures, "second", || error("Invalid second"))?.unwrap_or(0);
    let fraction =
        extract_second_fraction_match(&captures, "fraction", 6, || error("Invalid fraction"))?
            .unwrap_or(0);
    let tz = captures.name("tz").map(|tz| tz.as_str());
    let tz = parse_timezone_string(tz)?;
    let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|d| d.and_hms_opt(hour, minute, second))
        .and_then(|d| d.checked_add_signed(chrono::Duration::microseconds(fraction)))
        .ok_or_else(|| error("Invalid date/time values"))?;
    let dt = chrono::DateTime::from_naive_utc_and_offset(dt, Utc);
    spec::Literal::try_from(LiteralValue((dt, tz)))
}

#[derive(Debug)]
pub(crate) enum TimeZoneVariant {
    None,
    Utc,
    FixedOffset(chrono::FixedOffset),
    Named(Tz),
}

impl TimeZoneVariant {
    fn time_delta_from_unix_epoch<Tz, O>(
        dt: &chrono::DateTime<Utc>,
        tz: &Tz,
    ) -> SqlResult<TimeDelta>
    where
        Tz: chrono::TimeZone<Offset = O> + Debug,
        O: chrono::Offset,
    {
        let offset_seconds = tz
            .offset_from_utc_datetime(&dt.naive_utc())
            .fix()
            .local_minus_utc() as i64;
        let adjusted_dt = dt.add(TimeDelta::try_seconds(offset_seconds).ok_or_else(|| {
            SqlError::invalid("time_delta_from_unix_epoch: Invalid offset seconds")
        })?);
        Ok(adjusted_dt - chrono::DateTime::UNIX_EPOCH)
    }
}

fn parse_timezone_string(tz: Option<&str>) -> SqlResult<TimeZoneVariant> {
    match tz {
        None => Ok(TimeZoneVariant::None),
        Some(tz) if tz.trim().is_empty() => Ok(TimeZoneVariant::None),
        Some(tz) if tz.to_uppercase().trim() == "Z" => Ok(TimeZoneVariant::Utc),
        Some(tz) => {
            let error = || SqlError::invalid(format!("timezone in parse_timezone_string: {tz}"));
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
                let tz = tz.parse::<Tz>().map_err(|_| error())?;
                Ok(TimeZoneVariant::Named(tz))
            }
        }
    }
}

fn parse_interval_year_month_string(
    s: &str,
    negated: Negated,
    interval_regex: &regex::Regex,
) -> SqlResult<spec::Literal> {
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
    Ok(spec::Literal::IntervalYearMonth { months: Some(n) })
}

fn parse_interval_day_time_string(
    s: &str,
    negated: Negated,
    interval_regex: &regex::Regex,
) -> SqlResult<spec::Literal> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let days: i64 = extract_match(&captures, "day", error)?.unwrap_or(0);
    let hours: i64 = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minutes: i64 = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let seconds: i64 = extract_match(&captures, "second", error)?.unwrap_or(0);
    let microseconds: i64 =
        extract_second_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
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
    Ok(microseconds_to_interval(n))
}

fn parse_multi_unit_interval(
    values: Vec<(ast::Expr, ast::DateTimeField)>,
    negated: Negated,
) -> SqlResult<spec::Literal> {
    use ast::DateTimeField;

    let error = || SqlError::invalid("multi-unit interval");
    let mut months = 0i32;
    let mut delta = TimeDelta::zero();
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
                let weeks = TimeDelta::try_weeks(value).ok_or_else(error)?;
                delta = delta.checked_add(&weeks).ok_or_else(error)?;
            }
            DateTimeField::Day | DateTimeField::Days => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let days = TimeDelta::try_days(value).ok_or_else(error)?;
                delta = delta.checked_add(&days).ok_or_else(error)?;
            }
            DateTimeField::Hour | DateTimeField::Hours => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let hours = TimeDelta::try_hours(value).ok_or_else(error)?;
                delta = delta.checked_add(&hours).ok_or_else(error)?;
            }
            DateTimeField::Minute | DateTimeField::Minutes => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let minutes = TimeDelta::try_minutes(value).ok_or_else(error)?;
                delta = delta.checked_add(&minutes).ok_or_else(error)?;
            }
            DateTimeField::Second | DateTimeField::Seconds => {
                let Signed(value, negated) =
                    LiteralValue::<Signed<DecimalSecond>>::try_from(expr)?.0;
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
            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let milliseconds = TimeDelta::try_milliseconds(value).ok_or_else(error)?;
                delta = delta.checked_add(&milliseconds).ok_or_else(error)?;
            }
            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                let value = LiteralValue::<i64>::try_from(expr)?.0;
                let microseconds = TimeDelta::microseconds(value);
                delta = delta.checked_add(&microseconds).ok_or_else(error)?;
            }
            _ => return Err(error()),
        }
    }
    match (months != 0, delta != TimeDelta::zero()) {
        (true, false) => {
            let n = if negated {
                months.checked_mul(-1).ok_or_else(error)?
            } else {
                months
            };
            Ok(spec::Literal::IntervalYearMonth { months: Some(n) })
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

            Ok(spec::Literal::IntervalMonthDayNano {
                value: Some(spec::IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                }),
            })
        }
        (false, _) => {
            let microseconds = delta.num_microseconds().ok_or_else(error)?;
            let n = if negated {
                microseconds.checked_mul(-1).ok_or_else(error)?
            } else {
                microseconds
            };
            Ok(microseconds_to_interval(n))
        }
    }
}

pub fn microseconds_to_interval(microseconds: i64) -> spec::Literal {
    let total_days = microseconds / (24 * 60 * 60 * 1_000_000);
    let remaining_micros = microseconds % (24 * 60 * 60 * 1_000_000);
    if remaining_micros % 1000 == 0 {
        spec::Literal::IntervalDayTime {
            value: Some(spec::IntervalDayTime {
                days: total_days as i32,
                milliseconds: (remaining_micros / 1000) as i32,
            }),
        }
    } else {
        spec::Literal::IntervalMonthDayNano {
            value: Some(spec::IntervalMonthDayNano {
                months: 0,
                days: total_days as i32,
                nanoseconds: remaining_micros * 1000,
            }),
        }
    }
}

fn parse_unqualified_interval_string(s: &str, negated: Negated) -> SqlResult<spec::Literal> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(s)?;
    // consume the `INTERVAL` keyword if any
    let _ = parser.parse_keyword(Keyword::INTERVAL);
    let expr = parser.parse_interval()?;
    fail_on_extra_token(&mut parser, "interval")?;
    let interval = match expr {
        ast::Expr::Interval(interval) => interval,
        _ => return Err(SqlError::invalid(format!("interval: {s}"))),
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
        return Err(SqlError::invalid(format!("interval: {s}")));
    }
    spec::Literal::try_from(LiteralValue(Signed(interval, negated)))
}

/// [Credit]: <https://github.com/apache/datafusion/blob/a0a635afe481b7b3cdc89591f9eff209010b911a/datafusion/sql/src/expr/value.rs#L308-L318>
/// Try to decode a byte from a hex char.
///
/// None will be returned if the input char is hex-invalid.
const fn try_decode_hex_char(c: u8) -> SqlResult<Option<u8>> {
    match c {
        b'A'..=b'F' => Ok(Some(c - b'A' + 10)),
        b'a'..=b'f' => Ok(Some(c - b'a' + 10)),
        b'0'..=b'9' => Ok(Some(c - b'0')),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decimal128() -> SqlResult<()> {
        use sail_common::spec::Literal;
        let parse = |x: &str| -> SqlResult<Literal> { parse_decimal_128_string(x) };
        assert_eq!(
            parse("123.45")?,
            Literal::Decimal128 {
                precision: 5,
                scale: 2,
                value: Some(12345)
            }
        );
        assert_eq!(
            parse("-123.45")?,
            Literal::Decimal128 {
                precision: 5,
                scale: 2,
                value: Some(-12345),
            }
        );
        assert_eq!(
            parse("123.45e1")?,
            Literal::Decimal128 {
                precision: 5,
                scale: 1,
                value: Some(12345),
            }
        );
        assert_eq!(
            parse("123.45E-2")?,
            Literal::Decimal128 {
                precision: 5,
                scale: 4,
                value: Some(12345),
            }
        );
        assert_eq!(
            parse("1.23e10")?,
            Literal::Decimal128 {
                precision: 11,
                scale: 0,
                value: Some(12300000000),
            }
        );
        assert_eq!(
            parse("1.23E-10")?,
            Literal::Decimal128 {
                precision: 12,
                scale: 12,
                value: Some(123),
            }
        );
        assert_eq!(
            parse("0")?,
            Literal::Decimal128 {
                precision: 1,
                scale: 0,
                value: Some(0),
            }
        );
        assert_eq!(
            parse("0.")?,
            Literal::Decimal128 {
                precision: 1,
                scale: 0,
                value: Some(0),
            }
        );
        assert_eq!(
            parse("0.0")?,
            Literal::Decimal128 {
                precision: 1,
                scale: 1,
                value: Some(0),
            }
        );
        assert_eq!(
            parse(".0")?,
            Literal::Decimal128 {
                precision: 1,
                scale: 1,
                value: Some(0),
            }
        );
        assert_eq!(
            parse(".0e1")?,
            Literal::Decimal128 {
                precision: 1,
                scale: 0,
                value: Some(0),
            }
        );
        assert_eq!(
            parse(".0e-1")?,
            Literal::Decimal128 {
                precision: 2,
                scale: 2,
                value: Some(0),
            }
        );
        assert_eq!(
            parse("001.2")?,
            Literal::Decimal128 {
                precision: 2,
                scale: 1,
                value: Some(12),
            }
        );
        assert_eq!(
            parse("001.20")?,
            Literal::Decimal128 {
                precision: 3,
                scale: 2,
                value: Some(120),
            }
        );
        assert!(parse(".").is_err());
        assert!(parse("123.456.789").is_err());
        assert!(parse("1E100").is_err());
        assert!(parse("-.2E-100").is_err());
        assert!(parse("12345678901234567890123456789012345678").is_ok());
        assert!(parse("123456789012345678901234567890123456789").is_err());
        Ok(())
    }

    #[test]
    fn test_parse_decimal256() -> SqlResult<()> {
        use sail_common::spec::Literal;
        let parse = |x: &str| -> SqlResult<Literal> { parse_decimal_256_string(x) };
        assert!(parse(".").is_err());
        assert!(parse("123.456.789").is_err());
        assert!(parse("1E100").is_err());
        assert!(parse("-.2E-100").is_err());
        assert_eq!(
            parse("120000000000000000000000000000000000000000")?,
            Literal::Decimal256 {
                precision: 42,
                scale: 4,
                value: i256::from_string("120000000000000000000000000000000000000000")
            }
        );
        assert_eq!(
            parse("1200000000000000000000000000000000000.00000")?,
            Literal::Decimal256 {
                precision: 42,
                scale: 5,
                value: i256::from_string("120000000000000000000000000000000000000000")
            }
        );
        assert!(parse("123456789012345678901234567890123456789").is_ok());
        assert!(parse(
            "1234567890123456789012345678901234567891234567890123456789012345678901234567"
        )
        .is_ok());
        assert!(parse(
            "12345678901234567890123456789012345678912345678901234567890123456789012345677"
        )
        .is_err());
        Ok(())
    }

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
