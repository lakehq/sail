use lazy_static::lazy_static;
use regex::Regex;
use sail_common::spec;
use sail_common::spec::{
    i256, ARROW_DECIMAL128_MAX_PRECISION, ARROW_DECIMAL256_MAX_PRECISION,
    ARROW_DECIMAL256_MAX_SCALE,
};

use crate::error::{SqlError, SqlResult};
use crate::literal::utils::extract_match;

fn create_regex(regex: Result<Regex, regex::Error>) -> Regex {
    #[allow(clippy::unwrap_used)]
    regex.unwrap()
}

lazy_static! {
    static ref DECIMAL_REGEX: Regex = create_regex(Regex::new(
        r"^(?P<sign>[+-]?)(?P<whole>\d{1,38})[.]?(?P<fraction>\d{0,38})([eE](?P<exponent>[+-]?\d+))?$"
    ));
    static ref DECIMAL_FRACTION_REGEX: regex::Regex = create_regex(Regex::new(
        r"^(?P<sign>[+-]?)[.](?P<fraction>\d{1,38})([eE](?P<exponent>[+-]?\d+))?$"
    ));
}

pub fn parse_i8_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<i8>()
        .map_err(|_| SqlError::invalid(format!("tinyint: {value}")))?;
    Ok(spec::Literal::Int8 { value: Some(n) })
}

pub fn parse_i16_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<i16>()
        .map_err(|_| SqlError::invalid(format!("smallint: {value}")))?;
    Ok(spec::Literal::Int16 { value: Some(n) })
}

pub fn parse_i32_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<i32>()
        .map_err(|_| SqlError::invalid(format!("int: {value}")))?;
    Ok(spec::Literal::Int32 { value: Some(n) })
}

pub fn parse_i64_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<i64>()
        .map_err(|_| SqlError::invalid(format!("bigint: {value}")))?;
    Ok(spec::Literal::Int64 { value: Some(n) })
}

pub fn parse_f32_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<f32>()
        .map_err(|_| SqlError::invalid(format!("float: {value}")))?;
    if n.is_infinite() || n.is_nan() {
        return Err(SqlError::invalid(format!("out-of-range float: {value}")));
    }
    Ok(spec::Literal::Float32 { value: Some(n) })
}

pub fn parse_f64_string(value: &str) -> SqlResult<spec::Literal> {
    let n = value
        .parse::<f64>()
        .map_err(|_| SqlError::invalid(format!("double: {value}")))?;
    if n.is_infinite() || n.is_nan() {
        return Err(SqlError::invalid(format!("out-of-range double: {value}")));
    }
    Ok(spec::Literal::Float64 { value: Some(n) })
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
                value: Some(12345),
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
                value: i256::from_string("120000000000000000000000000000000000000000"),
            }
        );
        assert_eq!(
            parse("1200000000000000000000000000000000000.00000")?,
            Literal::Decimal256 {
                precision: 42,
                scale: 5,
                value: i256::from_string("120000000000000000000000000000000000000000"),
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
}
