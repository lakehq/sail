use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::literal::LiteralValue;

pub(crate) fn from_ast_value(value: ast::Value) -> SqlResult<spec::Expr> {
    use ast::Value;

    match value {
        Value::Number(value, postfix) => match postfix.as_deref() {
            Some("Y") | Some("y") => {
                let value = LiteralValue::<i8>::try_from(value.clone())?;
                spec::Expr::try_from(value)
            }
            Some("S") | Some("s") => {
                let value = LiteralValue::<i16>::try_from(value.clone())?;
                spec::Expr::try_from(value)
            }
            Some("L") | Some("l") => {
                let value = LiteralValue::<i64>::try_from(value.clone())?;
                spec::Expr::try_from(value)
            }
            Some("F") | Some("f") => {
                let value = LiteralValue::<f32>::try_from(value.clone())?;
                spec::Expr::try_from(value)
            }
            Some("D") | Some("d") => {
                let value = LiteralValue::<f64>::try_from(value.clone())?;
                spec::Expr::try_from(value)
            }
            Some(x) if x.to_uppercase() == "BD" => {
                if let Ok(value) = LiteralValue::<spec::Decimal128>::try_from(value.clone()) {
                    spec::Expr::try_from(value)
                } else {
                    let value = LiteralValue::<spec::Decimal256>::try_from(value.clone())?;
                    spec::Expr::try_from(value)
                }
            }
            None | Some("") => {
                if let Ok(value) = LiteralValue::<i32>::try_from(value.clone()) {
                    spec::Expr::try_from(value)
                } else if let Ok(value) = LiteralValue::<i64>::try_from(value.clone()) {
                    spec::Expr::try_from(value)
                } else if let Ok(value) = LiteralValue::<spec::Decimal128>::try_from(value.clone())
                {
                    spec::Expr::try_from(value)
                } else {
                    let value = LiteralValue::<spec::Decimal256>::try_from(value.clone())?;
                    spec::Expr::try_from(value)
                }
            }
            Some(&_) => Err(SqlError::invalid(format!(
                "number postfix: {:?}{:?}",
                value, postfix
            ))),
        },
        Value::HexStringLiteral(value) => {
            let value: LiteralValue<Vec<u8>> = value.try_into()?;
            spec::Expr::try_from(value)
        }
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::DollarQuotedString(ast::DollarQuotedString { value, .. })
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value) => spec::Expr::try_from(LiteralValue(value)),
        Value::Boolean(value) => spec::Expr::try_from(LiteralValue(value)),
        Value::Null => Ok(spec::Expr::Literal(spec::Literal::Null)),
        Value::Placeholder(placeholder) => Ok(spec::Expr::Placeholder(placeholder)),
        Value::EscapedStringLiteral(_)
        | Value::SingleQuotedByteStringLiteral(_)
        | Value::DoubleQuotedByteStringLiteral(_)
        | Value::TripleSingleQuotedByteStringLiteral(_)
        | Value::TripleDoubleQuotedByteStringLiteral(_)
        | Value::SingleQuotedRawStringLiteral(_)
        | Value::DoubleQuotedRawStringLiteral(_)
        | Value::TripleSingleQuotedRawStringLiteral(_)
        | Value::TripleDoubleQuotedRawStringLiteral(_)
        | Value::NationalStringLiteral(_) => {
            Err(SqlError::unsupported(format!("value: {:?}", value)))
        }
    }
}

/// [Credit]: <https://github.com/apache/datafusion/blob/64a38963cb10b629ddfbd97e08208cc1c717ef2e/datafusion/sql/src/expr/value.rs#L288-L306>
pub(crate) fn try_decode_hex_literal(s: &str) -> Option<Vec<u8>> {
    let hex_bytes = s.as_bytes();

    let mut decoded_bytes = Vec::with_capacity((hex_bytes.len() + 1) / 2);

    let start_idx = hex_bytes.len() % 2;
    if start_idx > 0 {
        // The first byte is formed of only one char.
        decoded_bytes.push(try_decode_hex_char(hex_bytes[0])?);
    }

    for i in (start_idx..hex_bytes.len()).step_by(2) {
        let high = try_decode_hex_char(hex_bytes[i])?;
        let low = try_decode_hex_char(hex_bytes[i + 1])?;
        decoded_bytes.push(high << 4 | low);
    }

    Some(decoded_bytes)
}

/// [Credit]: <https://github.com/apache/datafusion/blob/64a38963cb10b629ddfbd97e08208cc1c717ef2e/datafusion/sql/src/expr/value.rs#L311-L318>
const fn try_decode_hex_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    }
}
