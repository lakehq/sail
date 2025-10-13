use sail_common::spec;
use sail_sql_parser::ast::expression::BooleanLiteral;
use sail_sql_parser::ast::literal::{NumberLiteral, NumberSuffix, StringLiteral};
use sail_sql_parser::string::StringValue;

use crate::error::{SqlError, SqlResult};
use crate::literal::binary::BinaryValue;
use crate::literal::numeric::{
    parse_decimal_128_string, parse_decimal_256_string, parse_f32_string, parse_f64_string,
    parse_i16_string, parse_i32_string, parse_i64_string, parse_i8_string,
};

pub(crate) fn from_ast_number_literal(value: NumberLiteral) -> SqlResult<spec::Expr> {
    let NumberLiteral {
        span: _,
        value,
        suffix,
    } = value;
    let literal = match suffix {
        Some(NumberSuffix::Y) => parse_i8_string(&value)?,
        Some(NumberSuffix::S) => parse_i16_string(&value)?,
        Some(NumberSuffix::L) => parse_i64_string(&value)?,
        Some(NumberSuffix::F) => parse_f32_string(&value)?,
        Some(NumberSuffix::D) => parse_f64_string(&value)?,
        Some(NumberSuffix::Bd) => {
            parse_decimal_128_string(&value).or_else(|_| parse_decimal_256_string(&value))?
        }
        None => {
            if value.contains('e') || value.contains('E') {
                parse_f64_string(&value)
                    .or_else(|_| parse_decimal_128_string(&value))
                    .or_else(|_| parse_decimal_256_string(&value))?
            } else {
                parse_i32_string(&value)
                    .or_else(|_| parse_i64_string(&value))
                    .or_else(|_| parse_decimal_128_string(&value))
                    .or_else(|_| parse_decimal_256_string(&value))?
            }
        }
    };
    Ok(spec::Expr::Literal(literal))
}

/// Multiple string literals can be concatenated together to form a single
/// literal expression. This is a helper to handle such logic.
enum StringLiteralList {
    Binary(Vec<u8>),
    Text(String),
}

impl StringLiteralList {
    fn try_new(value: StringLiteral) -> SqlResult<Self> {
        let StringLiteral {
            span: _,
            tokens: _,
            value,
        } = value;
        match value {
            StringValue::Valid {
                value,
                prefix: Some('x' | 'X'),
            } => {
                let BinaryValue(value) = value.parse()?;
                Ok(Self::Binary(value))
            }
            StringValue::Valid { value, prefix: _ } => Ok(Self::Text(value)),
            StringValue::Invalid { reason } => Err(SqlError::invalid(reason)),
        }
    }

    fn try_extend(mut self, other: Self) -> SqlResult<Self> {
        match (&mut self, other) {
            (Self::Binary(binary), Self::Binary(other)) => {
                binary.extend(other);
                Ok(self)
            }
            (Self::Text(text), Self::Text(other)) => {
                text.push_str(&other);
                Ok(self)
            }
            _ => Err(SqlError::invalid("cannot mix binary and string literals")),
        }
    }
}

impl TryFrom<StringLiteralList> for spec::Expr {
    type Error = SqlError;

    fn try_from(value: StringLiteralList) -> Result<Self, Self::Error> {
        let literal = match value {
            StringLiteralList::Binary(binary) => spec::Literal::Binary {
                value: Some(binary),
            },
            StringLiteralList::Text(text) => spec::Literal::Utf8 { value: Some(text) },
        };
        Ok(spec::Expr::Literal(literal))
    }
}

pub(crate) fn from_ast_string_literal(
    head: StringLiteral,
    tail: Vec<StringLiteral>,
) -> SqlResult<spec::Expr> {
    let head = StringLiteralList::try_new(head)?;
    tail.into_iter()
        .try_fold(head, |acc, x| {
            acc.try_extend(StringLiteralList::try_new(x)?)
        })?
        .try_into()
}

pub(crate) fn from_ast_boolean_literal(value: BooleanLiteral) -> SqlResult<spec::Expr> {
    let value = match value {
        BooleanLiteral::True(_) => true,
        BooleanLiteral::False(_) => false,
    };
    Ok(spec::Expr::Literal(spec::Literal::Boolean {
        value: Some(value),
    }))
}

pub(crate) fn from_ast_string(s: StringLiteral) -> SqlResult<String> {
    let StringLiteral {
        span: _,
        tokens: _,
        value,
    } = s;
    match value {
        StringValue::Valid { value, prefix: _ } => Ok(value),
        StringValue::Invalid { reason } => Err(SqlError::invalid(reason)),
    }
}
