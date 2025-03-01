use sail_common::spec;
use sail_sql_parser::ast::expression::BooleanLiteral;
use sail_sql_parser::ast::literal::{NumberLiteral, NumberSuffix, StringLiteral};
use sail_sql_parser::string::StringValue;

use crate::error::{SqlError, SqlResult};
use crate::literal::{parse_decimal_128_string, parse_decimal_256_string, LiteralValue};

pub(crate) fn from_ast_number_literal(value: NumberLiteral) -> SqlResult<spec::Expr> {
    let NumberLiteral {
        span: _,
        value,
        suffix,
    } = value;
    match suffix {
        Some(NumberSuffix::Y) => {
            let value = LiteralValue::<i8>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        Some(NumberSuffix::S) => {
            let value = LiteralValue::<i16>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        Some(NumberSuffix::L) => {
            let value = LiteralValue::<i64>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        Some(NumberSuffix::F) => {
            let value = LiteralValue::<f32>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        Some(NumberSuffix::D) => {
            let value = LiteralValue::<f64>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        Some(NumberSuffix::Bd) => {
            if let Ok(value) = parse_decimal_128_string(value.as_str()) {
                Ok(spec::Expr::Literal(value))
            } else {
                Ok(spec::Expr::Literal(parse_decimal_256_string(
                    value.as_str(),
                )?))
            }
        }
        None => {
            if let Ok(value) = LiteralValue::<i32>::try_from(value.as_str()) {
                spec::Expr::try_from(value)
            } else if let Ok(value) = LiteralValue::<i64>::try_from(value.as_str()) {
                spec::Expr::try_from(value)
            } else if let Ok(value) = parse_decimal_128_string(value.as_str()) {
                Ok(spec::Expr::Literal(value))
            } else {
                Ok(spec::Expr::Literal(parse_decimal_256_string(
                    value.as_str(),
                )?))
            }
        }
    }
}

/// Multiple string literals can be concatenated together to form a single
/// literal expression. This is a helper to handle such logic.
enum StringLiteralList {
    Binary(Vec<u8>),
    Text(String),
}

impl StringLiteralList {
    fn try_new(value: StringLiteral) -> SqlResult<Self> {
        let StringLiteral { span: _, value } = value;
        match value {
            StringValue::Valid {
                value,
                prefix: Some('x' | 'X'),
            } => {
                let LiteralValue(value) = value.as_str().try_into()?;
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
        match value {
            StringLiteralList::Binary(binary) => spec::Expr::try_from(LiteralValue(binary)),
            StringLiteralList::Text(text) => spec::Expr::try_from(LiteralValue(text)),
        }
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
    spec::Expr::try_from(LiteralValue(value))
}

pub(crate) fn from_ast_string(s: StringLiteral) -> SqlResult<String> {
    let StringLiteral { span: _, value } = s;
    match value {
        StringValue::Valid { value, prefix: _ } => Ok(value),
        StringValue::Invalid { reason } => Err(SqlError::invalid(reason)),
    }
}
