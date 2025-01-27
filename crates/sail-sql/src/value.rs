use sail_common::spec;
use sail_sql_parser::ast::expression::BooleanLiteral;
use sail_sql_parser::ast::literal::{NumberLiteral, StringLiteral};

use crate::error::{SqlError, SqlResult};
use crate::literal::{parse_decimal_128_string, parse_decimal_256_string, LiteralValue};

pub(crate) fn from_ast_number_literal(value: NumberLiteral) -> SqlResult<spec::Expr> {
    let NumberLiteral {
        span: _,
        value,
        suffix,
    } = value;
    match suffix.as_str() {
        "Y" | "y" => {
            let value = LiteralValue::<i8>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        "S" | "s" => {
            let value = LiteralValue::<i16>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        "L" | "l" => {
            let value = LiteralValue::<i64>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        "F" | "f" => {
            let value = LiteralValue::<f32>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        "D" | "d" => {
            let value = LiteralValue::<f64>::try_from(value.as_str())?;
            spec::Expr::try_from(value)
        }
        x if x.to_uppercase() == "BD" => {
            if let Ok(value) = parse_decimal_128_string(value.as_str()) {
                Ok(spec::Expr::Literal(value))
            } else {
                Ok(spec::Expr::Literal(parse_decimal_256_string(
                    value.as_str(),
                )?))
            }
        }
        "" => {
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
        _ => Err(SqlError::invalid(format!(
            "number postfix: {value}{suffix}",
        ))),
    }
}

pub(crate) fn from_ast_string_literal(value: StringLiteral) -> SqlResult<spec::Expr> {
    let StringLiteral {
        span: _,
        value,
        style,
    } = value;
    match style.prefix() {
        Some('x' | 'X') => {
            let value: LiteralValue<Vec<u8>> = value.as_str().try_into()?;
            spec::Expr::try_from(value)
        }
        // TODO: handle escape strings
        _ => spec::Expr::try_from(LiteralValue(value)),
    }
}

pub(crate) fn from_ast_boolean_literal(value: BooleanLiteral) -> SqlResult<spec::Expr> {
    let value = match value {
        BooleanLiteral::True(_) => true,
        BooleanLiteral::False(_) => false,
    };
    spec::Expr::try_from(LiteralValue(value))
}
