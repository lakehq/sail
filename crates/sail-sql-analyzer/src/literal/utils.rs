use std::ops::Neg;
use std::str::FromStr;

use sail_sql_parser::ast::expression::{AtomExpr, Expr, UnaryOperator};
use sail_sql_parser::ast::literal::NumberLiteral;

use crate::error::{SqlError, SqlResult};
use crate::value::from_ast_string;

pub enum Signed<T> {
    Positive(T),
    Negative(T),
}

impl<T> Signed<T> {
    pub fn into_inner(self) -> T {
        match self {
            Signed::Positive(x) => x,
            Signed::Negative(x) => x,
        }
    }

    pub fn is_negative(&self) -> bool {
        match self {
            Signed::Positive(_) => false,
            Signed::Negative(_) => true,
        }
    }
}

impl<T> Neg for Signed<T> {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Signed::Positive(x) => Signed::Negative(x),
            Signed::Negative(x) => Signed::Positive(x),
        }
    }
}

impl<T: FromStr> FromStr for Signed<T> {
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        let v = s.parse::<T>().map_err(|_| SqlError::invalid(s))?;
        Ok(Signed::Positive(v))
    }
}

pub fn parse_signed_value<T>(expr: Expr) -> SqlResult<T>
where
    T: Neg<Output = T> + FromStr,
{
    match expr {
        Expr::UnaryOperator(UnaryOperator::Minus(_), expr) => {
            let value: T = parse_signed_value(*expr)?;
            Ok(-value)
        }
        Expr::Atom(AtomExpr::NumberLiteral(NumberLiteral {
            span: _,
            value,
            suffix: None,
        })) => match value.parse::<T>() {
            Ok(x) => Ok(x),
            Err(_) => Err(SqlError::invalid(format!("literal: {value}"))),
        },
        Expr::Atom(AtomExpr::StringLiteral(head, tail)) => {
            if !tail.is_empty() {
                return Err(SqlError::invalid(
                    "literal: cannot convert multiple adjacent string literals to a single value",
                ));
            }
            let value = from_ast_string(head)?;
            match value.parse::<T>() {
                Ok(x) => Ok(x),
                Err(_) => Err(SqlError::invalid(format!("literal: {value}"))),
            }
        }
        _ => Err(SqlError::invalid(format!("literal expression: {expr:?}"))),
    }
}

pub fn extract_match<T>(
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

pub fn extract_fraction_match<T>(
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
