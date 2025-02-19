use std::fmt;

use chumsky::error::{Rich, RichReason};
use sail_common::error::CommonError;
use sail_sql_parser::span::TokenSpan;
use thiserror::Error;

pub type SqlResult<T> = Result<T, SqlError>;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("error in SQL parser: {0}")]
    SqlParserError(String),
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl SqlError {
    pub fn todo(message: impl Into<String>) -> Self {
        SqlError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        SqlError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        SqlError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        SqlError::InvalidArgument(message.into())
    }

    pub fn parser<T, S, L>(errors: Vec<Rich<'_, T, S, L>>) -> Self
    where
        T: fmt::Display,
        S: Into<TokenSpan> + Clone,
        L: fmt::Display,
    {
        SqlError::SqlParserError(
            errors
                .into_iter()
                .map(|e| format!("{}", ParserErrorDisplay(&e)))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

impl From<CommonError> for SqlError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => SqlError::MissingArgument(message),
            CommonError::InvalidArgument(message) => SqlError::InvalidArgument(message),
            CommonError::NotSupported(message) => SqlError::NotSupported(message),
            CommonError::InternalError(message) => SqlError::InternalError(message),
        }
    }
}

struct ParserErrorDisplay<'e, 'a, T, S, L>(&'e Rich<'a, T, S, L>);

impl<T, S, L> fmt::Display for ParserErrorDisplay<'_, '_, T, S, L>
where
    T: fmt::Display,
    S: Into<TokenSpan> + Clone,
    L: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_span(f: &mut fmt::Formatter<'_>, span: &TokenSpan) -> fmt::Result {
            if !span.is_empty() {
                write!(f, " at {}:{}", span.start, span.end)?;
            }
            Ok(())
        }

        let Self(error) = self;
        let span: TokenSpan = error.span().clone().into();
        match error.reason() {
            RichReason::ExpectedFound { expected, found } => {
                write!(f, "found ")?;
                match found {
                    None => write!(f, "end of input")?,
                    Some(x) => {
                        write!(f, "{}", **x)?;
                        write_span(f, &span)?;
                    }
                }
                write!(f, " expected ")?;
                match &expected[..] {
                    [] => write!(f, "something else")?,
                    [item] => write!(f, "{item}")?,
                    [items @ .., last] => {
                        for item in items {
                            write!(f, "{item}, ")?;
                        }
                        write!(f, "or {last}")?;
                    }
                }
            }
            RichReason::Custom(message) => {
                write!(f, "{message}")?;
                write_span(f, &span)?;
            }
            RichReason::Many(_) => {
                write!(f, "multiple errors")?;
            }
        }
        for (label, span) in error.contexts() {
            write!(f, " in {label}")?;
            write_span(f, &span.clone().into())?;
        }
        Ok(())
    }
}
