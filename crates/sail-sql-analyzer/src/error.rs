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

    pub fn parser<T, S>(errors: Vec<Rich<'_, T, S>>) -> Self
    where
        T: fmt::Display,
        S: Into<TokenSpan> + Clone,
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

struct ParserErrorDisplay<'e, 'a, T, S>(&'e Rich<'a, T, S>);

impl<T, S> fmt::Display for ParserErrorDisplay<'_, '_, T, S>
where
    T: fmt::Display,
    S: Into<TokenSpan> + Clone,
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
                    None if span.is_empty() => {
                        write!(f, "end of input")?;
                    }
                    // It is unclear why the found token can be `None` when
                    // the span is not empty. But we take care of this case here.
                    None => {
                        write!(f, "something")?;
                        write_span(f, &span)?;
                    }
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
        }
        for (label, span) in error.contexts() {
            write!(f, " in {label}")?;
            write_span(f, &span.clone().into())?;
        }
        Ok(())
    }
}
