use std::fmt;

use sail_common::error::CommonError;
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

    pub fn parser<E>(errors: Vec<E>) -> Self
    where
        E: fmt::Display,
    {
        SqlError::SqlParserError(
            errors
                .into_iter()
                .map(|e| e.to_string())
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
