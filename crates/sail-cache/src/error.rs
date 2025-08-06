use sail_common::error::CommonError;
use thiserror::Error;
pub type CacheResult<T> = Result<T, CacheError>;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl CacheError {
    pub fn missing(message: impl Into<String>) -> Self {
        CacheError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        CacheError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        CacheError::NotSupported(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        CacheError::InternalError(message.into())
    }
}

impl From<CommonError> for CacheError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => CacheError::MissingArgument(message),
            CommonError::InvalidArgument(message) => CacheError::InvalidArgument(message),
            CommonError::NotSupported(message) => CacheError::NotSupported(message),
            CommonError::InternalError(message) => CacheError::InternalError(message),
        }
    }
}
