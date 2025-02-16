use thiserror::Error;

pub type CommonResult<T> = Result<T, CommonError>;

#[derive(Debug, Error)]
pub enum CommonError {
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl CommonError {
    pub fn missing(message: impl Into<String>) -> Self {
        CommonError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        CommonError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        CommonError::NotSupported(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        CommonError::InternalError(message.into())
    }
}
