use sail_common::error::CommonError;
use thiserror::Error;
pub type OptimizerResult<T> = Result<T, OptimizerError>;

#[derive(Debug, Error)]
pub enum OptimizerError {
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl OptimizerError {
    pub fn missing(message: impl Into<String>) -> Self {
        OptimizerError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        OptimizerError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        OptimizerError::NotSupported(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        OptimizerError::InternalError(message.into())
    }
}

impl From<CommonError> for OptimizerError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => OptimizerError::MissingArgument(message),
            CommonError::InvalidArgument(message) => OptimizerError::InvalidArgument(message),
            CommonError::NotSupported(message) => OptimizerError::NotSupported(message),
            CommonError::InternalError(message) => OptimizerError::InternalError(message),
        }
    }
}
