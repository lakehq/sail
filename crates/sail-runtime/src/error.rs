use sail_common::error::CommonError;
use thiserror::Error;
pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("Worker thread gone, executor was likely shut dow")]
    WorkerGone,
    #[error("Panic: {0}")]
    Panic(String),
}

impl RuntimeError {
    pub fn missing(message: impl Into<String>) -> Self {
        RuntimeError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        RuntimeError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        RuntimeError::NotSupported(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        RuntimeError::InternalError(message.into())
    }
}

impl From<CommonError> for RuntimeError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => RuntimeError::MissingArgument(message),
            CommonError::InvalidArgument(message) => RuntimeError::InvalidArgument(message),
            CommonError::NotSupported(message) => RuntimeError::NotSupported(message),
            CommonError::InternalError(message) => RuntimeError::InternalError(message),
        }
    }
}
