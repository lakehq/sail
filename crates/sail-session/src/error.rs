use std::sync::PoisonError;

use datafusion::common::DataFusionError;
use sail_execution::error::ExecutionError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

pub type SessionResult<T> = Result<T, SessionError>;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl SessionError {
    pub fn invalid(message: impl Into<String>) -> Self {
        SessionError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SessionError::InternalError(message.into())
    }
}
impl From<ExecutionError> for SessionError {
    fn from(value: ExecutionError) -> Self {
        match value {
            ExecutionError::DataFusionError(e) => SessionError::DataFusionError(e),
            ExecutionError::InvalidArgument(e) => SessionError::InvalidArgument(e),
            ExecutionError::JsonError(e) => SessionError::InternalError(e.to_string()),
            ExecutionError::IoError(e) => SessionError::IoError(e),
            ExecutionError::TonicTransportError(e) => SessionError::InternalError(e.to_string()),
            ExecutionError::TonicStatusError(e) => SessionError::InternalError(e.to_string()),
            ExecutionError::KubernetesError(e) => SessionError::InternalError(e.to_string()),
            ExecutionError::InternalError(e) => SessionError::InternalError(e),
        }
    }
}

impl<T> From<SendError<T>> for SessionError {
    fn from(error: SendError<T>) -> Self {
        SessionError::InternalError(error.to_string())
    }
}

impl<T> From<PoisonError<T>> for SessionError {
    fn from(error: PoisonError<T>) -> Self {
        SessionError::InternalError(error.to_string())
    }
}
