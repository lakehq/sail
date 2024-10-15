use std::sync::PoisonError;

use datafusion::common::DataFusionError;
use thiserror::Error;
use tokio::task::JoinError;

pub type ExecutionResult<T> = Result<T, ExecutionError>;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error in Tonic transport: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error("error in Tonic status: {0}")]
    TonicStatusError(#[from] tonic::Status),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<JoinError> for ExecutionError {
    fn from(error: JoinError) -> Self {
        ExecutionError::InternalError(error.to_string())
    }
}

impl<T> From<PoisonError<T>> for ExecutionError {
    fn from(error: PoisonError<T>) -> Self {
        ExecutionError::InternalError(error.to_string())
    }
}

impl From<ExecutionError> for tonic::Status {
    fn from(e: ExecutionError) -> tonic::Status {
        match e {
            ExecutionError::TonicStatusError(e) => e,
            x => tonic::Status::internal(x.to_string()),
        }
    }
}
