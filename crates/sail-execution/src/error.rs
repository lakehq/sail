use std::sync::PoisonError;

use datafusion::common::DataFusionError;
use prost::{DecodeError, EncodeError, UnknownEnumValue};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;

pub type ExecutionResult<T> = Result<T, ExecutionError>;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error in Tonic transport: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error("error in Tonic status: {0}")]
    TonicStatusError(#[from] tonic::Status),
    #[error("error in Kubernetes: {0}")]
    KubernetesError(#[from] kube::Error),
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

impl<T> From<mpsc::error::SendError<T>> for ExecutionError {
    fn from(error: mpsc::error::SendError<T>) -> Self {
        ExecutionError::InternalError(error.to_string())
    }
}

impl From<oneshot::error::RecvError> for ExecutionError {
    fn from(error: oneshot::error::RecvError) -> Self {
        ExecutionError::InternalError(error.to_string())
    }
}

impl From<EncodeError> for ExecutionError {
    fn from(error: EncodeError) -> Self {
        ExecutionError::InvalidArgument(error.to_string())
    }
}

impl From<DecodeError> for ExecutionError {
    fn from(error: DecodeError) -> Self {
        ExecutionError::InvalidArgument(error.to_string())
    }
}

impl From<UnknownEnumValue> for ExecutionError {
    fn from(error: UnknownEnumValue) -> Self {
        ExecutionError::InvalidArgument(error.to_string())
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
