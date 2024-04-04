use std::sync::PoisonError;

use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser;
use prost::DecodeError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;

pub(crate) type SparkResult<T> = Result<T, SparkError>;

#[derive(Debug, Error)]
pub enum SparkError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in SQL parser: {0}")]
    SqlParserError(#[from] sqlparser::parser::ParserError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("error in JSON serde: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("error in channel: {0}")]
    SendError(String),
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

impl SparkError {
    pub fn todo(message: impl Into<String>) -> Self {
        SparkError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        SparkError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        SparkError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        SparkError::InvalidArgument(message.into())
    }

    pub fn send<T>(message: impl Into<String>) -> Self {
        SparkError::SendError(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SparkError::InternalError(message.into())
    }
}

impl<T> From<PoisonError<T>> for SparkError {
    fn from(error: PoisonError<T>) -> Self {
        SparkError::InternalError(error.to_string())
    }
}

impl<T> From<SendError<T>> for SparkError {
    fn from(error: SendError<T>) -> Self {
        SparkError::SendError(error.to_string())
    }
}

impl From<JoinError> for SparkError {
    fn from(error: JoinError) -> Self {
        SparkError::InternalError(error.to_string())
    }
}

pub trait ProtoFieldExt<T> {
    fn required(self, description: impl Into<String>) -> SparkResult<T>;
}

impl<T> ProtoFieldExt<T> for Option<T> {
    fn required(self, description: impl Into<String>) -> SparkResult<T> {
        self.ok_or_else(|| SparkError::MissingArgument(description.into()))
    }
}

impl<T> ProtoFieldExt<T> for Result<T, DecodeError> {
    fn required(self, description: impl Into<String>) -> SparkResult<T> {
        self.map_err(|e| SparkError::InvalidArgument(format!("{}: {}", description.into(), e)))
    }
}

impl From<SparkError> for tonic::Status {
    fn from(error: SparkError) -> Self {
        match error {
            SparkError::DataFusionError(e @ DataFusionError::Plan(_))
            | SparkError::DataFusionError(e @ DataFusionError::Configuration(_)) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::DataFusionError(DataFusionError::SQL(e, _)) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::DataFusionError(DataFusionError::SchemaError(e, _)) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::DataFusionError(e @ DataFusionError::NotImplemented(_)) => {
                tonic::Status::unimplemented(e.to_string())
            }
            SparkError::DataFusionError(e) => tonic::Status::internal(e.to_string()),
            SparkError::SqlParserError(e) => tonic::Status::invalid_argument(e.to_string()),
            e @ SparkError::MissingArgument(_) | e @ SparkError::InvalidArgument(_) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::JsonError(e) => tonic::Status::invalid_argument(e.to_string()),
            SparkError::ArrowError(e) => tonic::Status::internal(e.to_string()),
            e @ SparkError::SendError(_) => tonic::Status::cancelled(e.to_string()),
            e @ SparkError::NotImplemented(_) => tonic::Status::unimplemented(e.to_string()),
            e @ SparkError::NotSupported(_) => tonic::Status::internal(e.to_string()),
            e @ SparkError::InternalError(_) => tonic::Status::internal(e.to_string()),
        }
    }
}
