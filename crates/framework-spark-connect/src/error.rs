use std::sync::PoisonError;

use datafusion::common::DataFusionError;
use framework_common::error::CommonError;
use framework_plan::error::PlanError;
use framework_sql::error::SqlError;
use prost::DecodeError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;

pub type SparkResult<T> = Result<T, SparkError>;

#[derive(Debug, Error)]
pub enum SparkError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
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

impl From<CommonError> for SparkError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::DataFusionError(e) => SparkError::DataFusionError(e),
            CommonError::MissingArgument(message) => SparkError::MissingArgument(message),
            CommonError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            CommonError::NotSupported(message) => SparkError::NotSupported(message),
            CommonError::InternalError(message) => SparkError::InternalError(message),
        }
    }
}

impl From<SqlError> for SparkError {
    fn from(error: SqlError) -> Self {
        match error {
            SqlError::DataFusionError(e) => SparkError::DataFusionError(e),
            SqlError::MissingArgument(message) => SparkError::MissingArgument(message),
            SqlError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            SqlError::NotSupported(message) => SparkError::NotSupported(message),
            SqlError::InternalError(message) => SparkError::InternalError(message),
            SqlError::SqlParserError(e) => SparkError::InvalidArgument(e.to_string()),
            SqlError::NotImplemented(message) => SparkError::NotImplemented(message),
        }
    }
}

impl From<PlanError> for SparkError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::DataFusionError(e) => SparkError::DataFusionError(e),
            PlanError::MissingArgument(message) => SparkError::MissingArgument(message),
            PlanError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            PlanError::NotSupported(message) => SparkError::NotSupported(message),
            PlanError::InternalError(message) => SparkError::InternalError(message),
            PlanError::NotImplemented(message) => SparkError::NotImplemented(message),
        }
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

impl From<DecodeError> for SparkError {
    fn from(error: DecodeError) -> Self {
        SparkError::InvalidArgument(error.to_string())
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
            e @ SparkError::MissingArgument(_) | e @ SparkError::InvalidArgument(_) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::IoError(e) => tonic::Status::internal(e.to_string()),
            SparkError::JsonError(e) => tonic::Status::invalid_argument(e.to_string()),
            SparkError::ArrowError(e) => tonic::Status::internal(e.to_string()),
            e @ SparkError::SendError(_) => tonic::Status::cancelled(e.to_string()),
            e @ SparkError::NotImplemented(_) => tonic::Status::unimplemented(e.to_string()),
            e @ SparkError::NotSupported(_) => tonic::Status::internal(e.to_string()),
            e @ SparkError::InternalError(_) => tonic::Status::internal(e.to_string()),
        }
    }
}
