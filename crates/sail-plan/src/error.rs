use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use sail_common::error::CommonError;
use thiserror::Error;

pub type PlanResult<T> = Result<T, PlanError>;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] ArrowError),
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

impl PlanError {
    pub fn todo(message: impl Into<String>) -> Self {
        PlanError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        PlanError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        PlanError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        PlanError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        PlanError::InternalError(message.into())
    }
}

impl From<CommonError> for PlanError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::DataFusionError(e) => PlanError::DataFusionError(e),
            CommonError::MissingArgument(message) => PlanError::MissingArgument(message),
            CommonError::InvalidArgument(message) => PlanError::InvalidArgument(message),
            CommonError::NotSupported(message) => PlanError::NotSupported(message),
            CommonError::InternalError(message) => PlanError::InternalError(message),
        }
    }
}
