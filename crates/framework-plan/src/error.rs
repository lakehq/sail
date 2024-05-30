use datafusion::common::DataFusionError;
use framework_common::error::CommonError;
use thiserror::Error;

pub type PlannerResult<T> = Result<T, PlannerError>;

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
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

impl PlannerError {
    pub fn todo(message: impl Into<String>) -> Self {
        PlannerError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        PlannerError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        PlannerError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        PlannerError::InvalidArgument(message.into())
    }
}

impl From<CommonError> for PlannerError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::DataFusionError(e) => PlannerError::DataFusionError(e),
            CommonError::MissingArgument(message) => PlannerError::MissingArgument(message),
            CommonError::InvalidArgument(message) => PlannerError::InvalidArgument(message),
            CommonError::NotSupported(message) => PlannerError::NotSupported(message),
            CommonError::InternalError(message) => PlannerError::InternalError(message),
        }
    }
}
