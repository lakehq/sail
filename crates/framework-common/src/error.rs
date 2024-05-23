use datafusion::common::DataFusionError;
use thiserror::Error;

pub type CommonResult<T> = Result<T, CommonError>;

#[derive(Debug, Error)]
pub enum CommonError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
}

impl CommonError {
    pub fn invalid(message: impl Into<String>) -> Self {
        CommonError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        CommonError::NotSupported(message.into())
    }
}
