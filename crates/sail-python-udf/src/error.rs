use datafusion_common::DataFusionError;
use pyo3::PyErr;
use thiserror::Error;

pub type PyUdfResult<T> = Result<T, PyUdfError>;

#[derive(Debug, Error)]
pub enum PyUdfError {
    #[error("error in Python: {0}")]
    PythonError(#[from] PyErr),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl PyUdfError {
    pub fn invalid(message: impl Into<String>) -> Self {
        PyUdfError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        PyUdfError::InternalError(message.into())
    }
}

impl From<PyUdfError> for DataFusionError {
    fn from(error: PyUdfError) -> Self {
        match error {
            PyUdfError::PythonError(e) => DataFusionError::External(e.into()),
            PyUdfError::IoError(e) => DataFusionError::External(e.into()),
            PyUdfError::InvalidArgument(message) => DataFusionError::Plan(message),
            PyUdfError::InternalError(message) => DataFusionError::Internal(message),
        }
    }
}
