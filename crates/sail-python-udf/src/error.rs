use datafusion_common::DataFusionError;
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, PyErr, PyResult, Python};
use sail_common_datafusion::error::{PythonErrorCause, PythonErrorCauseExtractor};
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

pub struct PyErrExtractor;

impl PythonErrorCauseExtractor for PyErrExtractor {
    fn extract(error: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        if let Some(e) = error.downcast_ref::<PyErr>() {
            let traceback = Python::attach(|py| -> PyResult<Vec<String>> {
                let traceback = PyModule::import(py, intern!(py, "traceback"))?;
                let format_exception = traceback.getattr(intern!(py, "format_exception"))?;
                format_exception.call1((e,))?.extract()
            });
            Some(PythonErrorCause {
                summary: e.to_string(),
                traceback: traceback.ok(),
            })
        } else {
            None
        }
    }
}
