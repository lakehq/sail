use std::fmt;

use datafusion_common::DataFusionError;

/// Errors specific to Python DataSource operations
#[derive(Debug)]
pub enum PythonDataSourceError {
    /// Error from Python execution
    PythonError(String),
    /// Schema validation error
    SchemaError(String),
    /// Version incompatibility
    VersionError(String),
    /// Arrow conversion error
    ArrowError(String),
    /// DataFusion error
    DataFusion(DataFusionError),
}

impl fmt::Display for PythonDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PythonError(msg) => write!(f, "Python error: {}", msg),
            Self::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            Self::VersionError(msg) => write!(f, "Version error: {}", msg),
            Self::ArrowError(msg) => write!(f, "Arrow conversion error: {}", msg),
            Self::DataFusion(e) => write!(f, "DataFusion error: {}", e),
        }
    }
}

impl std::error::Error for PythonDataSourceError {}

impl From<DataFusionError> for PythonDataSourceError {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

impl From<PythonDataSourceError> for DataFusionError {
    fn from(e: PythonDataSourceError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}

#[cfg(feature = "python")]
impl From<pyo3::PyErr> for PythonDataSourceError {
    fn from(e: pyo3::PyErr) -> Self {
        Self::PythonError(e.to_string())
    }
}

/// Convert PyO3 error to DataFusion error.
///
/// This is a shared helper to avoid duplicating this conversion pattern
/// across multiple modules (stream.rs, executor.rs, arrow_utils.rs, etc.).
#[cfg(feature = "python")]
pub fn py_err(e: pyo3::PyErr) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
}
