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
    /// Resource exhaustion (e.g., partition too large)
    ResourceExhausted(String),
}

/// Context for Python datasource operations, used for enhanced error reporting.
///
/// This struct captures the datasource name and current operation to provide
/// better error messages when Python operations fail.
#[derive(Debug, Clone)]
pub struct PythonDataSourceContext {
    /// Name of the datasource being operated on
    pub datasource_name: String,
    /// Current operation (e.g., "schema", "partitions", "read")
    pub operation: &'static str,
}

impl PythonDataSourceContext {
    /// Create a new context for error reporting.
    pub fn new(datasource_name: impl Into<String>, operation: &'static str) -> Self {
        Self {
            datasource_name: datasource_name.into(),
            operation,
        }
    }

    /// Wrap an error message with context information.
    pub fn wrap_error(&self, msg: impl Into<String>) -> PythonDataSourceError {
        PythonDataSourceError::PythonError(format!(
            "[{}::{}] {}",
            self.datasource_name,
            self.operation,
            msg.into()
        ))
    }

    /// Wrap a Python error with context information.
    #[cfg(feature = "python")]
    pub fn wrap_py_error(&self, e: pyo3::PyErr) -> PythonDataSourceError {
        self.wrap_error(e.to_string())
    }
}

impl fmt::Display for PythonDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PythonError(msg) => write!(f, "Python error: {}", msg),
            Self::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            Self::VersionError(msg) => write!(f, "Version error: {}", msg),
            Self::ArrowError(msg) => write!(f, "Arrow conversion error: {}", msg),
            Self::DataFusion(e) => write!(f, "DataFusion error: {}", e),
            Self::ResourceExhausted(msg) => write!(f, "Resource exhausted: {}", msg),
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
