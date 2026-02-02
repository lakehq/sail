//! Error types for Python DataSource operations.
//!
//! Provides structured error types with context for debugging Python datasource issues.

use datafusion_common::DataFusionError;
use thiserror::Error;

/// Result type alias for Python DataSource operations.
#[allow(dead_code)]
pub type PythonDataSourceResult<T> = Result<T, PythonDataSourceError>;

/// Errors specific to Python DataSource operations.
#[derive(Debug, Error)]
pub enum PythonDataSourceError {
    /// Error from Python execution
    #[error("Python error: {0}")]
    PythonError(String),
    /// Schema validation error
    #[error("Schema error: {0}")]
    SchemaError(String),
    /// Version incompatibility
    #[error("Version error: {0}")]
    VersionError(String),
    /// Arrow conversion error
    #[error("Arrow conversion error: {0}")]
    ArrowError(String),
    /// DataFusion error
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    /// Resource exhaustion (e.g., partition too large)
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
}

impl PythonDataSourceError {
    /// Create a Python error with the given message.
    pub fn python(msg: impl Into<String>) -> Self {
        Self::PythonError(msg.into())
    }

    /// Create a schema error with the given message.
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::SchemaError(msg.into())
    }

    /// Create a version error with the given message.
    pub fn version(msg: impl Into<String>) -> Self {
        Self::VersionError(msg.into())
    }

    /// Create an Arrow conversion error with the given message.
    pub fn arrow(msg: impl Into<String>) -> Self {
        Self::ArrowError(msg.into())
    }

    /// Create a resource exhausted error with the given message.
    pub fn resource_exhausted(msg: impl Into<String>) -> Self {
        Self::ResourceExhausted(msg.into())
    }
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
        PythonDataSourceError::python(format!(
            "[{}::{}] {}",
            self.datasource_name,
            self.operation,
            msg.into()
        ))
    }

    /// Wrap a Python error with context information, preserving traceback.
    pub fn wrap_py_error(&self, e: pyo3::PyErr) -> PythonDataSourceError {
        self.wrap_error(format_py_error_with_traceback(e))
    }
}

impl From<PythonDataSourceError> for DataFusionError {
    fn from(e: PythonDataSourceError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}

/// Format a Python error with its traceback for better debugging.
///
/// This extracts the full Python traceback when available, making it much
/// easier to debug Python datasource errors.
pub fn format_py_error_with_traceback(e: pyo3::PyErr) -> String {
    use pyo3::types::PyTracebackMethods;

    pyo3::Python::attach(|py| {
        let traceback = e
            .traceback(py)
            .and_then(|tb| tb.format().ok())
            .unwrap_or_default();

        if traceback.is_empty() {
            e.to_string()
        } else {
            format!("{}\nTraceback:\n{}", e, traceback)
        }
    })
}

impl From<pyo3::PyErr> for PythonDataSourceError {
    fn from(e: pyo3::PyErr) -> Self {
        Self::python(format_py_error_with_traceback(e))
    }
}

/// Convert PyO3 error to DataFusion error, preserving traceback.
///
/// This is a shared helper to avoid duplicating this conversion pattern
/// across multiple modules (stream.rs, executor.rs, arrow_utils.rs, etc.).
pub fn py_err(e: pyo3::PyErr) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(
        format_py_error_with_traceback(e),
    )))
}

/// Import cloudpickle with a helpful error message if it's not installed.
///
/// cloudpickle is required for serializing Python DataSources between
/// the client and server. This helper provides clear installation instructions
/// if the import fails.
pub fn import_cloudpickle(
    py: pyo3::Python<'_>,
) -> std::result::Result<pyo3::Bound<'_, pyo3::types::PyModule>, DataFusionError> {
    py.import("cloudpickle").map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to import cloudpickle: {}. \
            cloudpickle is required for Python DataSources. \
            Install it with: pip install cloudpickle",
            e
        ))
    })
}
