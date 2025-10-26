// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use datafusion_common::DataFusionError;

/// Python data source errors
#[derive(Debug)]
pub enum PythonDataSourceError {
    /// Python import error
    ImportError(String),
    /// Python execution error
    ExecutionError(String),
    /// Arrow conversion error
    ArrowError(String),
    /// Missing required option
    MissingOption(String),
    /// Schema inference error
    SchemaError(String),
    /// Partition planning error
    PartitionError(String),
    /// General error
    General(String),
}

impl fmt::Display for PythonDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PythonDataSourceError::ImportError(msg) => {
                writeln!(f, "Python import error: {}", msg)?;
                write!(
                    f,
                    "Hint: Ensure the Python module is installed and available in PYTHONPATH"
                )
            }
            PythonDataSourceError::ExecutionError(msg) => {
                write!(f, "Python execution error: {}", msg)
            }
            PythonDataSourceError::ArrowError(msg) => {
                writeln!(f, "Arrow conversion error: {}", msg)?;
                write!(
                    f,
                    "Hint: Ensure Python code returns valid PyArrow RecordBatch objects"
                )
            }
            PythonDataSourceError::MissingOption(msg) => {
                write!(f, "Missing required option: {}", msg)
            }
            PythonDataSourceError::SchemaError(msg) => {
                writeln!(f, "Schema error: {}", msg)?;
                write!(
                    f,
                    "Hint: Verify that the schema returned by infer_schema() matches the data"
                )
            }
            PythonDataSourceError::PartitionError(msg) => {
                write!(f, "Partition error: {}", msg)
            }
            PythonDataSourceError::General(msg) => write!(f, "Python datasource error: {}", msg),
        }
    }
}

impl std::error::Error for PythonDataSourceError {}

impl From<PythonDataSourceError> for DataFusionError {
    fn from(err: PythonDataSourceError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

impl From<pyo3::PyErr> for PythonDataSourceError {
    fn from(err: pyo3::PyErr) -> Self {
        PythonDataSourceError::ExecutionError(err.to_string())
    }
}

/// Result type for Python data source operations
pub type Result<T> = std::result::Result<T, PythonDataSourceError>;
