use std::collections::HashSet;

use datafusion::arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, PyErr, PyResult, Python};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A Python error in text form. This could be a remote error from a worker.
#[derive(Debug, Clone, Error)]
#[error("remote Python error: {summary}")]
pub struct RemotePythonError {
    pub summary: String,
    pub traceback: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CommonErrorCause {
    Unknown(String),
    Internal(String),
    NotImplemented(String),
    InvalidArgument(String),
    Io(String),
    Python {
        summary: String,
        traceback: Option<Vec<String>>,
    },
    ArrowCast(String),
    ArrowMemory(String),
    ArrowParse(String),
    ArrowCompute(String),
    ArrowIpc(String),
    ArrowCDataInterface(String),
    ArrowDivideByZero(String),
    ArrowArithmeticOverflow(String),
    ArrowDictionaryKeyOverflow(String),
    ArrowRunEndIndexOverflow(String),
    FormatCsv(String),
    FormatJson(String),
    FormatParquet(String),
    FormatAvro(String),
    Plan(String),
    Schema(String),
    Configuration(String),
    Execution(String),
}

impl CommonErrorCause {
    fn build(
        error: &(dyn std::error::Error + 'static),
        seen: &mut HashSet<*const dyn std::error::Error>,
    ) -> Self {
        let ptr = error as *const _;
        if seen.contains(&ptr) {
            return Self::Unknown("circular error reference".to_string());
        }
        seen.insert(ptr);
        if let Some(e) = error.downcast_ref::<ArrowError>() {
            match e {
                ArrowError::NotYetImplemented(x) => Self::NotImplemented(x.clone()),
                ArrowError::ExternalError(e) => Self::build(e.as_ref(), seen),
                ArrowError::CastError(x) => Self::ArrowCast(x.clone()),
                ArrowError::MemoryError(x) => Self::ArrowMemory(x.clone()),
                ArrowError::ParseError(x) => Self::ArrowParse(x.clone()),
                ArrowError::SchemaError(x) => Self::Schema(x.clone()),
                ArrowError::ComputeError(x) => Self::ArrowCompute(x.clone()),
                ArrowError::DivideByZero => Self::ArrowDivideByZero("divide by zero".to_string()),
                ArrowError::ArithmeticOverflow(x) => Self::ArrowArithmeticOverflow(x.clone()),
                ArrowError::CsvError(x) => Self::FormatCsv(x.clone()),
                ArrowError::JsonError(x) => Self::FormatJson(x.clone()),
                ArrowError::IoError(message, e) => Self::Io(format!("{message}: {e}")),
                ArrowError::IpcError(x) => Self::ArrowIpc(x.clone()),
                ArrowError::InvalidArgumentError(x) => Self::InvalidArgument(x.clone()),
                ArrowError::ParquetError(x) => Self::FormatParquet(x.clone()),
                ArrowError::CDataInterface(x) => Self::ArrowCDataInterface(x.clone()),
                ArrowError::DictionaryKeyOverflowError => {
                    Self::ArrowDictionaryKeyOverflow("dictionary key overflow".to_string())
                }
                ArrowError::RunEndIndexOverflowError => {
                    Self::ArrowRunEndIndexOverflow("run-end index overflow".to_string())
                }
            }
        } else if let Some(e) = error.downcast_ref::<DataFusionError>() {
            match e {
                DataFusionError::ArrowError(e, _) => Self::build(e, seen),
                DataFusionError::ParquetError(e) => Self::FormatParquet(e.to_string()),
                DataFusionError::AvroError(e) => Self::FormatAvro(e.to_string()),
                DataFusionError::ObjectStore(e) => Self::Io(e.to_string()),
                DataFusionError::IoError(e) => Self::Io(e.to_string()),
                DataFusionError::SQL(e, _) => Self::Unknown(e.to_string()),
                DataFusionError::NotImplemented(x) => Self::NotImplemented(x.clone()),
                DataFusionError::Internal(x) => Self::Internal(x.clone()),
                DataFusionError::Plan(x) => Self::Plan(x.clone()),
                DataFusionError::Configuration(x) => Self::Configuration(x.clone()),
                DataFusionError::SchemaError(e, _) => Self::Schema(e.to_string()),
                DataFusionError::Execution(x) => Self::Execution(x.clone()),
                DataFusionError::ExecutionJoin(e) => Self::Execution(e.to_string()),
                DataFusionError::ResourcesExhausted(x) => Self::Execution(x.clone()),
                DataFusionError::External(e) => Self::build(e.as_ref(), seen),
                DataFusionError::Context(_, e) => Self::build(e, seen),
                DataFusionError::Substrait(x) => Self::Unknown(x.clone()),
                DataFusionError::Diagnostic(_, e) => Self::build(e.as_ref(), seen),
                DataFusionError::Collection(errors) => match errors.first() {
                    None => Self::Unknown("empty error collection".to_string()),
                    Some(e) => Self::build(e, seen),
                },
                DataFusionError::Shared(e) => Self::build(e.as_ref(), seen),
            }
        } else if let Some(e) = error.downcast_ref::<PyErr>() {
            let traceback = Python::with_gil(|py| -> PyResult<Vec<String>> {
                let traceback = PyModule::import(py, intern!(py, "traceback"))?;
                let format_exception = traceback.getattr(intern!(py, "format_exception"))?;
                format_exception.call1((e,))?.extract()
            });
            Self::Python {
                summary: e.to_string(),
                traceback: traceback.ok(),
            }
        } else if let Some(e) = error.downcast_ref::<RemotePythonError>() {
            Self::Python {
                summary: e.summary.clone(),
                traceback: e.traceback.clone(),
            }
        } else if let Some(e) = error.source() {
            Self::build(e, seen)
        } else {
            Self::Unknown(error.to_string())
        }
    }

    /// Recursively traverse the error source and determine the best error cause
    /// from the innermost error.
    pub fn new(error: &(dyn std::error::Error + 'static)) -> Self {
        Self::build(error, &mut HashSet::new())
    }
}
