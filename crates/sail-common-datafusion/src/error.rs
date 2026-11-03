use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

use datafusion::arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An error with a cause received from a remote worker.
#[derive(Debug, Clone, Error)]
#[error("remote error: {cause}")]
pub struct RemoteError {
    pub cause: CommonErrorCause,
}

/// A deterministic execution failure that cannot be resolved by retrying the task.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct NonRetryableExecutionError(pub String);

/// A mutation may have committed and must not be retried automatically.
#[derive(Debug, Error)]
#[error("commit state unknown: {0}")]
pub struct CommitStateUnknownError(pub String);

impl From<String> for CommitStateUnknownError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonErrorCause {
    pub summary: String,
    pub traceback: Option<Vec<String>>,
}

/// A trait to extract Python error cause from a generic error.
///
/// The implementation should inspect the provided error without
/// recursively inspecting its inner errors.
///
/// This trait avoids direct dependency on `PyO3` for this crate.
/// Since this crate is used by many other crates, we can therefore
/// avoid unnecessary recompilation when switching Python environments.
pub trait PythonErrorCauseExtractor {
    fn extract(error: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CommonErrorCause {
    Unknown(String),
    Internal(String),
    NotImplemented(String),
    InvalidArgument(String),
    Io(String),
    Python(PythonErrorCause),
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
    ArrowOffsetOverflow(String),
    FormatCsv(String),
    FormatJson(String),
    FormatParquet(String),
    FormatAvro(String),
    Plan(String),
    Schema(String),
    Configuration(String),
    Execution(String),
    NonRetryableExecution(String),
    DeltaTable(String),
    CommitStateUnknown(String),
}

impl fmt::Display for CommonErrorCause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CommonErrorCause::Unknown(x)
            | CommonErrorCause::Internal(x)
            | CommonErrorCause::NotImplemented(x)
            | CommonErrorCause::InvalidArgument(x)
            | CommonErrorCause::Io(x)
            | CommonErrorCause::ArrowCast(x)
            | CommonErrorCause::ArrowMemory(x)
            | CommonErrorCause::ArrowParse(x)
            | CommonErrorCause::ArrowCompute(x)
            | CommonErrorCause::ArrowIpc(x)
            | CommonErrorCause::ArrowCDataInterface(x)
            | CommonErrorCause::ArrowDivideByZero(x)
            | CommonErrorCause::ArrowArithmeticOverflow(x)
            | CommonErrorCause::ArrowDictionaryKeyOverflow(x)
            | CommonErrorCause::ArrowRunEndIndexOverflow(x)
            | CommonErrorCause::ArrowOffsetOverflow(x)
            | CommonErrorCause::FormatCsv(x)
            | CommonErrorCause::FormatJson(x)
            | CommonErrorCause::FormatParquet(x)
            | CommonErrorCause::FormatAvro(x)
            | CommonErrorCause::Plan(x)
            | CommonErrorCause::Schema(x)
            | CommonErrorCause::Configuration(x)
            | CommonErrorCause::Execution(x)
            | CommonErrorCause::NonRetryableExecution(x)
            | CommonErrorCause::DeltaTable(x)
            | CommonErrorCause::CommitStateUnknown(x) => write!(f, "{x}"),
            CommonErrorCause::Python(PythonErrorCause { summary, .. }) => {
                write!(f, "{summary}")
            }
        }
    }
}

impl CommonErrorCause {
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self,
            Self::CommitStateUnknown(_) | Self::NonRetryableExecution(_)
        )
    }

    fn build<Py: PythonErrorCauseExtractor>(
        error: &(dyn std::error::Error + 'static),
        seen: &mut HashSet<*const dyn std::error::Error>,
    ) -> Self {
        let ptr = error as *const _;
        if seen.contains(&ptr) {
            return Self::Unknown("circular error reference".to_string());
        }
        seen.insert(ptr);
        if let Some(e) = error.downcast_ref::<ArrowError>() {
            return match e {
                ArrowError::NotYetImplemented(x) => Self::NotImplemented(x.clone()),
                ArrowError::ExternalError(e) => Self::build::<Py>(e.as_ref(), seen),
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
                ArrowError::AvroError(x) => Self::FormatAvro(x.clone()),
                ArrowError::CDataInterface(x) => Self::ArrowCDataInterface(x.clone()),
                ArrowError::DictionaryKeyOverflowError => {
                    Self::ArrowDictionaryKeyOverflow("dictionary key overflow".to_string())
                }
                ArrowError::RunEndIndexOverflowError => {
                    Self::ArrowRunEndIndexOverflow("run-end index overflow".to_string())
                }
                ArrowError::OffsetOverflowError(x) => {
                    Self::ArrowOffsetOverflow(format!("offset overflow: {x}"))
                }
            };
        }

        if let Some(e) = error.downcast_ref::<DataFusionError>() {
            return match e {
                DataFusionError::ArrowError(e, _) => Self::build::<Py>(e, seen),
                DataFusionError::ParquetError(e) => Self::FormatParquet(e.to_string()),
                DataFusionError::ObjectStore(e) => Self::Io(e.to_string()),
                DataFusionError::IoError(e) => Self::Io(e.to_string()),
                DataFusionError::NotImplemented(x) => Self::NotImplemented(x.clone()),
                DataFusionError::Internal(x) => Self::Internal(x.clone()),
                DataFusionError::Plan(x) => Self::Plan(x.clone()),
                DataFusionError::Configuration(x) => Self::Configuration(x.clone()),
                DataFusionError::SchemaError(e, _) => Self::Schema(e.to_string()),
                DataFusionError::Execution(x) => Self::Execution(x.clone()),
                DataFusionError::ExecutionJoin(e) => Self::Execution(e.to_string()),
                DataFusionError::ResourcesExhausted(x) => Self::Execution(x.clone()),
                DataFusionError::External(e) => Self::build::<Py>(e.as_ref(), seen),
                DataFusionError::Context(_, e) => Self::build::<Py>(e, seen),
                DataFusionError::Substrait(x) => Self::Unknown(x.clone()),
                DataFusionError::Diagnostic(_, e) => Self::build::<Py>(e.as_ref(), seen),
                DataFusionError::Collection(errors) => {
                    let causes = errors
                        .iter()
                        .map(|e| Self::build::<Py>(e, seen))
                        .collect::<Vec<_>>();
                    causes
                        .iter()
                        .find(|cause| !cause.is_retryable())
                        .or_else(|| causes.first())
                        .cloned()
                        .unwrap_or_else(|| Self::Unknown("empty error collection".to_string()))
                }
                DataFusionError::Shared(e) => Self::build::<Py>(e.as_ref(), seen),
                DataFusionError::Ffi(x) => Self::Unknown(x.clone()),
            };
        }

        if let Some(cause) = Py::extract(error) {
            return Self::Python(cause);
        }

        if let Some(e) = error.downcast_ref::<RemoteError>() {
            return e.cause.clone();
        }

        if let Some(e) = error.downcast_ref::<NonRetryableExecutionError>() {
            return Self::NonRetryableExecution(e.to_string());
        }

        if let Some(e) = error.downcast_ref::<CommitStateUnknownError>() {
            return Self::CommitStateUnknown(e.to_string());
        }

        if let Some(e) = error.source() {
            Self::build::<Py>(e, seen)
        } else {
            Self::Unknown(error.to_string())
        }
    }

    /// Recursively traverse the error source and determine the best error cause
    /// from the innermost error.
    pub fn new<Py: PythonErrorCauseExtractor>(error: &(dyn std::error::Error + 'static)) -> Self {
        Self::build::<Py>(error, &mut HashSet::new())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct NoPython;

    impl PythonErrorCauseExtractor for NoPython {
        fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
            None
        }
    }

    #[test]
    fn nonretryable_execution_survives_wrappers_and_remote_roundtrip()
    -> Result<(), Box<dyn std::error::Error>> {
        let message = "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row";
        let error = DataFusionError::Collection(vec![
            DataFusionError::Execution("transient failure".into()),
            DataFusionError::Context(
                "projection".into(),
                Box::new(DataFusionError::Shared(Arc::new(
                    DataFusionError::ArrowError(
                        Box::new(ArrowError::ExternalError(Box::new(
                            NonRetryableExecutionError(message.into()),
                        ))),
                        None,
                    ),
                ))),
            ),
        ]);
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(!cause.is_retryable());
        assert!(matches!(&cause, CommonErrorCause::NonRetryableExecution(x) if x == message));
        let serialized = serde_json::to_string(&cause)?;
        assert_eq!(
            serialized,
            format!(r#"{{"nonRetryableExecution":"{message}"}}"#)
        );
        let remote = RemoteError {
            cause: serde_json::from_str(&serialized)?,
        };
        let cause = CommonErrorCause::new::<NoPython>(&remote);
        assert!(!cause.is_retryable());
        assert_eq!(cause.to_string(), message);
        Ok(())
    }

    #[test]
    fn legacy_execution_and_transient_io_remain_retryable() -> Result<(), Box<dyn std::error::Error>>
    {
        let legacy = r#"{"execution":"worker heartbeat timeout"}"#;
        let cause: CommonErrorCause = serde_json::from_str(legacy)?;
        assert!(cause.is_retryable());
        assert_eq!(serde_json::to_string(&cause)?, legacy);
        // An untyped execution error must not become terminal by message matching.
        let error = DataFusionError::Execution(
            "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row".into(),
        );
        assert!(CommonErrorCause::new::<NoPython>(&error).is_retryable());
        let error = DataFusionError::Context(
            "reading partition".into(),
            Box::new(DataFusionError::Shared(Arc::new(DataFusionError::IoError(
                std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
            )))),
        );
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(matches!(cause, CommonErrorCause::Io(_)));
        assert!(cause.is_retryable());
        Ok(())
    }
}
