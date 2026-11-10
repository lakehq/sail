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

/// A mutation may have committed and must not be retried automatically.
#[derive(Debug, Error)]
pub struct CommitStateUnknownError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl CommitStateUnknownError {
    pub fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

impl From<String> for CommitStateUnknownError {
    fn from(message: String) -> Self {
        Self {
            message,
            source: None,
        }
    }
}

impl fmt::Display for CommitStateUnknownError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "commit state unknown: {}", self.message)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

/// A definite rejection invalidated prepared output; retry requires a new plan.
#[derive(Debug, Error)]
#[error("commit requires replanning: {0}")]
pub struct CommitRequiresReplanningError(pub String);

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
    DeltaTable(String),
    CommitStateUnknown(String),
    CommitRequiresReplanning(String),
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
            | CommonErrorCause::DeltaTable(x)
            | CommonErrorCause::CommitStateUnknown(x)
            | CommonErrorCause::CommitRequiresReplanning(x) => write!(f, "{x}"),
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
            Self::CommitStateUnknown(_) | Self::CommitRequiresReplanning(_)
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
                        .find(|cause| matches!(cause, Self::CommitStateUnknown(_)))
                        .or_else(|| causes.iter().find(|cause| !cause.is_retryable()))
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

        if let Some(e) = error.downcast_ref::<CommitStateUnknownError>() {
            return Self::CommitStateUnknown(e.to_string());
        }

        if let Some(e) = error.downcast_ref::<CommitRequiresReplanningError>() {
            return Self::CommitRequiresReplanning(e.to_string());
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
#[expect(clippy::unwrap_used)]
mod tests {
    use std::error::Error;
    use std::io;

    use super::*;

    struct NoPythonError;

    impl PythonErrorCauseExtractor for NoPythonError {
        fn extract(_: &(dyn Error + 'static)) -> Option<PythonErrorCause> {
            None
        }
    }

    #[test]
    fn replanning_cause_survives_wrappers_and_serialization() {
        fn rejection() -> DataFusionError {
            DataFusionError::External(Box::new(CommitRequiresReplanningError(
                "prepared output is stale".into(),
            )))
        }
        for error in [
            rejection(),
            DataFusionError::Context("commit failed".into(), Box::new(rejection())),
            DataFusionError::Shared(std::sync::Arc::new(rejection())),
            DataFusionError::ArrowError(
                Box::new(ArrowError::ExternalError(Box::new(rejection()))),
                None,
            ),
            DataFusionError::Collection(vec![
                DataFusionError::Execution("worker disconnected".into()),
                rejection(),
            ]),
        ] {
            let cause = CommonErrorCause::new::<NoPythonError>(&error);
            assert!(!cause.is_retryable());
            let json = serde_json::to_value(&cause).unwrap();
            assert_eq!(
                json,
                serde_json::json!({
                    "commitRequiresReplanning": "commit requires replanning: prepared output is stale"
                })
            );
            let remote = DataFusionError::External(Box::new(RemoteError {
                cause: serde_json::from_value(json).unwrap(),
            }));
            assert!(matches!(CommonErrorCause::new::<NoPythonError>(&remote),
                CommonErrorCause::CommitRequiresReplanning(message)
                    if message == "commit requires replanning: prepared output is stale"));
        }
        assert!(
            CommonErrorCause::new::<NoPythonError>(&DataFusionError::Execution(
                "worker disconnected".into()
            ))
            .is_retryable()
        );
    }

    #[test]
    fn unknown_commit_takes_precedence_over_definite_rejection() {
        let error = DataFusionError::Collection(vec![
            DataFusionError::External(Box::new(CommitRequiresReplanningError(
                "stale output".into(),
            ))),
            DataFusionError::External(Box::new(CommitStateUnknownError::from(
                "response lost".to_string(),
            ))),
        ]);
        assert!(matches!(
            CommonErrorCause::new::<NoPythonError>(&error),
            CommonErrorCause::CommitStateUnknown(_)
        ));
    }

    #[test]
    fn commit_state_unknown_without_source_preserves_behavior() {
        let error = CommitStateUnknownError::from("catalog commit may have succeeded".to_string());
        let expected = "commit state unknown: catalog commit may have succeeded";
        assert_eq!(error.to_string(), expected);
        assert!(error.source().is_none());
        let cause =
            CommonErrorCause::new::<NoPythonError>(&DataFusionError::External(Box::new(error)));
        assert!(
            matches!(&cause, CommonErrorCause::CommitStateUnknown(message) if message == expected)
        );
        assert!(!cause.is_retryable());
    }

    #[test]
    fn commit_state_unknown_with_source_preserves_typed_cause() {
        let error = CommitStateUnknownError::with_source(
            "commit may have succeeded",
            io::Error::new(io::ErrorKind::PermissionDenied, "verification denied"),
        );
        let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(source.to_string(), "verification denied");
        let expected = "commit state unknown: commit may have succeeded: verification denied";
        assert_eq!(error.to_string(), expected);
        let cause =
            CommonErrorCause::new::<NoPythonError>(&DataFusionError::External(Box::new(error)));
        assert!(
            matches!(&cause, CommonErrorCause::CommitStateUnknown(message) if message == expected)
        );
        assert!(!cause.is_retryable());
    }
}
