use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use datafusion::common::DataFusionError;
use sail_common_datafusion::error::{CommonErrorCause, RemoteError};
use sail_python_udf::error::PyErrExtractor;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

pub type TaskStreamResult<T> = Result<T, TaskStreamError>;

#[derive(Debug, Clone)]
pub enum TaskStreamError {
    /// An attempt ended without declaring success or reporting a producer error.
    ProducerDropped,
    /// The producer aborted the stream and will report its task failure.
    ProducerFailed(RemoteError),
    Unknown(String),
    External(Arc<dyn std::error::Error + Send + Sync + 'static>),
}

impl TaskStreamError {
    pub fn from_execution_error(error: &DataFusionError) -> Self {
        let cause = CommonErrorCause::new::<PyErrExtractor>(error);
        // DataFusion's source() only exposes the first member of a Collection.
        // A canceled sibling must not hide a deterministic or uncertain-commit failure.
        if !cause.is_retryable() {
            return cause.into();
        }
        let mut source: &(dyn std::error::Error + 'static) = error;
        loop {
            if matches!(source.downcast_ref::<Self>(), Some(Self::ProducerDropped)) {
                return Self::ProducerDropped;
            }
            match source.source() {
                Some(inner) => source = inner,
                None => break,
            }
        }
        cause.into()
    }

    pub fn into_producer_failure(self) -> Self {
        match self {
            Self::ProducerDropped | Self::ProducerFailed(_) => self,
            error => Self::ProducerFailed(RemoteError {
                cause: error.into(),
            }),
        }
    }

    pub fn is_retryable_producer_failure(&self) -> bool {
        if !CommonErrorCause::from(self.clone()).is_retryable() {
            return false;
        }
        let mut source: &(dyn std::error::Error + 'static) = self;
        loop {
            if matches!(
                source.downcast_ref::<Self>(),
                Some(Self::ProducerDropped | Self::ProducerFailed(_))
            ) {
                return true;
            }
            match source.source() {
                Some(inner) => source = inner,
                None => return false,
            }
        }
    }
}

impl fmt::Display for TaskStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TaskStreamError::ProducerDropped => {
                write!(
                    f,
                    "task stream producer dropped without completing the stream"
                )
            }
            TaskStreamError::ProducerFailed(e) => write!(f, "{e}"),
            TaskStreamError::Unknown(x) => write!(f, "{x}"),
            TaskStreamError::External(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TaskStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TaskStreamError::ProducerDropped | TaskStreamError::Unknown(_) => None,
            TaskStreamError::ProducerFailed(e) => Some(e),
            TaskStreamError::External(e) => Some(e.as_ref()),
        }
    }
}

impl From<Status> for TaskStreamError {
    fn from(status: Status) -> Self {
        if status.code() == Code::Internal {
            let details = status.get_error_details();
            if let Some(info) = details.error_info()
                && info.reason == "task stream producer dropped"
                && info.domain == "sail"
            {
                return Self::ProducerDropped;
            }
            if let Some(info) = details.error_info()
                && let ("task stream" | "task stream producer failed", "sail", Some(cause)) = (
                    info.reason.as_str(),
                    info.domain.as_str(),
                    info.metadata.get("cause"),
                )
                && let Ok(cause) = serde_json::from_str::<CommonErrorCause>(cause)
            {
                return if info.reason == "task stream producer failed" {
                    Self::ProducerFailed(RemoteError { cause })
                } else {
                    cause.into()
                };
            }
        }
        Self::Unknown(status.message().to_string())
    }
}

impl From<TaskStreamError> for Status {
    fn from(error: TaskStreamError) -> Self {
        if matches!(error, TaskStreamError::ProducerDropped) {
            let mut details = ErrorDetails::new();
            details.set_error_info("task stream producer dropped", "sail", HashMap::new());
            return Status::with_error_details(Code::Internal, error.to_string(), details);
        }
        let reason = if matches!(error, TaskStreamError::ProducerFailed(_)) {
            "task stream producer failed"
        } else {
            "task stream"
        };
        let mut metadata = HashMap::new();
        if let Ok(cause) = serde_json::to_string(&CommonErrorCause::from(error)) {
            metadata.insert("cause".into(), cause);
        }

        let mut details = ErrorDetails::new();
        details.set_error_info(reason, "sail", metadata);

        Status::with_error_details(Code::Internal, "task stream error", details)
    }
}

impl From<FlightError> for TaskStreamError {
    fn from(value: FlightError) -> Self {
        match value {
            FlightError::Arrow(e) => Self::External(Arc::new(e)),
            FlightError::Tonic(status) => (*status).into(),
            FlightError::NotYetImplemented(x)
            | FlightError::ProtocolError(x)
            | FlightError::DecodeError(x) => Self::Unknown(x),
            FlightError::ExternalError(e) => Self::External(Arc::from(e)),
        }
    }
}

impl From<CommonErrorCause> for TaskStreamError {
    fn from(value: CommonErrorCause) -> Self {
        Self::External(Arc::new(RemoteError { cause: value }))
    }
}

impl From<TaskStreamError> for CommonErrorCause {
    fn from(value: TaskStreamError) -> Self {
        match value {
            TaskStreamError::ProducerDropped => CommonErrorCause::Unknown(value.to_string()),
            TaskStreamError::ProducerFailed(e) => e.cause,
            TaskStreamError::Unknown(x) => CommonErrorCause::Unknown(x),
            TaskStreamError::External(e) => CommonErrorCause::new::<PyErrExtractor>(e.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_collection_cause_precedes_producer_drop() {
        for cause in [
            CommonErrorCause::NonRetryableExecution("deterministic failure".into()),
            CommonErrorCause::CommitStateUnknown("publication unacknowledged".into()),
        ] {
            for dropped_first in [false, true] {
                let mut errors = vec![
                    DataFusionError::External(Box::new(TaskStreamError::ProducerDropped)),
                    DataFusionError::External(Box::new(RemoteError {
                        cause: cause.clone(),
                    })),
                ];
                if !dropped_first {
                    errors.reverse();
                }
                let error = DataFusionError::Context(
                    "shuffle input".into(),
                    Box::new(DataFusionError::Shared(Arc::new(
                        DataFusionError::Collection(errors),
                    ))),
                );
                let error = TaskStreamError::from_execution_error(&error);
                assert!(!matches!(error, TaskStreamError::ProducerDropped));
                let actual = CommonErrorCause::from(TaskStreamError::from(Status::from(error)));
                assert!(!actual.is_retryable());
                assert!(matches!((&actual, &cause),
                    (CommonErrorCause::NonRetryableExecution(a), CommonErrorCause::NonRetryableExecution(b))
                    | (CommonErrorCause::CommitStateUnknown(a), CommonErrorCause::CommitStateUnknown(b)) if a == b));
            }
        }
    }

    #[test]
    fn only_retryable_producer_terminals_can_wait_for_control_plane() {
        for cause in [
            CommonErrorCause::Io("connection reset".into()),
            CommonErrorCause::NonRetryableExecution("deterministic failure".into()),
            CommonErrorCause::CommitStateUnknown("publication unacknowledged".into()),
        ] {
            for producer in [false, true] {
                let error = TaskStreamError::from(cause.clone());
                let error = if producer {
                    error.into_producer_failure()
                } else {
                    error
                };
                let error = TaskStreamError::from(Status::from(error));
                assert_eq!(
                    error.is_retryable_producer_failure(),
                    producer && cause.is_retryable()
                );
                let wrapped = TaskStreamError::External(Arc::new(DataFusionError::Context(
                    "merged stream".into(),
                    Box::new(DataFusionError::External(Box::new(error))),
                )));
                assert_eq!(
                    wrapped.is_retryable_producer_failure(),
                    producer && cause.is_retryable()
                );
            }
        }
        for error in [
            TaskStreamError::from(Status::unavailable("connect failed")),
            TaskStreamError::from(FlightError::DecodeError("bad IPC".into())),
            TaskStreamError::from(FlightError::ProtocolError("missing schema".into())),
        ] {
            assert!(!error.is_retryable_producer_failure());
        }
    }

    #[test]
    fn incomplete_attempt_survives_execution_and_flight_wrapping() {
        let error = DataFusionError::Context(
            "shuffle input".into(),
            Box::new(DataFusionError::External(Box::new(
                TaskStreamError::ProducerDropped,
            ))),
        );
        let status = Status::from(TaskStreamError::from_execution_error(&error));
        assert!(matches!(
            TaskStreamError::from(status),
            TaskStreamError::ProducerDropped
        ));
    }
}
