use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use sail_common_datafusion::error::{CommonErrorCause, RemotePythonError};
use sail_python_udf::error::PyErrExtractor;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

pub type TaskStreamResult<T> = Result<T, TaskStreamError>;

#[derive(Debug, Clone)]
pub enum TaskStreamError {
    Unknown(String),
    External(Arc<dyn std::error::Error + Send + Sync + 'static>),
}

impl fmt::Display for TaskStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TaskStreamError::Unknown(x) => write!(f, "{x}"),
            TaskStreamError::External(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TaskStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TaskStreamError::Unknown(_) => None,
            TaskStreamError::External(e) => Some(e.as_ref()),
        }
    }
}

impl From<Status> for TaskStreamError {
    fn from(status: Status) -> Self {
        if status.code() == Code::Internal {
            let details = status.get_error_details();
            if let Some(info) = details.error_info() {
                if let ("task stream", "sail", Some(cause)) = (
                    info.reason.as_str(),
                    info.domain.as_str(),
                    info.metadata.get("cause"),
                ) {
                    if let Ok(cause) = serde_json::from_str::<CommonErrorCause>(cause) {
                        return cause.into();
                    }
                }
            }
        }
        Self::Unknown(status.message().to_string())
    }
}

impl From<TaskStreamError> for Status {
    fn from(error: TaskStreamError) -> Self {
        let mut metadata = HashMap::new();
        if let Ok(cause) = serde_json::to_string(&CommonErrorCause::from(error)) {
            metadata.insert("cause".into(), cause);
        }

        let mut details = ErrorDetails::new();
        details.set_error_info("task stream", "sail", metadata);

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
        match value {
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
            | CommonErrorCause::DeltaTable(x) => Self::Unknown(x),
            CommonErrorCause::Python(cause) => {
                Self::External(Arc::new(RemotePythonError::from(cause)))
            }
        }
    }
}

impl From<TaskStreamError> for CommonErrorCause {
    fn from(value: TaskStreamError) -> Self {
        match value {
            TaskStreamError::Unknown(x) => CommonErrorCause::Unknown(x),
            TaskStreamError::External(e) => CommonErrorCause::new::<PyErrExtractor>(&e),
        }
    }
}
