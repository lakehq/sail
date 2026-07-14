use std::collections::HashMap;
use std::num::ParseIntError;
use std::str::ParseBoolError;
use std::sync::PoisonError;

use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use prost::{DecodeError, UnknownEnumValue};
use sail_cache::error::CacheError;
use sail_common::error::CommonError;
use sail_common_datafusion::error::{CommonErrorCause, PythonErrorCause};
use sail_execution::error::ExecutionError;
use sail_plan::error::PlanError;
use sail_python_udf::error::PyErrExtractor;
use sail_session::error::SessionError;
use sail_sql_analyzer::error::SqlError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

pub type SparkResult<T> = Result<T, SparkError>;

#[derive(Debug, Error)]
pub enum SparkError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] ArrowError),
    #[error("error in JSON serde: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("error in channel: {0}")]
    SendError(String),
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("analysis error: {0}")]
    AnalysisError(String),
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),
}

impl SparkError {
    pub fn todo(message: impl Into<String>) -> Self {
        SparkError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        SparkError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        SparkError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        SparkError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SparkError::InternalError(message.into())
    }

    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        SparkError::DeadlineExceeded(message.into())
    }
}

impl From<CommonError> for SparkError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => SparkError::MissingArgument(message),
            CommonError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            CommonError::NotSupported(message) => SparkError::NotSupported(message),
            CommonError::InternalError(message) => SparkError::InternalError(message),
        }
    }
}

impl From<SqlError> for SparkError {
    fn from(error: SqlError) -> Self {
        match error {
            SqlError::MissingArgument(message) => SparkError::MissingArgument(message),
            SqlError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            SqlError::NotSupported(message) => SparkError::NotSupported(message),
            SqlError::InternalError(message) => SparkError::InternalError(message),
            SqlError::SqlParserError(e) => SparkError::InvalidArgument(e.to_string()),
            SqlError::NotImplemented(message) => SparkError::NotImplemented(message),
            SqlError::AnalysisError(message) => SparkError::AnalysisError(message),
        }
    }
}

impl From<PlanError> for SparkError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::DataFusionError(e) => SparkError::DataFusionError(e),
            PlanError::ArrowError(e) => SparkError::ArrowError(e),
            PlanError::MissingArgument(message) => SparkError::MissingArgument(message),
            PlanError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            PlanError::NotSupported(message) => SparkError::NotSupported(message),
            PlanError::InternalError(message) => SparkError::InternalError(message),
            PlanError::NotImplemented(message) => SparkError::NotImplemented(message),
            PlanError::AnalysisError(message) => SparkError::AnalysisError(message),
            PlanError::DeltaTableError(e) => SparkError::InternalError(e.to_string()),
        }
    }
}

impl From<ExecutionError> for SparkError {
    fn from(value: ExecutionError) -> Self {
        match value {
            ExecutionError::DataFusionError(e) => SparkError::DataFusionError(e),
            ExecutionError::InvalidArgument(e) => SparkError::InvalidArgument(e),
            ExecutionError::JsonError(e) => SparkError::JsonError(e),
            ExecutionError::IoError(e) => SparkError::IoError(e),
            ExecutionError::TonicTransportError(e) => SparkError::InternalError(e.to_string()),
            ExecutionError::TonicStatusError(e) => SparkError::InternalError(e.to_string()),
            ExecutionError::KubernetesError(e) => SparkError::InternalError(e.to_string()),
            ExecutionError::InternalError(e) => SparkError::InternalError(e),
        }
    }
}

impl From<CacheError> for SparkError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::MissingArgument(message) => SparkError::MissingArgument(message),
            CacheError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            CacheError::NotSupported(message) => SparkError::NotSupported(message),
            CacheError::InternalError(message) => SparkError::InternalError(message),
        }
    }
}

impl From<SessionError> for SparkError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::DataFusionError(e) => SparkError::DataFusionError(e),
            SessionError::IoError(e) => SparkError::IoError(e),
            SessionError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            SessionError::InternalError(message) => SparkError::InternalError(message),
        }
    }
}

impl<T> From<PoisonError<T>> for SparkError {
    fn from(error: PoisonError<T>) -> Self {
        SparkError::InternalError(error.to_string())
    }
}

impl<T> From<SendError<T>> for SparkError {
    fn from(error: SendError<T>) -> Self {
        SparkError::SendError(error.to_string())
    }
}

impl From<JoinError> for SparkError {
    fn from(error: JoinError) -> Self {
        SparkError::InternalError(error.to_string())
    }
}

impl From<ParseBoolError> for SparkError {
    fn from(error: ParseBoolError) -> Self {
        SparkError::InvalidArgument(error.to_string())
    }
}

impl From<ParseIntError> for SparkError {
    fn from(error: ParseIntError) -> Self {
        SparkError::InvalidArgument(error.to_string())
    }
}

impl From<DecodeError> for SparkError {
    fn from(error: DecodeError) -> Self {
        SparkError::InvalidArgument(error.to_string())
    }
}

impl From<UnknownEnumValue> for SparkError {
    fn from(error: UnknownEnumValue) -> Self {
        SparkError::InvalidArgument(error.to_string())
    }
}

pub trait ProtoFieldExt<T> {
    fn required(self, description: impl Into<String>) -> SparkResult<T>;
}

impl<T> ProtoFieldExt<T> for Option<T> {
    fn required(self, description: impl Into<String>) -> SparkResult<T> {
        self.ok_or_else(|| SparkError::MissingArgument(description.into()))
    }
}

impl<T, E> ProtoFieldExt<T> for Result<T, E>
where
    E: std::fmt::Display,
{
    fn required(self, description: impl Into<String>) -> SparkResult<T> {
        self.map_err(|e| SparkError::InvalidArgument(format!("{}: {}", description.into(), e)))
    }
}

#[derive(Debug, Clone)]
#[expect(clippy::enum_variant_names)]
pub(crate) enum SparkThrowable {
    ParseException(String),
    AnalysisException(String),
    #[expect(dead_code)]
    StreamingQueryException(String),
    QueryExecutionException(String),
    NumberFormatException(String),
    IllegalArgumentException(String),
    ArithmeticException(String),
    UnsupportedOperationException(String),
    ArrayIndexOutOfBoundsException(String),
    DateTimeException(String),
    SparkRuntimeException(String),
    #[expect(dead_code)]
    SparkUpgradeException(String),
    SparkException(String),
    PythonException(String),
}

impl SparkThrowable {
    pub fn message(&self) -> &str {
        match self {
            SparkThrowable::ParseException(message)
            | SparkThrowable::AnalysisException(message)
            | SparkThrowable::StreamingQueryException(message)
            | SparkThrowable::QueryExecutionException(message)
            | SparkThrowable::NumberFormatException(message)
            | SparkThrowable::IllegalArgumentException(message)
            | SparkThrowable::ArithmeticException(message)
            | SparkThrowable::UnsupportedOperationException(message)
            | SparkThrowable::ArrayIndexOutOfBoundsException(message)
            | SparkThrowable::DateTimeException(message)
            | SparkThrowable::SparkRuntimeException(message)
            | SparkThrowable::SparkUpgradeException(message)
            | SparkThrowable::SparkException(message)
            | SparkThrowable::PythonException(message) => message,
        }
    }

    pub fn class_name(&self) -> &'static str {
        match self {
            SparkThrowable::ParseException(_) => {
                "org.apache.spark.sql.catalyst.parser.ParseException"
            }
            SparkThrowable::AnalysisException(_) => "org.apache.spark.sql.AnalysisException",
            SparkThrowable::StreamingQueryException(_) => {
                "org.apache.spark.sql.streaming.StreamingQueryException"
            }
            SparkThrowable::QueryExecutionException(_) => {
                "org.apache.spark.sql.execution.QueryExecutionException"
            }
            SparkThrowable::NumberFormatException(_) => "java.lang.NumberFormatException",
            SparkThrowable::IllegalArgumentException(_) => "java.lang.IllegalArgumentException",
            SparkThrowable::ArithmeticException(_) => "java.lang.ArithmeticException",
            SparkThrowable::UnsupportedOperationException(_) => {
                "java.lang.UnsupportedOperationException"
            }
            SparkThrowable::ArrayIndexOutOfBoundsException(_) => {
                "java.lang.ArrayIndexOutOfBoundsException"
            }
            SparkThrowable::DateTimeException(_) => "java.time.DateTimeException",
            SparkThrowable::SparkRuntimeException(_) => "org.apache.spark.SparkRuntimeException",
            SparkThrowable::SparkUpgradeException(_) => "org.apache.spark.SparkUpgradeException",
            SparkThrowable::SparkException(_) => "org.apache.spark.SparkException",
            SparkThrowable::PythonException(_) => "org.apache.spark.api.python.PythonException",
        }
    }
}

/// The maximum length of a gRPC status message in bytes.
///
/// gRPC has a hard limit of 16384 bytes for the total metadata size.
/// When error details are present, the message is encoded in two headers:
/// - `grpc-message`: percent-encoded (worst case 3x expansion for non-ASCII bytes)
/// - `grpc-status-details-bin`: base64-encoded protobuf (~4/3x expansion)
///
/// Combined worst-case encoding overhead is approximately 4.33x the raw message length
/// (3x + 4/3x). A cap of 2500 bytes leaves room for `ErrorInfo` details and other
/// gRPC headers in addition to the duplicated status message.
const MAX_GRPC_STATUS_MESSAGE_LEN: usize = 2500;

const TRUNCATED_MARKER: &str = "\n[truncated]\n";

/// Truncate a gRPC status message to at most [`MAX_GRPC_STATUS_MESSAGE_LEN`] bytes.
///
/// Truncation respects UTF-8 character boundaries and keeps both the start and the end
/// of the message. Python tracebacks often include the Spark error class in the final
/// exception line, so prefix-only truncation can hide the user-facing error.
fn truncate_grpc_message(message: &str) -> String {
    if message.len() <= MAX_GRPC_STATUS_MESSAGE_LEN {
        return message.to_string();
    }

    let keep = MAX_GRPC_STATUS_MESSAGE_LEN.saturating_sub(TRUNCATED_MARKER.len());
    let head_take = keep / 3;
    let tail_take = keep - head_take;

    let head_end = message.floor_char_boundary(head_take);
    let tail_start = message.ceil_char_boundary(message.len().saturating_sub(tail_take));

    format!(
        "{}{TRUNCATED_MARKER}{}",
        &message[..head_end],
        &message[tail_start..]
    )
}

impl From<SparkThrowable> for Status {
    fn from(throwable: SparkThrowable) -> Status {
        let class = throwable.class_name();

        let mut metadata = HashMap::new();
        // We do not add the "stackTrace" field since the Java stack trace is not available.
        metadata.insert("classes".into(), format!("[\"{class}\"]"));

        let mut details = ErrorDetails::new();
        details.set_error_info(class, "org.apache.spark", metadata);

        // Truncate the message if it exceeds the maximum length.
        // gRPC has a hard limit of 16384 bytes for total metadata size.
        // Both `grpc-message` and `grpc-status-details-bin` encode the message,
        // so long messages (e.g., Python tracebacks) can easily exceed the limit.
        let message = truncate_grpc_message(throwable.message());

        // The original Spark Connect server implementation uses the "INTERNAL" status code
        // for all Spark exceptions, so we do the same here.
        // Reference: org.apache.spark.sql.connect.utils.ErrorUtils#buildStatusFromThrowable
        Status::with_error_details(Code::Internal, message, details)
    }
}

impl From<CommonErrorCause> for SparkThrowable {
    fn from(value: CommonErrorCause) -> Self {
        match value {
            CommonErrorCause::NotImplemented(x) => SparkThrowable::UnsupportedOperationException(x),
            CommonErrorCause::InvalidArgument(x) => SparkThrowable::IllegalArgumentException(x),
            CommonErrorCause::Unknown(x) | CommonErrorCause::Internal(x) => {
                SparkThrowable::SparkRuntimeException(x)
            }
            CommonErrorCause::Io(x)
            | CommonErrorCause::ArrowMemory(x)
            | CommonErrorCause::ArrowCompute(x)
            | CommonErrorCause::ArrowIpc(x)
            | CommonErrorCause::ArrowCDataInterface(x)
            | CommonErrorCause::FormatCsv(x)
            | CommonErrorCause::FormatJson(x)
            | CommonErrorCause::FormatParquet(x)
            | CommonErrorCause::FormatAvro(x)
            | CommonErrorCause::ArrowDictionaryKeyOverflow(x)
            | CommonErrorCause::ArrowRunEndIndexOverflow(x)
            | CommonErrorCause::ArrowOffsetOverflow(x) => {
                SparkThrowable::QueryExecutionException(x)
            }
            CommonErrorCause::ArrowDivideByZero(x)
            | CommonErrorCause::ArrowArithmeticOverflow(x) => {
                SparkThrowable::ArithmeticException(x)
            }
            CommonErrorCause::ArrowParse(x) => SparkThrowable::ParseException(x),
            CommonErrorCause::Python(PythonErrorCause { summary, traceback }) => {
                // The message must end with a newline character
                // since the PySpark unit tests expect it.
                let message = if let Some(traceback) = traceback {
                    // Each line string already ends with a newline character.
                    traceback.join("")
                } else {
                    format!("{summary}\n")
                };
                if message.contains("net.razorvine.pickle.PickleException") {
                    SparkThrowable::SparkException(message)
                } else {
                    SparkThrowable::PythonException(message)
                }
            }
            CommonErrorCause::ArrowCast(x) => cast_error_to_throwable(x),
            CommonErrorCause::Schema(x)
            | CommonErrorCause::Plan(x)
            | CommonErrorCause::Configuration(x) => SparkThrowable::AnalysisException(x),
            CommonErrorCause::Execution(x) => {
                if is_timestamp_parse_error(&x) {
                    SparkThrowable::DateTimeException(x)
                } else if is_array_index_out_of_bounds_error(&x) {
                    SparkThrowable::ArrayIndexOutOfBoundsException(x)
                } else {
                    // TODO: handle situations where a different exception type is more appropriate.
                    SparkThrowable::AnalysisException(x)
                }
            }
            CommonErrorCause::DeltaTable(x) => SparkThrowable::QueryExecutionException(x),
        }
    }
}

fn is_timestamp_parse_error(message: &str) -> bool {
    message.starts_with("Error parsing timestamp")
}

fn is_array_index_out_of_bounds_error(message: &str) -> bool {
    message.contains("[INVALID_ARRAY_INDEX]")
}

fn cast_error_to_throwable(message: String) -> SparkThrowable {
    // Spark ANSI cast failures map by target type:
    //   numeric target -> NumberFormatException   (CAST('abc' AS DOUBLE/INT/...))
    //   boolean target -> SparkRuntimeException    (CAST('abc' AS BOOLEAN))
    // Arrow's `CastError` message ends with "to value of <Type> type".
    if message.contains("Boolean type") {
        SparkThrowable::SparkRuntimeException(message)
    } else if [
        "Int8", "Int16", "Int32", "Int64", "UInt", "Float16", "Float32", "Float64", "Decimal",
    ]
    .iter()
    .any(|t| message.contains(t))
    {
        SparkThrowable::NumberFormatException(message)
    } else {
        SparkThrowable::AnalysisException(message)
    }
}

impl From<SparkError> for Status {
    fn from(error: SparkError) -> Self {
        match error {
            SparkError::ArrowError(e) => {
                SparkThrowable::from(CommonErrorCause::new::<PyErrExtractor>(&e)).into()
            }
            SparkError::DataFusionError(e) => {
                SparkThrowable::from(CommonErrorCause::new::<PyErrExtractor>(&e)).into()
            }
            e @ SparkError::MissingArgument(_) | e @ SparkError::InvalidArgument(_) => {
                SparkThrowable::IllegalArgumentException(e.to_string()).into()
            }
            SparkError::IoError(e) => SparkThrowable::QueryExecutionException(e.to_string()).into(),
            SparkError::JsonError(e) => {
                SparkThrowable::IllegalArgumentException(e.to_string()).into()
            }
            SparkError::NotImplemented(s) | SparkError::NotSupported(s) => {
                SparkThrowable::UnsupportedOperationException(s).into()
            }
            SparkError::AnalysisError(s) => SparkThrowable::AnalysisException(s).into(),
            SparkError::DeadlineExceeded(message) => {
                Status::deadline_exceeded(truncate_grpc_message(&message))
            }
            e @ SparkError::SendError(_) => {
                Status::cancelled(truncate_grpc_message(&e.to_string()))
            }
            e @ SparkError::InternalError(_) => {
                Status::internal(truncate_grpc_message(&e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_grpc_message_preserves_tail() {
        let message = format!(
            "python traceback start\n{}\npyspark.errors.PythonException: [UDTF_ARROW_TYPE_CONVERSION_ERROR]\n",
            "x".repeat(MAX_GRPC_STATUS_MESSAGE_LEN * 2)
        );

        let truncated = truncate_grpc_message(&message);

        assert!(truncated.len() <= MAX_GRPC_STATUS_MESSAGE_LEN);
        assert!(truncated.starts_with("python traceback start\n"));
        assert!(truncated.contains(TRUNCATED_MARKER));
        assert!(truncated.ends_with("[UDTF_ARROW_TYPE_CONVERSION_ERROR]\n"));
    }

    #[test]
    fn execution_timestamp_parse_error_maps_to_datetime_exception() {
        let throwable = SparkThrowable::from(CommonErrorCause::Execution(
            "Error parsing timestamp from '2023-01-01' using format '%d-%m-%Y': input contains invalid characters".to_string(),
        ));

        assert!(matches!(&throwable, SparkThrowable::DateTimeException(_)));
        assert_eq!(throwable.class_name(), "java.time.DateTimeException");
    }
}
