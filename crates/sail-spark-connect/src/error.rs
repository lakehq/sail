use std::collections::HashMap;
use std::num::ParseIntError;
use std::str::ParseBoolError;
use std::sync::PoisonError;

use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use prost::{DecodeError, UnknownEnumValue};
use sail_cache::error::CacheError;
use sail_common::actor::ActorSendError;
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
    #[error("parse error: {0}")]
    ParseError(String),
    // https://github.com/apache/spark/blob/v4.2.1/common/utils/src/main/resources/error/error-conditions.json#L3876
    #[error("The handle {0} is invalid. {1}")]
    InvalidHandle(String, InvalidHandle),
}

// https://github.com/apache/spark/blob/v4.2.1/common/utils/src/main/resources/error/error-conditions.json#L3879-L3913
#[derive(Debug, Error)]
pub enum InvalidHandle {
    #[error("Handle must be an UUID string of the format '00112233-4455-6677-8899-aabbccddeeff'")]
    Format,
    #[error("The existing Spark server driver instance has restarted. Please reconnect.")]
    SessionChanged,
    #[error("Session was closed.")]
    SessionClosed,
    #[error("Session not found.")]
    SessionNotFound,
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
            ExecutionError::FlightTransportError(e) => SparkError::InternalError(e.to_string()),
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
            SessionError::NotFound(session_id) => {
                SparkError::InvalidHandle(session_id, InvalidHandle::SessionNotFound)
            }
            SessionError::NotRunning(session_id) => {
                SparkError::InvalidHandle(session_id, InvalidHandle::SessionClosed)
            }
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

impl<T> From<ActorSendError<T>> for SparkError {
    fn from(error: ActorSendError<T>) -> Self {
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
    SparkSQLException(String, SparkErrorClass),
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
            | SparkThrowable::SparkSQLException(message, _)
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
            SparkThrowable::SparkSQLException(_, _) => "org.apache.spark.SparkSQLException",
            SparkThrowable::PythonException(_) => "org.apache.spark.api.python.PythonException",
        }
    }

    pub fn error_class(&self) -> Option<&SparkErrorClass> {
        match self {
            SparkThrowable::SparkSQLException(_, error_class) => Some(error_class),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SparkErrorClass {
    pub name: &'static str,
    pub message_parameters: HashMap<&'static str, String>,
    pub sql_state: &'static str,
}

impl SparkErrorClass {
    fn invalid_handle(handle: &str, sub_class: &InvalidHandle) -> Self {
        SparkErrorClass {
            name: match sub_class {
                InvalidHandle::Format => "INVALID_HANDLE.FORMAT",
                InvalidHandle::SessionChanged => "INVALID_HANDLE.SESSION_CHANGED",
                InvalidHandle::SessionClosed => "INVALID_HANDLE.SESSION_CLOSED",
                InvalidHandle::SessionNotFound => "INVALID_HANDLE.SESSION_NOT_FOUND",
            },
            message_parameters: HashMap::from([("handle", handle.to_string())]),
            sql_state: "HY000",
        }
    }

    fn message(&self, detail: &impl std::fmt::Display) -> String {
        format!("[{}] {detail} SQLSTATE: {}", self.name, self.sql_state)
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

/// The maximum size of metadata fields in `ErrorInfo`.
///
/// Matches the default of Spark's `spark.connect.grpc.maxMetadataSize`.
/// Like Spark, `errorClass` and `messageParameters` are omitted together
/// when the serialized parameters exceed this size.
const MAX_METADATA_SIZE: usize = 1024;

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

        if let Some(error_class) = throwable.error_class() {
            let message_parameters = serde_json::json!(error_class.message_parameters).to_string();
            if message_parameters.len() <= MAX_METADATA_SIZE {
                metadata.insert("errorClass".into(), error_class.name.to_string());
                metadata.insert("messageParameters".into(), message_parameters);
            }
            metadata.insert("sqlState".into(), error_class.sql_state.to_string());
        }

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
            CommonErrorCause::Unknown(x)
            | CommonErrorCause::Internal(x)
            | CommonErrorCause::CommitStateUnknown(x) => SparkThrowable::SparkRuntimeException(x),
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
                } else if is_division_by_zero_error(&x) {
                    SparkThrowable::ArithmeticException(x)
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

fn is_division_by_zero_error(message: &str) -> bool {
    message.contains("[DIVIDE_BY_ZERO]") || message.contains("[REMAINDER_BY_ZERO]")
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
            SparkError::ParseError(s) => SparkThrowable::ParseException(s).into(),
            ref e @ SparkError::InvalidHandle(ref handle, ref sub_class) => {
                let error_class = SparkErrorClass::invalid_handle(handle, sub_class);
                SparkThrowable::SparkSQLException(error_class.message(e), error_class).into()
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
    fn error_class_metadata_is_added() -> Result<(), String> {
        let sid = "00000000-0000-0000-0000-000000000000".to_string();
        let status = Status::from(SparkError::InvalidHandle(sid, InvalidHandle::SessionClosed));
        let info = status
            .get_details_error_info()
            .ok_or("missing error info")?;
        let error_class = info.metadata.get("errorClass");
        assert_eq!(
            error_class.ok_or("missing error class")?,
            "INVALID_HANDLE.SESSION_CLOSED"
        );
        let message_parameters = info.metadata.get("messageParameters");
        assert_eq!(
            message_parameters.ok_or("missing message parameters")?,
            "{\"handle\":\"00000000-0000-0000-0000-000000000000\"}"
        );
        let sql_state = info.metadata.get("sqlState");
        assert_eq!(sql_state.ok_or("missing sql state")?, "HY000");
        Ok(())
    }

    #[test]
    fn oversized_message_parameters_are_omitted() -> Result<(), String> {
        let sid = "x".repeat(MAX_METADATA_SIZE + 1);
        let status = Status::from(SparkError::InvalidHandle(sid, InvalidHandle::SessionClosed));
        let info = status
            .get_details_error_info()
            .ok_or("missing error info")?;
        assert!(!info.metadata.contains_key("errorClass"));
        assert!(!info.metadata.contains_key("messageParameters"));
        let sql_state = info.metadata.get("sqlState");
        assert_eq!(sql_state.ok_or("missing sql state")?, "HY000");
        Ok(())
    }

    #[test]
    fn execution_timestamp_parse_error_maps_to_datetime_exception() {
        let throwable = SparkThrowable::from(CommonErrorCause::Execution(
            "Error parsing timestamp from '2023-01-01' using format '%d-%m-%Y': input contains invalid characters".to_string(),
        ));

        assert!(matches!(&throwable, SparkThrowable::DateTimeException(_)));
        assert_eq!(throwable.class_name(), "java.time.DateTimeException");
    }

    /// The interval arithmetic functions return their Arrow errors as external errors.
    #[test]
    fn external_arrow_arithmetic_error_maps_to_arithmetic_exception() {
        let error = DataFusionError::External(Box::new(ArrowError::ArithmeticOverflow(
            "[INTERVAL_DIVIDED_BY_ZERO] Division by zero.".to_string(),
        )));
        let throwable = SparkThrowable::from(CommonErrorCause::new::<PyErrExtractor>(&error));
        assert!(matches!(&throwable, SparkThrowable::ArithmeticException(_)));
        assert_eq!(
            throwable.message(),
            "[INTERVAL_DIVIDED_BY_ZERO] Division by zero."
        );
    }

    #[test]
    fn execution_division_by_zero_error_maps_to_arithmetic_exception() {
        for message in [
            "[DIVIDE_BY_ZERO] Division by zero.",
            "[REMAINDER_BY_ZERO] Remainder by zero.",
        ] {
            let throwable = SparkThrowable::from(CommonErrorCause::Execution(message.to_string()));
            assert!(matches!(&throwable, SparkThrowable::ArithmeticException(_)));
            assert_eq!(throwable.class_name(), "java.lang.ArithmeticException");
        }
    }
}
