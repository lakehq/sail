use std::collections::HashMap;
use std::sync::PoisonError;

use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use prost::{DecodeError, UnknownEnumValue};
use pyo3::PyErr;
use sail_common::error::CommonError;
use sail_plan::error::PlanError;
use sail_sql::error::SqlError;
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

    pub fn send(message: impl Into<String>) -> Self {
        SparkError::SendError(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SparkError::InternalError(message.into())
    }
}

impl From<CommonError> for SparkError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::DataFusionError(e) => SparkError::DataFusionError(e),
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
            SqlError::DataFusionError(e) => SparkError::DataFusionError(e),
            SqlError::MissingArgument(message) => SparkError::MissingArgument(message),
            SqlError::InvalidArgument(message) => SparkError::InvalidArgument(message),
            SqlError::NotSupported(message) => SparkError::NotSupported(message),
            SqlError::InternalError(message) => SparkError::InternalError(message),
            SqlError::SqlParserError(e) => SparkError::InvalidArgument(e.to_string()),
            SqlError::NotImplemented(message) => SparkError::NotImplemented(message),
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

#[allow(clippy::enum_variant_names)]
enum SparkThrowable {
    ParseException(String),
    AnalysisException(String),
    #[allow(dead_code)]
    StreamingQueryException(String),
    QueryExecutionException(String),
    #[allow(dead_code)]
    NumberFormatException(String),
    IllegalArgumentException(String),
    ArithmeticException(String),
    UnsupportedOperationException(String),
    #[allow(dead_code)]
    ArrayIndexOutOfBoundsException(String),
    #[allow(dead_code)]
    DateTimeException(String),
    SparkRuntimeException(String),
    #[allow(dead_code)]
    SparkUpgradeException(String),
    PythonException(String),
}

impl SparkThrowable {
    fn message(&self) -> &str {
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
            | SparkThrowable::PythonException(message) => message,
        }
    }

    fn class_name(&self) -> &'static str {
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
            SparkThrowable::PythonException(_) => "org.apache.spark.api.python.PythonException",
        }
    }
}

impl From<SparkThrowable> for Status {
    fn from(throwable: SparkThrowable) -> Status {
        let class = throwable.class_name();

        let mut metadata = HashMap::new();
        // We do not add the "stackTrace" field since the Java stack trace is not available.
        metadata.insert("classes".into(), format!("[\"{class}\"]"));

        let mut details = ErrorDetails::new();
        details.set_error_info(class, "org.apache.spark", metadata);

        // The original Spark Connect server implementation uses the "INTERNAL" status code
        // for all Spark exceptions, so we do the same here.
        // Reference: org.apache.spark.sql.connect.utils.ErrorUtils#buildStatusFromThrowable
        Status::with_error_details(Code::Internal, throwable.message(), details)
    }
}

impl From<SparkError> for Status {
    fn from(error: SparkError) -> Self {
        // TODO: extract nested error from `DataFusionError::Context`
        match error {
            SparkError::ArrowError(ArrowError::ExternalError(e))
            | SparkError::DataFusionError(DataFusionError::ArrowError(
                ArrowError::ExternalError(e),
                _,
            ))
            | SparkError::DataFusionError(DataFusionError::External(e)) => {
                if let Some(e) = e.downcast_ref::<PyErr>() {
                    // TODO: get Python traceback
                    SparkThrowable::PythonException(e.to_string()).into()
                } else {
                    SparkThrowable::SparkRuntimeException(e.to_string()).into()
                }
            }
            SparkError::ArrowError(e)
            | SparkError::DataFusionError(DataFusionError::ArrowError(e, _)) => match e {
                ArrowError::NotYetImplemented(s) => {
                    SparkThrowable::UnsupportedOperationException(s).into()
                }
                ArrowError::CastError(s) | ArrowError::SchemaError(s) => {
                    SparkThrowable::AnalysisException(s).into()
                }
                ArrowError::ParseError(s) => SparkThrowable::ParseException(s).into(),
                ArrowError::DivideByZero => {
                    SparkThrowable::ArithmeticException("divide by zero".to_string()).into()
                }
                ArrowError::InvalidArgumentError(s) => {
                    SparkThrowable::IllegalArgumentException(s).into()
                }
                _ => SparkThrowable::QueryExecutionException(e.to_string()).into(),
            },
            SparkError::DataFusionError(e @ DataFusionError::Plan(_))
            | SparkError::DataFusionError(e @ DataFusionError::Configuration(_)) => {
                SparkThrowable::AnalysisException(e.to_string()).into()
            }
            SparkError::DataFusionError(DataFusionError::SQL(e, _)) => {
                SparkThrowable::ParseException(e.to_string()).into()
            }
            SparkError::DataFusionError(DataFusionError::SchemaError(e, _)) => {
                SparkThrowable::AnalysisException(e.to_string()).into()
            }
            SparkError::DataFusionError(DataFusionError::NotImplemented(s)) => {
                SparkThrowable::UnsupportedOperationException(s).into()
            }
            SparkError::DataFusionError(e @ DataFusionError::Execution(_)) => {
                // TODO: handle situations where a different exception type is more appropriate.
                SparkThrowable::AnalysisException(e.to_string()).into()
            }
            SparkError::DataFusionError(e) => {
                SparkThrowable::SparkRuntimeException(e.to_string()).into()
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
            e @ SparkError::SendError(_) => Status::cancelled(e.to_string()),
            e @ SparkError::InternalError(_) => Status::internal(e.to_string()),
        }
    }
}
