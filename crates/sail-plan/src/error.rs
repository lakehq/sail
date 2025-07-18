use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use sail_common::error::CommonError;
use sail_python_udf::error::PyUdfError;
use sail_sql_analyzer::error::SqlError;
use thiserror::Error;

pub type PlanResult<T> = Result<T, PlanError>;

#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum PlanError {
    // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] ArrowError),
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
    #[error("delta table error: {0}")]
    DeltaTableError(String),
}

impl PlanError {
    pub fn todo(message: impl Into<String>) -> Self {
        PlanError::NotImplemented(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        PlanError::NotSupported(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        PlanError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        PlanError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        PlanError::InternalError(message.into())
    }
}

impl From<CommonError> for PlanError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => PlanError::MissingArgument(message),
            CommonError::InvalidArgument(message) => PlanError::InvalidArgument(message),
            CommonError::NotSupported(message) => PlanError::NotSupported(message),
            CommonError::InternalError(message) => PlanError::InternalError(message),
        }
    }
}

impl From<SqlError> for PlanError {
    fn from(value: SqlError) -> Self {
        match value {
            SqlError::SqlParserError(message) => PlanError::InvalidArgument(message),
            SqlError::MissingArgument(message) => PlanError::MissingArgument(message),
            SqlError::InvalidArgument(message) => PlanError::InvalidArgument(message),
            SqlError::NotImplemented(message) => PlanError::NotImplemented(message),
            SqlError::NotSupported(message) => PlanError::NotSupported(message),
            SqlError::InternalError(message) => PlanError::InternalError(message),
        }
    }
}

impl From<PyUdfError> for PlanError {
    fn from(error: PyUdfError) -> Self {
        match error {
            PyUdfError::PythonError(e) => {
                PlanError::DataFusionError(DataFusionError::External(e.into()))
            }
            PyUdfError::IoError(e) => PlanError::DataFusionError(DataFusionError::IoError(e)),
            PyUdfError::InvalidArgument(message) => PlanError::InvalidArgument(message),
            PyUdfError::InternalError(message) => PlanError::InternalError(message),
        }
    }
}
