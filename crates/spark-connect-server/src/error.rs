use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SparkError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in SQL parser: {0}")]
    SqlParserError(#[from] sqlparser::parser::ParserError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl SparkError {
    pub fn todo(message: impl Into<String>) -> Self {
        SparkError::NotImplemented(message.into())
    }

    pub fn missing(message: impl Into<String>) -> Self {
        SparkError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        SparkError::InvalidArgument(message.into())
    }
}

pub trait ProtoFieldExt<T> {
    fn required(self, description: impl Into<String>) -> Result<T, SparkError>;
}

impl<T> ProtoFieldExt<T> for Option<T> {
    fn required(self, description: impl Into<String>) -> Result<T, SparkError> {
        self.ok_or_else(|| SparkError::MissingArgument(description.into()))
    }
}

impl From<SparkError> for tonic::Status {
    fn from(error: SparkError) -> Self {
        match error {
            SparkError::DataFusionError(DataFusionError::Plan(s))
            | SparkError::DataFusionError(DataFusionError::Configuration(s)) => {
                tonic::Status::invalid_argument(s)
            }
            SparkError::DataFusionError(DataFusionError::SQL(e, _)) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::DataFusionError(DataFusionError::SchemaError(e, _)) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            SparkError::DataFusionError(DataFusionError::NotImplemented(s)) => {
                tonic::Status::unimplemented(s)
            }
            SparkError::DataFusionError(e) => tonic::Status::internal(e.to_string()),
            SparkError::SqlParserError(e) => tonic::Status::invalid_argument(e.to_string()),
            SparkError::MissingArgument(s) | SparkError::InvalidArgument(s) => {
                tonic::Status::invalid_argument(s)
            }
            SparkError::ArrowError(e) => tonic::Status::internal(e.to_string()),
            SparkError::NotImplemented(s) => tonic::Status::unimplemented(s),
        }
    }
}
