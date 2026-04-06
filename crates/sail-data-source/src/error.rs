use datafusion_common::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataSourceError {
    #[error("missing option: {key}")]
    MissingOption { key: &'static str },
    #[error("invalid option: {key}: {value}")]
    InvalidOption { key: String, value: String },
}

pub type DataSourceResult<T> = Result<T, DataSourceError>;

impl From<DataSourceError> for DataFusionError {
    fn from(e: DataSourceError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}
