use datafusion_common::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataSourceError {
    #[error("missing option: {key}")]
    MissingOption { key: &'static str },
    #[error("invalid option: {key}: {value}{}", .cause.as_deref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    InvalidOption {
        key: String,
        value: String,
        cause: Option<String>,
    },
}

pub type DataSourceResult<T> = Result<T, DataSourceError>;

impl From<DataSourceError> for DataFusionError {
    fn from(e: DataSourceError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}
