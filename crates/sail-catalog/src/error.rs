use datafusion_common::DataFusionError;
use thiserror::Error;

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("{0} already exists: {1}")]
    AlreadyExists(&'static str, String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("external error: {0}")]
    External(String),
}
