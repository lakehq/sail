use thiserror::Error;

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("external error: {0}")]
    External(String),
}
