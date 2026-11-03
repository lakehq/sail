use thiserror::Error;

pub type LineageResult<T> = Result<T, LineageError>;

#[derive(Debug, Error)]
pub enum LineageError {
    #[error("invalid lineage configuration: {0}")]
    InvalidConfig(String),
    #[error("lineage transport error: {0}")]
    Transport(String),
    #[error("error in IO: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error in HTTP: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("error in object store: {0}")]
    ObjectStoreError(#[from] object_store::Error),
}
