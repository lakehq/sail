use std::error::Error as StdError;

use datafusion::arrow::error::ArrowError;
use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;
use thiserror::Error;

/// Result alias used across the Delta integration.
pub type DeltaResult<T, E = DeltaTableError> = Result<T, E>;

/// Delta table specific error type.
#[derive(Error, Debug)]
pub enum DeltaTableError {
    #[error("Kernel error: {0}")]
    KernelError(#[from] delta_kernel::error::Error),

    #[error("Failed to read delta log object: {source}")]
    ObjectStore {
        #[from]
        source: ObjectStoreError,
    },

    #[error("Failed to parse parquet: {source}")]
    Parquet {
        #[from]
        source: ParquetError,
    },

    #[error("Failed to convert into Arrow schema: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Delta transaction IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("Invalid table version: {0}")]
    InvalidVersion(i64),

    #[error("Not a Delta table: {0}")]
    NotATable(String),

    #[error("Schema mismatch: {msg}")]
    SchemaMismatch { msg: String },

    #[error("Metadata error: {0}")]
    MetadataError(String),

    #[error("Generic DeltaTable error: {0}")]
    Generic(String),

    #[error("Generic error: {source}")]
    GenericError {
        source: Box<dyn StdError + Send + Sync + 'static>,
    },

    #[error("Table has not yet been initialized")]
    NotInitialized,
}

impl DeltaTableError {
    /// Helper to construct a [`DeltaTableError::Generic`] variant.
    pub fn generic(msg: impl ToString) -> Self {
        Self::Generic(msg.to_string())
    }
}

impl From<object_store::path::Error> for DeltaTableError {
    fn from(err: object_store::path::Error) -> Self {
        Self::GenericError {
            source: Box::new(err),
        }
    }
}

impl From<serde_json::Error> for DeltaTableError {
    fn from(err: serde_json::Error) -> Self {
        Self::Generic(err.to_string())
    }
}
