// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion_common::{Column, DataFusionError, SchemaError};
use object_store::Error as ObjectStoreError;
use thiserror::Error;

use crate::kernel::transaction::TransactionError;

/// Result type that is used throughout the Delta Lake integration.
pub type DeltaResult<T> = Result<T, DeltaError>;

/// Kernel-like error variants used by the Sail Delta implementation.
#[derive(Debug, Error)]
pub enum KernelError {
    #[error("No table version found.")]
    MissingVersion,

    #[error("Invalid table location: {0}")]
    InvalidTableLocation(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Missing column: {0}")]
    MissingColumn(String),

    #[error("{0}")]
    Schema(String),

    #[error("{0}")]
    Generic(String),

    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error(transparent)]
    InvalidUrl(#[from] url::ParseError),

    #[error("{0}")]
    Unsupported(String),

    #[error("{0}")]
    InternalError(String),

    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    #[error("No protocol found in delta log.")]
    MissingProtocol,

    #[error("No table metadata or protocol found in delta log.")]
    MissingMetadataAndProtocol,

    #[error("Failed to parse value '{0}' as '{1}'")]
    ParseError(String, String),
}

impl KernelError {
    pub fn generic(msg: impl ToString) -> Self {
        Self::Generic(msg.to_string())
    }

    pub fn generic_err(
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::External(source.into())
    }

    pub fn schema(msg: impl ToString) -> Self {
        Self::Schema(msg.to_string())
    }

    pub fn invalid_table_location(location: impl ToString) -> Self {
        Self::InvalidTableLocation(location.to_string())
    }

    pub fn missing_column(name: impl ToString) -> Self {
        Self::MissingColumn(name.to_string())
    }
}

/// Error type that bridges Delta Kernel and DataFusion failures.
#[derive(Debug, Error)]
pub enum DeltaError {
    #[error(transparent)]
    Kernel(#[from] KernelError),

    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Url(#[from] url::ParseError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Delta table operation failed: {0}")]
    Generic(String),

    #[error("Delta transaction error: {0}")]
    Transaction(#[from] TransactionError),
}

impl DeltaError {
    /// Convenience helper that mirrors [`KernelError::generic`].
    pub fn generic(msg: impl ToString) -> Self {
        KernelError::generic(msg).into()
    }

    /// Convenience helper that mirrors [`KernelError::generic_err`].
    pub fn generic_err(
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        KernelError::generic_err(source).into()
    }

    /// Convenience helper that mirrors [`KernelError::schema`].
    pub fn schema(msg: impl ToString) -> Self {
        KernelError::schema(msg).into()
    }

    /// Convenience helper that mirrors [`KernelError::invalid_table_location`].
    pub fn invalid_table_location(location: impl ToString) -> Self {
        KernelError::invalid_table_location(location).into()
    }

    /// Convenience helper that mirrors [`KernelError::missing_column`].
    pub fn missing_column(name: impl ToString) -> Self {
        KernelError::missing_column(name).into()
    }
}

impl From<DeltaError> for DataFusionError {
    fn from(err: DeltaError) -> Self {
        match err {
            DeltaError::DataFusion(inner) => inner,
            DeltaError::Io(err) => DataFusionError::IoError(err),
            DeltaError::Arrow(err) => DataFusionError::ArrowError(Box::new(err), None),
            DeltaError::ObjectStore(err) => DataFusionError::ObjectStore(Box::new(err)),
            DeltaError::Url(err) => {
                DataFusionError::Configuration(format!("Invalid URL format: {err}"))
            }
            DeltaError::Json(err) => DataFusionError::External(Box::new(err)),
            DeltaError::Config(msg) => DataFusionError::Configuration(msg),
            DeltaError::Generic(msg) => DataFusionError::Execution(msg),
            DeltaError::Transaction(err) => DataFusionError::External(Box::new(err)),
            DeltaError::Kernel(err) => map_kernel_error_to_datafusion(err),
        }
    }
}

impl From<object_store::path::Error> for DeltaError {
    fn from(err: object_store::path::Error) -> Self {
        KernelError::ObjectStorePath(err).into()
    }
}

fn map_kernel_error_to_datafusion(err: KernelError) -> DataFusionError {
    match err {
        KernelError::Arrow(err) => DataFusionError::ArrowError(Box::new(err), None),
        KernelError::Io(err) => DataFusionError::IoError(err),
        KernelError::ObjectStore(err) => DataFusionError::ObjectStore(Box::new(err)),
        KernelError::ObjectStorePath(source) => {
            DataFusionError::ObjectStore(Box::new(ObjectStoreError::InvalidPath { source }))
        }
        KernelError::Parquet(err) => DataFusionError::ParquetError(Box::new(err)),
        KernelError::FileNotFound(path) => {
            DataFusionError::ObjectStore(Box::new(ObjectStoreError::NotFound {
                path,
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "File not found in Delta kernel",
                )),
            }))
        }
        KernelError::MissingColumn(column) => DataFusionError::SchemaError(
            Box::new(SchemaError::FieldNotFound {
                field: Box::new(Column::from_name(column)),
                valid_fields: vec![],
            }),
            Box::new(None),
        ),
        KernelError::InvalidUrl(err) => {
            DataFusionError::Configuration(format!("Invalid Delta URL: {err}"))
        }
        KernelError::InvalidTableLocation(location) => {
            DataFusionError::Configuration(format!("Invalid table location: {location}"))
        }
        KernelError::MissingVersion => {
            DataFusionError::Execution("No table version found.".to_string())
        }
        KernelError::Unsupported(msg) => DataFusionError::NotImplemented(msg),
        KernelError::Generic(msg) | KernelError::Schema(msg) => DataFusionError::Execution(msg),
        KernelError::External(source) => DataFusionError::External(source),
        KernelError::InternalError(msg) => DataFusionError::Internal(msg),
        KernelError::MissingMetadata => {
            DataFusionError::Execution("No table metadata found in delta log.".to_string())
        }
        KernelError::MissingProtocol => {
            DataFusionError::Execution("No protocol found in delta log.".to_string())
        }
        KernelError::MissingMetadataAndProtocol => DataFusionError::Execution(
            "No table metadata or protocol found in delta log.".to_string(),
        ),
        KernelError::ParseError(value, ty) => {
            DataFusionError::Execution(format!("Failed to parse value '{value}' as '{ty}'"))
        }
    }
}
