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

pub type DeltaResult<T> = Result<T, DeltaError>;

#[derive(Debug, Error)]
pub enum DeltaError {
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

    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Delta transaction error: {0}")]
    Transaction(#[from] TransactionError),
}

impl DeltaError {
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

impl From<DeltaError> for DataFusionError {
    fn from(err: DeltaError) -> Self {
        match err {
            DeltaError::DataFusion(inner) => inner,
            DeltaError::Io(err) => DataFusionError::IoError(err),
            DeltaError::Arrow(err) => DataFusionError::ArrowError(Box::new(err), None),
            DeltaError::ObjectStore(err) => DataFusionError::ObjectStore(Box::new(err)),
            DeltaError::ObjectStorePath(source) => {
                DataFusionError::ObjectStore(Box::new(ObjectStoreError::InvalidPath { source }))
            }
            DeltaError::Parquet(err) => DataFusionError::ParquetError(Box::new(err)),
            DeltaError::Json(err) => DataFusionError::External(Box::new(err)),
            DeltaError::Config(msg) => DataFusionError::Configuration(msg),
            DeltaError::Transaction(err) => DataFusionError::External(Box::new(err)),
            DeltaError::FileNotFound(path) => {
                DataFusionError::ObjectStore(Box::new(ObjectStoreError::NotFound {
                    path,
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "File not found in Delta kernel",
                    )),
                }))
            }
            DeltaError::MissingColumn(column) => DataFusionError::SchemaError(
                Box::new(SchemaError::FieldNotFound {
                    field: Box::new(Column::from_name(column)),
                    valid_fields: vec![],
                }),
                Box::new(None),
            ),
            DeltaError::InvalidUrl(err) => {
                DataFusionError::Configuration(format!("Invalid Delta URL: {err}"))
            }
            DeltaError::InvalidTableLocation(location) => {
                DataFusionError::Configuration(format!("Invalid table location: {location}"))
            }
            DeltaError::MissingVersion => {
                DataFusionError::Execution("No table version found.".to_string())
            }
            DeltaError::Unsupported(msg) => DataFusionError::NotImplemented(msg),
            DeltaError::Generic(msg) | DeltaError::Schema(msg) => DataFusionError::Execution(msg),
            DeltaError::External(source) => DataFusionError::External(source),
            DeltaError::InternalError(msg) => DataFusionError::Internal(msg),
            DeltaError::MissingMetadata => {
                DataFusionError::Execution("No table metadata found in delta log.".to_string())
            }
            DeltaError::MissingProtocol => {
                DataFusionError::Execution("No protocol found in delta log.".to_string())
            }
            DeltaError::MissingMetadataAndProtocol => DataFusionError::Execution(
                "No table metadata or protocol found in delta log.".to_string(),
            ),
            DeltaError::ParseError(value, ty) => {
                DataFusionError::Execution(format!("Failed to parse value '{value}' as '{ty}'"))
            }
        }
    }
}
