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

use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use object_store::path::Error as ObjectStorePathError;
use object_store::Error as ObjectStoreError;
use thiserror::Error;

/// Convenience result type for Iceberg-specific operations.
pub type IcebergResult<T> = Result<T, IcebergError>;

/// Canonical error type that tracks failures inside the Iceberg integration.
#[derive(Debug, Error)]
pub enum IcebergError {
    #[error("Iceberg IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    Parquet(#[from] ParquetError),

    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),

    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] ObjectStorePathError),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid URL '{url}': {source}")]
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },

    #[error("Table location format error: {0}")]
    InvalidLocation(String),

    #[error("Iceberg schema error: {0}")]
    Schema(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("General Iceberg error: {0}")]
    General(String),
}

impl From<IcebergError> for DataFusionError {
    fn from(err: IcebergError) -> Self {
        match err {
            IcebergError::DataFusion(inner) => inner,
            IcebergError::Arrow(inner) => DataFusionError::ArrowError(Box::new(inner), None),
            IcebergError::Parquet(inner) => DataFusionError::ParquetError(Box::new(inner)),
            IcebergError::ObjectStore(inner) => DataFusionError::ObjectStore(Box::new(inner)),
            IcebergError::ObjectStorePath(inner) => {
                DataFusionError::ObjectStore(Box::new(ObjectStoreError::InvalidPath {
                    source: inner,
                }))
            }
            IcebergError::Io(inner) => DataFusionError::IoError(inner),
            IcebergError::Avro(inner) => DataFusionError::External(Box::new(inner)),
            IcebergError::Json(inner) => DataFusionError::External(Box::new(inner)),
            IcebergError::InvalidUrl { url, source } => {
                DataFusionError::Configuration(format!("Invalid URL '{url}': {source}"))
            }
            IcebergError::InvalidLocation(msg) => DataFusionError::Configuration(msg),
            IcebergError::Schema(msg) => {
                DataFusionError::Execution(format!("Iceberg Schema Error: {msg}"))
            }
            IcebergError::NotImplemented(msg) => DataFusionError::NotImplemented(msg),
            IcebergError::General(msg) => DataFusionError::Execution(msg),
        }
    }
}

impl IcebergError {
    /// Attach the original URI that failed to parse.
    pub fn invalid_url(url: impl ToString, source: url::ParseError) -> Self {
        Self::InvalidUrl {
            url: url.to_string(),
            source,
        }
    }

    /// Convenience helper for general string-based errors.
    pub fn general(msg: impl ToString) -> Self {
        Self::General(msg.to_string())
    }

    /// Convenience helper for schema-related errors.
    pub fn schema(msg: impl ToString) -> Self {
        Self::Schema(msg.to_string())
    }
}
