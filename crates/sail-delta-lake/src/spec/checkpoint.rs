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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::{
    Add, DeletionVectorDescriptor, DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol,
    Remove, TableFeature, Transaction,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointActionRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<CheckpointAdd>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove: Option<CheckpointRemove>,
    #[serde(rename = "metaData", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<CheckpointProtocol>,
    #[serde(rename = "txn", skip_serializing_if = "Option::is_none")]
    pub txn: Option<Transaction>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointProtocol {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,
}

impl From<Protocol> for CheckpointProtocol {
    fn from(value: Protocol) -> Self {
        Self {
            min_reader_version: value.min_reader_version(),
            min_writer_version: value.min_writer_version(),
            reader_features: value.reader_features().map(|features| {
                features
                    .iter()
                    .map(|feature| feature.as_str().to_string())
                    .collect()
            }),
            writer_features: value.writer_features().map(|features| {
                features
                    .iter()
                    .map(|feature| feature.as_str().to_string())
                    .collect()
            }),
        }
    }
}

impl TryFrom<CheckpointProtocol> for Protocol {
    type Error = DeltaTableError;

    fn try_from(value: CheckpointProtocol) -> Result<Self, Self::Error> {
        Ok(Protocol::new(
            value.min_reader_version,
            value.min_writer_version,
            value
                .reader_features
                .map(|features| {
                    features
                        .into_iter()
                        .map(|feature| TableFeature::parse_str_name(&feature))
                        .collect::<DeltaResult<Vec<_>>>()
                })
                .transpose()?,
            value
                .writer_features
                .map(|features| {
                    features
                        .into_iter()
                        .map(|feature| TableFeature::parse_str_name(&feature))
                        .collect::<DeltaResult<Vec<_>>>()
                })
                .transpose()?,
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointDeletionVector {
    pub storage_type: String,
    pub path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,
    pub size_in_bytes: i32,
    pub cardinality: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointAdd {
    #[serde(with = "crate::spec::utils::serde_path")]
    pub path: String,
    pub partition_values: HashMap<String, Option<String>>,
    pub size: i64,
    pub modification_time: i64,
    pub data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<CheckpointDeletionVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clustering_provider: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRemove {
    #[serde(with = "crate::spec::utils::serde_path")]
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,
    pub data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_file_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<CheckpointDeletionVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpointHint {
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_of_add_files: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}

impl From<DeletionVectorDescriptor> for CheckpointDeletionVector {
    fn from(value: DeletionVectorDescriptor) -> Self {
        Self {
            storage_type: value.storage_type.as_ref().to_string(),
            path_or_inline_dv: value.path_or_inline_dv,
            offset: value.offset,
            size_in_bytes: value.size_in_bytes,
            cardinality: value.cardinality,
        }
    }
}

impl TryFrom<CheckpointDeletionVector> for DeletionVectorDescriptor {
    type Error = DeltaTableError;

    fn try_from(value: CheckpointDeletionVector) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_type: std::str::FromStr::from_str(&value.storage_type).map_err(|_| {
                DeltaTableError::generic(format!(
                    "Unsupported deletion vector storage type '{}'",
                    value.storage_type
                ))
            })?,
            path_or_inline_dv: value.path_or_inline_dv,
            offset: value.offset,
            size_in_bytes: value.size_in_bytes,
            cardinality: value.cardinality,
        })
    }
}

impl From<Add> for CheckpointAdd {
    fn from(value: Add) -> Self {
        Self {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(CheckpointDeletionVector::from),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

impl TryFrom<CheckpointAdd> for Add {
    type Error = DeltaTableError;

    fn try_from(value: CheckpointAdd) -> Result<Self, Self::Error> {
        Ok(Self {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(TryInto::try_into).transpose()?,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
            commit_version: None,
            commit_timestamp: None,
        })
    }
}

impl From<Remove> for CheckpointRemove {
    fn from(value: Remove) -> Self {
        Self {
            path: value.path,
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            stats: None,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(CheckpointDeletionVector::from),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl TryFrom<CheckpointRemove> for Remove {
    type Error = DeltaTableError;

    fn try_from(value: CheckpointRemove) -> Result<Self, Self::Error> {
        Ok(Self {
            path: value.path,
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(TryInto::try_into).transpose()?,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        })
    }
}
