// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs>
// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/mod.rs>
// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/protocol/mod.rs>

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use chrono::DateTime;
use delta_kernel::actions::{Metadata, Protocol};
use object_store::path::Path;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use crate::kernel::statistics::Stats;
use crate::kernel::{DeltaResult, DeltaTableError};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum StorageType {
    #[serde(rename = "u")]
    #[default]
    UuidRelativePath,
    #[serde(rename = "i")]
    Inline,
    #[serde(rename = "p")]
    AbsolutePath,
}

impl FromStr for StorageType {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u" => Ok(Self::UuidRelativePath),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::AbsolutePath),
            _ => Err(DeltaTableError::generic(format!(
                "Unknown storage format: '{s}'."
            ))),
        }
    }
}

impl AsRef<str> for StorageType {
    fn as_ref(&self) -> &str {
        match self {
            Self::UuidRelativePath => "u",
            Self::Inline => "i",
            Self::AbsolutePath => "p",
        }
    }
}

impl fmt::Display for StorageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeletionVectorDescriptor {
    pub storage_type: StorageType,
    pub path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,
    pub size_in_bytes: i32,
    pub cardinality: i64,
}

/// Delta Lake action envelope.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Action {
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    Protocol(Protocol),
    Add(Add),
    Remove(Remove),
    Cdc(AddCDCFile),
    Txn(Transaction),
    CommitInfo(CommitInfo),
    DomainMetadata(DomainMetadata),
    CheckpointMetadata(CheckpointMetadata),
    Sidecar(Sidecar),
}

impl From<Add> for Action {
    fn from(value: Add) -> Self {
        Self::Add(value)
    }
}

impl From<Remove> for Action {
    fn from(value: Remove) -> Self {
        Self::Remove(value)
    }
}

impl From<AddCDCFile> for Action {
    fn from(value: AddCDCFile) -> Self {
        Self::Cdc(value)
    }
}

impl From<Transaction> for Action {
    fn from(value: Transaction) -> Self {
        Self::Txn(value)
    }
}

impl From<CommitInfo> for Action {
    fn from(value: CommitInfo) -> Self {
        Self::CommitInfo(value)
    }
}

impl From<DomainMetadata> for Action {
    fn from(value: DomainMetadata) -> Self {
        Self::DomainMetadata(value)
    }
}

impl From<CheckpointMetadata> for Action {
    fn from(value: CheckpointMetadata) -> Self {
        Self::CheckpointMetadata(value)
    }
}

impl From<Sidecar> for Action {
    fn from(value: Sidecar) -> Self {
        Self::Sidecar(value)
    }
}

impl Hash for Add {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Add {
    /// Returns parsed statistics if present.
    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        Stats::from_json_opt(self.stats.as_deref())
    }

    pub fn into_remove(self, deletion_timestamp: i64) -> Remove {
        self.into_remove_with_options(deletion_timestamp, RemoveOptions::default())
    }

    pub fn into_remove_with_options(
        self,
        deletion_timestamp: i64,
        options: RemoveOptions,
    ) -> Remove {
        let tags = if options.include_tags {
            self.tags
        } else {
            None
        };

        Remove {
            path: self.path,
            data_change: true,
            deletion_timestamp: Some(deletion_timestamp),
            extended_file_metadata: options.extended_file_metadata,
            partition_values: Some(self.partition_values),
            size: Some(self.size),
            tags,
            deletion_vector: self.deletion_vector,
            base_row_id: self.base_row_id,
            default_row_commit_version: self.default_row_commit_version,
        }
    }
}

impl Hash for Remove {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Borrow<str> for Remove {
    fn borrow(&self) -> &str {
        self.path.as_ref()
    }
}
/// File addition action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    #[serde(with = "serde_path")]
    pub path: String,
    pub partition_values: HashMap<String, Option<String>>,
    pub size: i64,
    pub modification_time: i64,
    pub data_change: bool,
    pub stats: Option<String>,
    pub tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clustering_provider: Option<String>,
    #[serde(skip)]
    pub commit_version: Option<i64>,
    #[serde(skip)]
    pub commit_timestamp: Option<i64>,
}

/// File removal action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    #[serde(with = "serde_path")]
    pub path: String,
    pub data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_file_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct RemoveOptions {
    pub extended_file_metadata: Option<bool>,
    pub include_tags: bool,
}

impl Default for RemoveOptions {
    fn default() -> Self {
        Self {
            extended_file_metadata: None,
            include_tags: true,
        }
    }
}

/// Change data capture action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCDCFile {
    #[serde(with = "serde_path")]
    pub path: String,
    pub partition_values: HashMap<String, Option<String>>,
    pub size: i64,
    pub data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Application transaction action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub app_id: String,
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<i64>,
}

/// The isolation level applied during a transaction.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    Serializable,
    WriteSerializable,
    SnapshotIsolation,
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(DeltaTableError::generic(format!(
                "Invalid string for IsolationLevel: {s}"
            ))),
        }
    }
}

/// Commit metadata action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_blind_append: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_info: Option<String>,
    #[serde(flatten, default)]
    pub info: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_metadata: Option<String>,
}

/// Domain metadata action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainMetadata {
    pub domain: String,
    pub configuration: String,
    pub removed: bool,
}

/// Checkpoint metadata action (Delta checkpoint v2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointMetadata {
    pub flavor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Checkpoint sidecar descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sidecar {
    pub file_name: String,
    pub size_in_bytes: i64,
    pub modification_time: i64,
    #[serde(rename = "type")]
    pub sidecar_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

impl TryFrom<Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: Add) -> DeltaResult<Self> {
        (&value).try_into()
    }
}

impl TryFrom<&Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: &Add) -> DeltaResult<Self> {
        let last_modified =
            DateTime::from_timestamp_millis(value.modification_time).ok_or_else(|| {
                DeltaTableError::generic(format!(
                    "invalid modification_time: {:?}",
                    value.modification_time
                ))
            })?;

        Ok(Self {
            location: Path::parse(&value.path)?,
            last_modified,
            size: value.size as u64,
            e_tag: None,
            version: None,
        })
    }
}

/// Serde helpers for encoding/decoding log paths.
pub(crate) mod serde_path {
    use std::str::Utf8Error;

    use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        decode_path(&s).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = encode_path(value);
        String::serialize(&encoded, serializer)
    }

    const INVALID: &AsciiSet = &CONTROLS
        .add(b'\\')
        .add(b'{')
        .add(b'^')
        .add(b'}')
        .add(b'%')
        .add(b'`')
        .add(b']')
        .add(b'"')
        .add(b'>')
        .add(b'[')
        .add(b'<')
        .add(b'#')
        .add(b'|')
        .add(b'\r')
        .add(b'\n')
        .add(b'*')
        .add(b'?');

    fn encode_path(path: &str) -> String {
        percent_encode(path.as_bytes(), INVALID).to_string()
    }

    pub fn decode_path(path: &str) -> Result<String, Utf8Error> {
        Ok(percent_decode_str(path).decode_utf8()?.to_string())
    }
}
