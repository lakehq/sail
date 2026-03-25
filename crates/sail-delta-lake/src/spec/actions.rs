// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright 2025-2026 LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use chrono::DateTime;
use object_store::path::Path;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use crate::spec::statistics::Stats;
use crate::spec::{DeltaError as DeltaTableError, DeltaResult, IsolationLevel, Metadata, Protocol};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs#L694-L1065>
// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/mod.rs#L18-L27>
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
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
pub enum Action {
    #[serde(rename = "metaData", alias = "metadata")]
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
            stats: None,
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
    #[serde(with = "crate::spec::utils::serde_path")]
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
    #[serde(with = "crate::spec::utils::serde_path")]
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
    pub stats: Option<String>,
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
    #[serde(with = "crate::spec::utils::serde_path")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_commit_timestamp: Option<i64>,
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
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Checkpoint sidecar descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sidecar {
    #[serde(with = "crate::spec::utils::serde_path")]
    pub path: String,
    pub size_in_bytes: i64,
    pub modification_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Actions that are valid in JSON commit entries only.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CommitAction {
    #[serde(rename = "metaData", alias = "metadata")]
    Metadata(Metadata),
    Protocol(Protocol),
    Add(Add),
    Remove(Remove),
    Cdc(AddCDCFile),
    Txn(Transaction),
    CommitInfo(CommitInfo),
    DomainMetadata(DomainMetadata),
}

impl From<CommitAction> for Action {
    fn from(action: CommitAction) -> Self {
        match action {
            CommitAction::Metadata(m) => Action::Metadata(m),
            CommitAction::Protocol(p) => Action::Protocol(p),
            CommitAction::Add(a) => Action::Add(a),
            CommitAction::Remove(r) => Action::Remove(r),
            CommitAction::Cdc(c) => Action::Cdc(c),
            CommitAction::Txn(t) => Action::Txn(t),
            CommitAction::CommitInfo(c) => Action::CommitInfo(c),
            CommitAction::DomainMetadata(d) => Action::DomainMetadata(d),
        }
    }
}

impl TryFrom<Action> for CommitAction {
    type Error = DeltaTableError;

    fn try_from(action: Action) -> DeltaResult<Self> {
        match action {
            Action::Metadata(m) => Ok(Self::Metadata(m)),
            Action::Protocol(p) => Ok(Self::Protocol(p)),
            Action::Add(a) => Ok(Self::Add(a)),
            Action::Remove(r) => Ok(Self::Remove(r)),
            Action::Cdc(c) => Ok(Self::Cdc(c)),
            Action::Txn(t) => Ok(Self::Txn(t)),
            Action::CommitInfo(c) => Ok(Self::CommitInfo(c)),
            Action::DomainMetadata(d) => Ok(Self::DomainMetadata(d)),
            Action::CheckpointMetadata(_) | Action::Sidecar(_) => Err(
                DeltaTableError::generic(
                    "checkpoint-only actions (CheckpointMetadata, Sidecar) are not allowed in commit files",
                ),
            ),
        }
    }
}

// Convenience `From` impls so callers can build `CommitAction`s directly from inner types.
impl From<Metadata> for CommitAction {
    fn from(v: Metadata) -> Self {
        Self::Metadata(v)
    }
}

impl From<Protocol> for CommitAction {
    fn from(v: Protocol) -> Self {
        Self::Protocol(v)
    }
}

impl From<Add> for CommitAction {
    fn from(v: Add) -> Self {
        Self::Add(v)
    }
}

impl From<AddCDCFile> for CommitAction {
    fn from(v: AddCDCFile) -> Self {
        Self::Cdc(v)
    }
}

impl From<Remove> for CommitAction {
    fn from(v: Remove) -> Self {
        Self::Remove(v)
    }
}

impl From<Transaction> for CommitAction {
    fn from(v: Transaction) -> Self {
        Self::Txn(v)
    }
}

impl From<CommitInfo> for CommitAction {
    fn from(v: CommitInfo) -> Self {
        Self::CommitInfo(v)
    }
}

impl From<DomainMetadata> for CommitAction {
    fn from(v: DomainMetadata) -> Self {
        Self::DomainMetadata(v)
    }
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
