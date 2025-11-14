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
use std::hash::{Hash, Hasher};
use std::mem::take;

use chrono::DateTime;
use delta_kernel::actions::{Metadata, Protocol};
use deltalake::errors::{DeltaResult, DeltaTableError};
pub use deltalake::kernel::models::DeletionVectorDescriptor;
use deltalake::kernel::{
    Action as DeltaRsAction, Add as DeltaRsAdd, AddCDCFile as DeltaRsAddCDCFile,
    CheckpointMetadata as DeltaRsCheckpointMetadata, CommitInfo as DeltaRsCommitInfo,
    DomainMetadata as DeltaRsDomainMetadata, IsolationLevel as DeltaRsIsolationLevel,
    Remove as DeltaRsRemove, Sidecar as DeltaRsSidecar, Transaction as DeltaRsTransaction,
};
use object_store::path::Path;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

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
        self.stats
            .as_ref()
            .map(|stats| serde_json::from_str::<PartialStats>(stats).map(|mut ps| ps.as_stats()))
            .transpose()
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

impl From<&Add> for DeltaRsAdd {
    fn from(value: &Add) -> Self {
        DeltaRsAdd {
            path: value.path.clone(),
            partition_values: value.partition_values.clone(),
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats.clone(),
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone(),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider.clone(),
        }
    }
}

impl From<Add> for DeltaRsAdd {
    fn from(value: Add) -> Self {
        DeltaRsAdd {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

impl From<&DeltaRsAdd> for Add {
    fn from(value: &DeltaRsAdd) -> Self {
        Add {
            path: value.path.clone(),
            partition_values: value.partition_values.clone(),
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats.clone(),
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone(),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider.clone(),
        }
    }
}

impl From<DeltaRsAdd> for Add {
    fn from(value: DeltaRsAdd) -> Self {
        Add {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

impl From<&Remove> for DeltaRsRemove {
    fn from(value: &Remove) -> Self {
        DeltaRsRemove {
            path: value.path.clone(),
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values.clone(),
            size: value.size,
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone(),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<Remove> for DeltaRsRemove {
    fn from(value: Remove) -> Self {
        DeltaRsRemove {
            path: value.path,
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            tags: value.tags,
            deletion_vector: value.deletion_vector,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<&DeltaRsRemove> for Remove {
    fn from(value: &DeltaRsRemove) -> Self {
        Remove {
            path: value.path.clone(),
            data_change: value.data_change,
            deletion_timestamp: value.deletion_timestamp,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values.clone(),
            size: value.size,
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone(),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<DeltaRsRemove> for Remove {
    fn from(value: DeltaRsRemove) -> Self {
        Remove {
            path: value.path,
            data_change: value.data_change,
            deletion_timestamp: value.deletion_timestamp,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            tags: value.tags,
            deletion_vector: value.deletion_vector,
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<AddCDCFile> for DeltaRsAddCDCFile {
    fn from(value: AddCDCFile) -> Self {
        DeltaRsAddCDCFile {
            path: value.path,
            size: value.size,
            partition_values: value.partition_values,
            data_change: value.data_change,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsAddCDCFile> for AddCDCFile {
    fn from(value: DeltaRsAddCDCFile) -> Self {
        AddCDCFile {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            data_change: value.data_change,
            tags: value.tags,
        }
    }
}

impl From<Transaction> for DeltaRsTransaction {
    fn from(value: Transaction) -> Self {
        DeltaRsTransaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<DeltaRsTransaction> for Transaction {
    fn from(value: DeltaRsTransaction) -> Self {
        Transaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<CommitInfo> for DeltaRsCommitInfo {
    fn from(value: CommitInfo) -> Self {
        DeltaRsCommitInfo {
            timestamp: value.timestamp,
            user_id: value.user_id,
            user_name: value.user_name,
            operation: value.operation,
            operation_parameters: value.operation_parameters,
            read_version: value.read_version,
            isolation_level: value.isolation_level,
            is_blind_append: value.is_blind_append,
            engine_info: value.engine_info,
            info: value.info,
            user_metadata: value.user_metadata,
        }
    }
}

impl From<DeltaRsCommitInfo> for CommitInfo {
    fn from(value: DeltaRsCommitInfo) -> Self {
        CommitInfo {
            timestamp: value.timestamp,
            user_id: value.user_id,
            user_name: value.user_name,
            operation: value.operation,
            operation_parameters: value.operation_parameters,
            read_version: value.read_version,
            isolation_level: value.isolation_level,
            is_blind_append: value.is_blind_append,
            engine_info: value.engine_info,
            info: value.info,
            user_metadata: value.user_metadata,
        }
    }
}

impl From<DomainMetadata> for DeltaRsDomainMetadata {
    fn from(value: DomainMetadata) -> Self {
        DeltaRsDomainMetadata {
            domain: value.domain,
            configuration: value.configuration,
            removed: value.removed,
        }
    }
}

impl From<DeltaRsDomainMetadata> for DomainMetadata {
    fn from(value: DeltaRsDomainMetadata) -> Self {
        DomainMetadata {
            domain: value.domain,
            configuration: value.configuration,
            removed: value.removed,
        }
    }
}

impl From<CheckpointMetadata> for DeltaRsCheckpointMetadata {
    fn from(value: CheckpointMetadata) -> Self {
        DeltaRsCheckpointMetadata {
            flavor: value.flavor,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsCheckpointMetadata> for CheckpointMetadata {
    fn from(value: DeltaRsCheckpointMetadata) -> Self {
        CheckpointMetadata {
            flavor: value.flavor,
            tags: value.tags,
        }
    }
}

impl From<Sidecar> for DeltaRsSidecar {
    fn from(value: Sidecar) -> Self {
        DeltaRsSidecar {
            file_name: value.file_name,
            size_in_bytes: value.size_in_bytes,
            modification_time: value.modification_time,
            sidecar_type: value.sidecar_type,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsSidecar> for Sidecar {
    fn from(value: DeltaRsSidecar) -> Self {
        Sidecar {
            file_name: value.file_name,
            size_in_bytes: value.size_in_bytes,
            modification_time: value.modification_time,
            sidecar_type: value.sidecar_type,
            tags: value.tags,
        }
    }
}

impl From<Action> for DeltaRsAction {
    fn from(value: Action) -> Self {
        match value {
            Action::Metadata(m) => DeltaRsAction::Metadata(m),
            Action::Protocol(p) => DeltaRsAction::Protocol(p),
            Action::Add(a) => DeltaRsAction::Add(a.into()),
            Action::Remove(r) => DeltaRsAction::Remove(r.into()),
            Action::Cdc(c) => DeltaRsAction::Cdc(c.into()),
            Action::Txn(t) => DeltaRsAction::Txn(t.into()),
            Action::CommitInfo(c) => DeltaRsAction::CommitInfo(c.into()),
            Action::DomainMetadata(d) => DeltaRsAction::DomainMetadata(d.into()),
            Action::CheckpointMetadata(_) => {
                panic!("CheckpointMetadata actions are not supported in delta-rs conversions")
            }
            Action::Sidecar(_) => {
                panic!("Sidecar actions are not supported in delta-rs conversions")
            }
        }
    }
}

impl From<&Action> for DeltaRsAction {
    fn from(value: &Action) -> Self {
        match value {
            Action::Metadata(m) => DeltaRsAction::Metadata(m.clone()),
            Action::Protocol(p) => DeltaRsAction::Protocol(p.clone()),
            Action::Add(a) => DeltaRsAction::Add(a.into()),
            Action::Remove(r) => DeltaRsAction::Remove(r.into()),
            Action::Cdc(c) => DeltaRsAction::Cdc(c.clone().into()),
            Action::Txn(t) => DeltaRsAction::Txn(t.clone().into()),
            Action::CommitInfo(c) => DeltaRsAction::CommitInfo(c.clone().into()),
            Action::DomainMetadata(d) => DeltaRsAction::DomainMetadata(d.clone().into()),
            Action::CheckpointMetadata(_) => {
                panic!("CheckpointMetadata actions are not supported in delta-rs conversions")
            }
            Action::Sidecar(_) => {
                panic!("Sidecar actions are not supported in delta-rs conversions")
            }
        }
    }
}

impl From<DeltaRsAction> for Action {
    fn from(value: DeltaRsAction) -> Self {
        match value {
            DeltaRsAction::Metadata(m) => Action::Metadata(m),
            DeltaRsAction::Protocol(p) => Action::Protocol(p),
            DeltaRsAction::Add(a) => Action::Add(a.into()),
            DeltaRsAction::Remove(r) => Action::Remove(r.into()),
            DeltaRsAction::Cdc(c) => Action::Cdc(c.into()),
            DeltaRsAction::Txn(t) => Action::Txn(t.into()),
            DeltaRsAction::CommitInfo(c) => Action::CommitInfo(c.into()),
            DeltaRsAction::DomainMetadata(d) => Action::DomainMetadata(d.into()),
        }
    }
}

impl From<&DeltaRsAction> for Action {
    fn from(value: &DeltaRsAction) -> Self {
        match value {
            DeltaRsAction::Metadata(m) => Action::Metadata(m.clone()),
            DeltaRsAction::Protocol(p) => Action::Protocol(p.clone()),
            DeltaRsAction::Add(a) => Action::Add(a.into()),
            DeltaRsAction::Remove(r) => Action::Remove(r.into()),
            DeltaRsAction::Cdc(c) => Action::Cdc(c.clone().into()),
            DeltaRsAction::Txn(t) => Action::Txn(t.clone().into()),
            DeltaRsAction::CommitInfo(c) => Action::CommitInfo(c.clone().into()),
            DeltaRsAction::DomainMetadata(d) => Action::DomainMetadata(d.clone().into()),
        }
    }
}

/// Column statistics stored in `Stats`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    Column(HashMap<String, ColumnValueStat>),
    Value(serde_json::Value),
}

impl ColumnValueStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&serde_json::Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

/// Column null-count statistics stored in `Stats`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    Column(HashMap<String, ColumnCountStat>),
    Value(i64),
}

impl ColumnCountStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<i64> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

/// Statistics associated with an Add action.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub num_records: i64,
    pub min_values: HashMap<String, ColumnValueStat>,
    pub max_values: HashMap<String, ColumnValueStat>,
    pub null_count: HashMap<String, ColumnCountStat>,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct PartialStats {
    pub num_records: i64,
    pub min_values: Option<HashMap<String, ColumnValueStat>>,
    pub max_values: Option<HashMap<String, ColumnValueStat>>,
    pub null_count: Option<HashMap<String, ColumnCountStat>>,
}

impl PartialStats {
    fn as_stats(&mut self) -> Stats {
        Stats {
            num_records: self.num_records,
            min_values: take(&mut self.min_values).unwrap_or_default(),
            max_values: take(&mut self.max_values).unwrap_or_default(),
            null_count: take(&mut self.null_count).unwrap_or_default(),
        }
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
    pub isolation_level: Option<DeltaRsIsolationLevel>,
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
        let last_modified = DateTime::from_timestamp_millis(value.modification_time).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                value.modification_time
            )),
        )?;

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
