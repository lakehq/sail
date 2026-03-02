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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/protocol/checkpoints.rs>

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use chrono::{TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::kernel::models::{
    Action, Add, DeletionVectorDescriptor, Metadata, Protocol, Remove, Transaction,
};
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::storage::{get_actions, LogStore};

const DELTA_LOG_FOLDER: &str = "_delta_log";
static DELTA_LOG_REGEX: LazyLock<Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.json$"));
static CHECKPOINT_REGEX: LazyLock<Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.checkpoint.*\.parquet$"));

fn regex_from_lazy(
    lazy: &'static LazyLock<Result<Regex, regex::Error>>,
    name: &str,
) -> DeltaResult<&'static Regex> {
    match LazyLock::force(lazy) {
        Ok(regex) => Ok(regex),
        Err(err) => Err(DeltaTableError::generic(format!(
            "Failed to compile {name} regex: {err}"
        ))),
    }
}

fn delta_log_regex() -> DeltaResult<&'static Regex> {
    regex_from_lazy(&DELTA_LOG_REGEX, "delta log")
}

fn checkpoint_regex() -> DeltaResult<&'static Regex> {
    regex_from_lazy(&CHECKPOINT_REGEX, "checkpoint")
}

fn parse_version(regex: &Regex, location: &Path) -> Option<i64> {
    regex
        .captures(location.as_ref())
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointActionRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    add: Option<CheckpointAdd>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remove: Option<CheckpointRemove>,
    #[serde(rename = "metaData", skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<CheckpointProtocol>,
    #[serde(rename = "txn", skip_serializing_if = "Option::is_none")]
    txn: Option<Transaction>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointProtocol {
    min_reader_version: i32,
    min_writer_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointDeletionVector {
    storage_type: String,
    path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointAdd {
    #[serde(with = "serde_path_compat")]
    path: String,
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<CheckpointDeletionVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_row_commit_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    clustering_provider: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointRemove {
    #[serde(with = "serde_path_compat")]
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_timestamp: Option<i64>,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    extended_file_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_values: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<CheckpointDeletionVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_row_commit_version: Option<i64>,
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

fn protocol_to_checkpoint(protocol: Protocol) -> DeltaResult<CheckpointProtocol> {
    let value = serde_json::to_value(protocol)?;
    serde_json::from_value(value).map_err(DeltaTableError::generic_err)
}

fn protocol_from_checkpoint(protocol: CheckpointProtocol) -> DeltaResult<Protocol> {
    let value = serde_json::to_value(protocol)?;
    serde_json::from_value(value).map_err(DeltaTableError::generic_err)
}

mod serde_path_compat {
    use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = percent_encode(value.as_bytes(), INVALID).to_string();
        String::serialize(&encoded, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        percent_decode_str(&s)
            .decode_utf8()
            .map(|v| v.to_string())
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Default)]
struct ReconciledCheckpointState {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    txns: HashMap<String, Transaction>,
    adds: HashMap<String, Add>,
    removes: HashMap<String, Remove>,
}

impl ReconciledCheckpointState {
    fn apply_action(&mut self, action: Action) {
        match action {
            Action::Protocol(protocol) => {
                self.protocol = Some(protocol);
            }
            Action::Metadata(metadata) => {
                self.metadata = Some(metadata);
            }
            Action::Txn(txn) => {
                self.txns.insert(txn.app_id.clone(), txn);
            }
            Action::Add(add) => {
                self.removes.remove(&add.path);
                self.adds.insert(add.path.clone(), add);
            }
            Action::Remove(remove) => {
                self.adds.remove(&remove.path);
                self.removes.insert(remove.path.clone(), remove);
            }
            Action::CommitInfo(_)
            | Action::Cdc(_)
            | Action::DomainMetadata(_)
            | Action::CheckpointMetadata(_)
            | Action::Sidecar(_) => {}
        }
    }

    fn apply_checkpoint_row(&mut self, row: CheckpointActionRow) -> DeltaResult<()> {
        if let Some(protocol) = row.protocol {
            self.protocol = Some(protocol_from_checkpoint(protocol)?);
        }
        if let Some(metadata) = row.metadata {
            self.metadata = Some(metadata);
        }
        if let Some(txn) = row.txn {
            self.txns.insert(txn.app_id.clone(), txn);
        }
        if let Some(add) = row.add {
            let add: Add = add.try_into()?;
            self.removes.remove(&add.path);
            self.adds.insert(add.path.clone(), add);
        }
        if let Some(remove) = row.remove {
            let remove: Remove = remove.try_into()?;
            self.adds.remove(&remove.path);
            self.removes.insert(remove.path.clone(), remove);
        }
        Ok(())
    }

    fn into_checkpoint_rows(self) -> DeltaResult<(Vec<CheckpointActionRow>, i64)> {
        let protocol = self.protocol.ok_or_else(|| {
            DeltaTableError::generic("Cannot create checkpoint without protocol action")
        })?;
        let metadata = self.metadata.ok_or_else(|| {
            DeltaTableError::generic("Cannot create checkpoint without metadata action")
        })?;

        let add_count = i64::try_from(self.adds.len())
            .map_err(|_| DeltaTableError::generic("add action count overflow"))?;

        let mut rows = Vec::with_capacity(
            2usize
                .saturating_add(self.txns.len())
                .saturating_add(self.removes.len())
                .saturating_add(self.adds.len()),
        );
        rows.push(CheckpointActionRow {
            protocol: Some(protocol_to_checkpoint(protocol)?),
            ..Default::default()
        });
        rows.push(CheckpointActionRow {
            metadata: Some(metadata),
            ..Default::default()
        });

        for (_, txn) in self.txns.into_iter().collect::<BTreeMap<_, _>>() {
            rows.push(CheckpointActionRow {
                txn: Some(txn),
                ..Default::default()
            });
        }
        for (_, remove) in self.removes.into_iter().collect::<BTreeMap<_, _>>() {
            rows.push(CheckpointActionRow {
                remove: Some(remove.into()),
                ..Default::default()
            });
        }
        for (_, add) in self.adds.into_iter().collect::<BTreeMap<_, _>>() {
            rows.push(CheckpointActionRow {
                add: Some(add.into()),
                ..Default::default()
            });
        }

        Ok((rows, add_count))
    }
}

fn encode_checkpoint_rows(rows: &Vec<CheckpointActionRow>) -> DeltaResult<RecordBatch> {
    use serde_arrow::schema::SchemaLike;

    let mut samples = rows.clone();
    samples.push(checkpoint_schema_probe_row()?);
    let fields = Vec::<FieldRef>::from_samples(&samples, checkpoint_tracing_options())
        .map_err(DeltaTableError::generic_err)?;
    serde_arrow::to_record_batch(&fields, rows).map_err(DeltaTableError::generic_err)
}

fn decode_checkpoint_rows(batch: &RecordBatch) -> DeltaResult<Vec<CheckpointActionRow>> {
    serde_arrow::from_record_batch(batch).map_err(DeltaTableError::generic_err)
}

fn checkpoint_schema_probe_row() -> DeltaResult<CheckpointActionRow> {
    let value = serde_json::json!({
        "add": {
            "path": "_probe_add.parquet",
            "partitionValues": { "p": "x" },
            "size": 0,
            "modificationTime": 0,
            "dataChange": true,
            "stats": "{}",
            "tags": { "t": "x" },
            "deletionVector": {
                "storageType": "u",
                "pathOrInlineDv": "dv.bin",
                "offset": 0,
                "sizeInBytes": 1,
                "cardinality": 1
            },
            "baseRowId": 0,
            "defaultRowCommitVersion": 0,
            "clusteringProvider": "none"
        },
        "remove": {
            "path": "_probe_remove.parquet",
            "dataChange": true,
            "deletionTimestamp": 0,
            "extendedFileMetadata": true,
            "partitionValues": { "p": "x" },
            "size": 0,
            "stats": "{}",
            "tags": { "t": "x" },
            "deletionVector": {
                "storageType": "u",
                "pathOrInlineDv": "dv.bin",
                "offset": 0,
                "sizeInBytes": 1,
                "cardinality": 1
            },
            "baseRowId": 0,
            "defaultRowCommitVersion": 0
        },
        "metaData": {
            "id": "probe",
            "name": "probe",
            "description": "probe",
            "format": { "provider": "parquet", "options": { "k": "v" } },
            "schemaString": r#"{"type":"struct","fields":[]}"#,
            "partitionColumns": ["p"],
            "configuration": { "delta.appendOnly": "false" },
            "createdTime": 0
        },
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        },
        "txn": {
            "appId": "probe",
            "version": 0,
            "lastUpdated": 0
        }
    });
    serde_json::from_value(value).map_err(DeltaTableError::generic_err)
}

fn checkpoint_tracing_options() -> serde_arrow::schema::TracingOptions {
    fn map_utf8_utf8(field_name: &str, nullable: bool, value_nullable: bool) -> Field {
        let entry_struct = ArrowDataType::Struct(
            vec![
                std::sync::Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                std::sync::Arc::new(Field::new("value", ArrowDataType::Utf8, value_nullable)),
            ]
            .into(),
        );
        Field::new(
            field_name,
            ArrowDataType::Map(
                std::sync::Arc::new(Field::new("key_value", entry_struct, false)),
                false,
            ),
            nullable,
        )
    }

    serde_arrow::schema::TracingOptions::default()
        .map_as_struct(false)
        .allow_null_fields(true)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        .overwrite(
            "add.partitionValues",
            map_utf8_utf8("partitionValues", false, true),
        )
        .expect("checkpoint tracing overwrite for add.partitionValues should be valid")
        .overwrite("add.tags", map_utf8_utf8("tags", true, true))
        .expect("checkpoint tracing overwrite for add.tags should be valid")
        .overwrite(
            "remove.partitionValues",
            map_utf8_utf8("partitionValues", true, false),
        )
        .expect("checkpoint tracing overwrite for remove.partitionValues should be valid")
        .overwrite("remove.tags", map_utf8_utf8("tags", true, false))
        .expect("checkpoint tracing overwrite for remove.tags should be valid")
        .overwrite(
            "metaData.configuration",
            map_utf8_utf8("configuration", false, true),
        )
        .expect("checkpoint tracing overwrite for metadata.configuration should be valid")
        .overwrite(
            "metaData.format.options",
            map_utf8_utf8("options", false, true),
        )
        .expect("checkpoint tracing overwrite for metadata.format.options should be valid")
}

fn find_union_path_in_type(dtype: &ArrowDataType, path: &str) -> Option<String> {
    match dtype {
        ArrowDataType::Union(_, _) => Some(path.to_string()),
        ArrowDataType::Struct(fields) => fields.iter().find_map(|f| {
            let child_path = format!("{path}.{}", f.name());
            find_union_path_in_type(f.data_type(), &child_path)
        }),
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let child_path = format!("{path}.{}", field.name());
            find_union_path_in_type(field.data_type(), &child_path)
        }
        ArrowDataType::Map(field, _) => {
            let child_path = format!("{path}.{}", field.name());
            find_union_path_in_type(field.data_type(), &child_path)
        }
        _ => None,
    }
}

fn ensure_schema_supported_for_parquet(batch: &RecordBatch) -> DeltaResult<()> {
    for field in batch.schema().fields() {
        let path = field.name().to_string();
        if let Some(union_path) = find_union_path_in_type(field.data_type(), &path) {
            return Err(DeltaTableError::generic(format!(
                "Unsupported checkpoint schema contains Union type at '{union_path}'"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LastCheckpointHint {
    version: i64,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_of_add_files: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<std::collections::HashMap<String, String>>,
}

struct CheckpointManager<'a> {
    log_store: &'a dyn LogStore,
    operation_id: Uuid,
}

impl<'a> CheckpointManager<'a> {
    fn new(log_store: &'a dyn LogStore, operation_id: Uuid) -> Self {
        Self {
            log_store,
            operation_id,
        }
    }

    async fn create_checkpoint(&self, version: i64) -> DeltaResult<()> {
        if version < 0 {
            return Err(DeltaTableError::generic(format!(
                "Cannot create checkpoint for negative version: {version}"
            )));
        }

        let store = self.log_store.object_store(Some(self.operation_id));
        let log_entries = store
            .list(Some(&Path::from(DELTA_LOG_FOLDER)))
            .try_collect::<Vec<_>>()
            .await?;
        let delta_log_pattern = delta_log_regex()?;
        let checkpoint_pattern = checkpoint_regex()?;
        let mut commit_entries: Vec<(i64, ObjectMeta)> = Vec::new();
        let mut checkpoint_entries: Vec<(i64, ObjectMeta)> = Vec::new();
        for meta in log_entries {
            if let Some(v) = parse_version(delta_log_pattern, &meta.location) {
                if v <= version {
                    commit_entries.push((v, meta));
                }
                continue;
            }
            if let Some(v) = parse_version(checkpoint_pattern, &meta.location) {
                if v <= version {
                    checkpoint_entries.push((v, meta));
                }
            }
        }
        commit_entries.sort_by(|(av, _), (bv, _)| av.cmp(bv));
        checkpoint_entries.sort_by(|(av, _), (bv, _)| av.cmp(bv));

        let mut state = ReconciledCheckpointState::default();
        let start_commit_version = if let Some((cp_ver, cp_meta)) = checkpoint_entries.pop() {
            let rows = read_checkpoint_rows_from_parquet(store.clone(), cp_meta).await?;
            for row in rows {
                state.apply_checkpoint_row(row)?;
            }
            cp_ver.saturating_add(1)
        } else {
            0
        };
        replay_commit_actions(
            &mut state,
            store.clone(),
            commit_entries,
            start_commit_version,
            version,
        )
        .await?;
        let (rows, checkpoint_add_count) = state.into_checkpoint_rows()?;
        let checkpoint_row_count = i64::try_from(rows.len())
            .map_err(|_| DeltaTableError::generic("checkpoint action count overflow"))?;
        let checkpoint_batch = encode_checkpoint_rows(&rows)?;
        ensure_schema_supported_for_parquet(&checkpoint_batch)?;

        let cp_path = Path::from(format!(
            "{DELTA_LOG_FOLDER}/{version:020}.checkpoint.parquet"
        ));
        let object_store_writer = ParquetObjectWriter::new(store.clone(), cp_path.clone());
        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, checkpoint_batch.schema(), None)
                .map_err(DeltaTableError::generic_err)?;
        writer
            .write(&checkpoint_batch)
            .await
            .map_err(DeltaTableError::generic_err)?;
        let _ = writer.close().await.map_err(DeltaTableError::generic_err)?;
        let file_meta = store.head(&cp_path).await?;
        let last_checkpoint_path = Path::from(format!("{DELTA_LOG_FOLDER}/_last_checkpoint"));
        let hint = LastCheckpointHint {
            version,
            size: checkpoint_row_count,
            parts: None,
            size_in_bytes: Some(file_meta.size as i64),
            num_of_add_files: Some(checkpoint_add_count),
            checkpoint_schema: None,
            checksum: None,
            tags: None,
        };
        let hint_bytes = serde_json::to_vec(&hint).map_err(DeltaTableError::generic_err)?;
        store.put(&last_checkpoint_path, hint_bytes.into()).await?;

        Ok(())
    }
}

async fn replay_commit_actions(
    state: &mut ReconciledCheckpointState,
    root_store: std::sync::Arc<dyn ObjectStore>,
    commit_entries: Vec<(i64, ObjectMeta)>,
    start_version: i64,
    end_version: i64,
) -> DeltaResult<()> {
    if start_version > end_version {
        return Ok(());
    }

    let mut expected_version = start_version;
    for (version, meta) in commit_entries {
        if version < start_version || version > end_version {
            continue;
        }
        if version != expected_version {
            return Err(DeltaTableError::generic(format!(
                "Missing commit file while building checkpoint: expected version {expected_version}, found {version}"
            )));
        }
        let bytes = root_store.get(&meta.location).await?.bytes().await?;
        let actions = get_actions(version, &bytes)?;
        for action in actions {
            state.apply_action(action);
        }
        expected_version = expected_version.saturating_add(1);
    }

    if expected_version.saturating_sub(1) != end_version {
        return Err(DeltaTableError::generic(format!(
            "Missing commit file while building checkpoint: expected final version {end_version}, replay reached {}",
            expected_version.saturating_sub(1)
        )));
    }
    Ok(())
}

async fn read_checkpoint_rows_from_parquet(
    root_store: std::sync::Arc<dyn ObjectStore>,
    meta: ObjectMeta,
) -> DeltaResult<Vec<CheckpointActionRow>> {
    let bytes = root_store.get(&meta.location).await?.bytes().await?;
    tokio::task::spawn_blocking(move || {
        let mut batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(DeltaTableError::generic_err)?
            .build()
            .map_err(DeltaTableError::generic_err)?;
        let mut rows = Vec::new();
        for batch in &mut batches {
            let batch = batch.map_err(DeltaTableError::generic_err)?;
            let mut decoded = decode_checkpoint_rows(&batch)?;
            rows.append(&mut decoded);
        }
        Ok::<_, DeltaTableError>(rows)
    })
    .await
    .map_err(DeltaTableError::generic_err)?
}

/// Creates a checkpoint for the given table version.
pub(crate) async fn create_checkpoint_for(
    version: i64,
    log_store: &dyn LogStore,
    operation_id: Uuid,
) -> DeltaResult<()> {
    CheckpointManager::new(log_store, operation_id)
        .create_checkpoint(version)
        .await
}

/// Delete expired Delta log files up to a safe checkpoint boundary.
pub async fn cleanup_expired_logs_for(
    mut keep_version: i64,
    log_store: &dyn LogStore,
    cutoff_timestamp: i64,
    operation_id: Option<Uuid>,
) -> DeltaResult<usize> {
    debug!("called cleanup_expired_logs_for");
    let delta_log_pattern = delta_log_regex()?;
    let checkpoint_pattern = checkpoint_regex()?;
    let object_store = log_store.object_store(operation_id);
    let log_path = Path::from(DELTA_LOG_FOLDER);

    let log_entries = object_store.list(Some(&log_path)).collect::<Vec<_>>().await;

    debug!("starting keep_version: {keep_version}");
    debug!(
        "starting cutoff_timestamp: {:?}",
        Utc.timestamp_millis_opt(cutoff_timestamp).unwrap()
    );

    let min_retention_version = log_entries
        .iter()
        .filter_map(|entry| entry.as_ref().ok())
        .filter_map(|meta| {
            parse_version(delta_log_pattern, &meta.location)
                .map(|ver| (ver, meta.last_modified.timestamp_millis()))
        })
        .filter(|(_, ts)| *ts >= cutoff_timestamp)
        .map(|(ver, _)| ver)
        .min()
        .unwrap_or(keep_version);

    keep_version = keep_version.min(min_retention_version);

    let safe_checkpoint_version = log_entries
        .iter()
        .filter_map(|entry| entry.as_ref().ok())
        .filter_map(|meta| parse_version(checkpoint_pattern, &meta.location))
        .filter(|ver| *ver <= keep_version)
        .max();

    let Some(safe_checkpoint_version) = safe_checkpoint_version else {
        debug!(
            "Not cleaning metadata files, could not find a checkpoint with version <= keep_version ({keep_version})"
        );
        return Ok(0);
    };

    debug!("safe_checkpoint_version: {safe_checkpoint_version}");

    let locations = futures::stream::iter(log_entries.into_iter())
        .filter_map(|meta| async move {
            let meta = match meta {
                Ok(m) => m,
                Err(err) => {
                    error!("Error received while cleaning up expired logs: {err:?}");
                    return None;
                }
            };

            let ts = meta.last_modified.timestamp_millis();
            let log_ver = parse_version(delta_log_pattern, &meta.location)?;

            if log_ver < safe_checkpoint_version && ts <= cutoff_timestamp {
                Some(Ok(meta.location))
            } else {
                None
            }
        })
        .boxed();

    let deleted = object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await?;

    debug!("Deleted {} expired logs", deleted.len());
    Ok(deleted.len())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        decode_checkpoint_rows, encode_checkpoint_rows, CheckpointActionRow,
        ReconciledCheckpointState,
    };
    use crate::kernel::models::{Action, Add, Remove};

    #[test]
    fn checkpoint_row_roundtrip_preserves_add_path() {
        let rows = vec![CheckpointActionRow {
            add: Some(
                Add {
                    path: "part-000.parquet".to_string(),
                    partition_values: HashMap::new(),
                    size: 10,
                    modification_time: 20,
                    data_change: true,
                    stats: None,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                    commit_version: None,
                    commit_timestamp: None,
                }
                .into(),
            ),
            ..Default::default()
        }];
        let batch = encode_checkpoint_rows(&rows).expect("encode checkpoint rows");
        let decoded = decode_checkpoint_rows(&batch).expect("decode checkpoint rows");
        assert_eq!(decoded.len(), 1);
        let add = decoded[0].add.as_ref().expect("add row present");
        assert_eq!(add.path, "part-000.parquet");
    }

    #[test]
    fn reconciled_state_remove_masks_old_add() {
        let mut state = ReconciledCheckpointState::default();
        state.apply_action(Action::Add(Add {
            path: "a.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 1,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        }));
        state.apply_action(Action::Remove(Remove {
            path: "a.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(2),
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        assert!(!state.adds.contains_key("a.parquet"));
        assert!(state.removes.contains_key("a.parquet"));
    }
}
