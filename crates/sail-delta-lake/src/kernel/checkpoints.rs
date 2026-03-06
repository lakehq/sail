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
use uuid::Uuid;

use crate::kernel::{DeltaResult, DeltaTableError};
use crate::spec::{
    protocol_from_checkpoint, protocol_to_checkpoint, Action, Add, CheckpointActionRow,
    LastCheckpointHint, Metadata, Protocol, Remove, Transaction,
};
use crate::storage::{get_actions, LogStore};

const DELTA_LOG_FOLDER: &str = "_delta_log";
static DELTA_LOG_REGEX: LazyLock<Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.json$"));
// Multipart checkpoints are deprecated in the Delta protocol.
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

#[derive(Debug, Clone)]
pub(crate) struct ReplayedTableState {
    pub version: i64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub txns: HashMap<String, Transaction>,
    pub adds: Vec<Add>,
    pub removes: Vec<Remove>,
    pub commit_timestamps: BTreeMap<i64, i64>,
}

fn encode_checkpoint_rows(rows: &Vec<CheckpointActionRow>) -> DeltaResult<RecordBatch> {
    use serde_arrow::schema::SchemaLike;

    let mut samples = rows.clone();
    samples.push(checkpoint_schema_probe_row()?);
    let fields = Vec::<FieldRef>::from_samples(&samples, checkpoint_tracing_options()?)
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

fn checkpoint_tracing_options() -> DeltaResult<serde_arrow::schema::TracingOptions> {
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
        .map_err(DeltaTableError::generic_err)?
        .overwrite("add.tags", map_utf8_utf8("tags", true, true))
        .map_err(DeltaTableError::generic_err)?
        .overwrite(
            "remove.partitionValues",
            map_utf8_utf8("partitionValues", true, false),
        )
        .map_err(DeltaTableError::generic_err)?
        .overwrite("remove.tags", map_utf8_utf8("tags", true, false))
        .map_err(DeltaTableError::generic_err)?
        .overwrite(
            "metaData.configuration",
            map_utf8_utf8("configuration", false, true),
        )
        .map_err(DeltaTableError::generic_err)?
        .overwrite(
            "metaData.format.options",
            map_utf8_utf8("options", false, true),
        )
        .map_err(DeltaTableError::generic_err)
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
            size: Some(checkpoint_row_count),
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

/// Load the reconciled table state at a specific version by replaying checkpoint + commits.
pub(crate) async fn load_replayed_table_state(
    version: i64,
    log_store: &dyn LogStore,
) -> DeltaResult<ReplayedTableState> {
    if version < 0 {
        return Err(DeltaTableError::generic(format!(
            "Cannot load table state for negative version: {version}"
        )));
    }

    let store = log_store.object_store(None);
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
        store,
        commit_entries.clone(),
        start_commit_version,
        version,
    )
    .await?;

    let protocol = state
        .protocol
        .ok_or_else(|| DeltaTableError::generic("Cannot load table state without protocol"))?;
    let metadata = state
        .metadata
        .ok_or_else(|| DeltaTableError::generic("Cannot load table state without metadata"))?;
    let txns = state.txns;
    let adds = state
        .adds
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let removes = state
        .removes
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let commit_timestamps = commit_entries
        .into_iter()
        .filter(|(v, _)| *v >= start_commit_version && *v <= version)
        .map(|(v, meta)| (v, meta.last_modified.timestamp_millis()))
        .collect::<BTreeMap<_, _>>();

    Ok(ReplayedTableState {
        version,
        protocol,
        metadata,
        txns,
        adds,
        removes,
        commit_timestamps,
    })
}

/// Resolve the latest table version that can be replayed from `_delta_log`.
///
/// This includes both commit JSON files and checkpoint parquet files. We rely on this when
/// loading snapshots because commit JSON files can be pruned while checkpoints are retained.
pub(crate) async fn latest_replayable_version(log_store: &dyn LogStore) -> DeltaResult<i64> {
    let store = log_store.object_store(None);
    let log_entries = store
        .list(Some(&Path::from(DELTA_LOG_FOLDER)))
        .try_collect::<Vec<_>>()
        .await?;
    let delta_log_pattern = delta_log_regex()?;
    let checkpoint_pattern = checkpoint_regex()?;

    let latest = log_entries
        .iter()
        .filter_map(|meta| {
            parse_version(delta_log_pattern, &meta.location)
                .or_else(|| parse_version(checkpoint_pattern, &meta.location))
        })
        .max();

    latest.ok_or(crate::error::DeltaError::MissingVersion)
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

    use super::{decode_checkpoint_rows, encode_checkpoint_rows, ReconciledCheckpointState};
    use crate::error::DeltaResult;
    use crate::spec::{Action, Add, CheckpointActionRow, Remove};

    #[test]
    fn checkpoint_row_roundtrip_preserves_add_path() -> DeltaResult<()> {
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
        let batch = encode_checkpoint_rows(&rows)?;
        let decoded = decode_checkpoint_rows(&batch)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded
                .first()
                .and_then(|row| row.add.as_ref())
                .map(|add| add.path.as_str()),
            Some("part-000.parquet")
        );
        Ok(())
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
