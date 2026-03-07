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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, LazyLock};

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

use crate::spec::{
    checkpoint_path, delta_log_root_path, last_checkpoint_path, Action, Add, CheckpointActionRow,
    DeltaError as DeltaTableError, DeltaResult, LastCheckpointHint, Metadata, Protocol, Remove,
    TableProperties, Transaction,
};
use crate::storage::{get_actions, LogStore};
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

#[derive(Debug, Clone, Copy)]
struct CheckpointRetentionTimestamps {
    deleted_file_retention_timestamp: i64,
    transaction_expiration_timestamp: i64,
}

impl CheckpointRetentionTimestamps {
    fn try_new(metadata: &Metadata, reference_timestamp: i64) -> DeltaResult<Self> {
        let table_properties = TableProperties::from(metadata.configuration().iter());
        Ok(Self {
            deleted_file_retention_timestamp: retention_cutoff_timestamp(
                reference_timestamp,
                table_properties.deleted_file_retention_duration(),
                "delta.deletedFileRetentionDuration",
            )?,
            transaction_expiration_timestamp: retention_cutoff_timestamp(
                reference_timestamp,
                table_properties.log_retention_duration(),
                "delta.logRetentionDuration",
            )?,
        })
    }
}

fn retention_cutoff_timestamp(
    reference_timestamp: i64,
    retention_duration: std::time::Duration,
    property_name: &str,
) -> DeltaResult<i64> {
    let retention_millis = i64::try_from(retention_duration.as_millis()).map_err(|_| {
        DeltaTableError::generic(format!(
            "{property_name} exceeds the supported millisecond range"
        ))
    })?;
    reference_timestamp
        .checked_sub(retention_millis)
        .ok_or_else(|| {
            DeltaTableError::generic(format!(
                "Failed to compute retention cutoff for {property_name}"
            ))
        })
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
            self.protocol = Some(protocol);
        }
        if let Some(metadata) = row.metadata {
            self.metadata = Some(metadata);
        }
        if let Some(txn) = row.txn {
            self.txns.insert(txn.app_id.clone(), txn);
        }
        if let Some(add) = row.add {
            self.removes.remove(&add.path);
            self.adds.insert(add.path.clone(), add);
        }
        if let Some(remove) = row.remove {
            self.adds.remove(&remove.path);
            self.removes.insert(remove.path.clone(), remove);
        }
        Ok(())
    }

    fn prune_expired_checkpoint_actions(&mut self, reference_timestamp: i64) -> DeltaResult<()> {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            DeltaTableError::generic("Cannot prune checkpoint actions without metadata action")
        })?;
        let retention = CheckpointRetentionTimestamps::try_new(metadata, reference_timestamp)?;

        let txns_before = self.txns.len();
        self.txns.retain(|_, txn| {
            txn.last_updated
                .map(|last_updated| last_updated > retention.transaction_expiration_timestamp)
                .unwrap_or(true)
        });

        let removes_before = self.removes.len();
        self.removes.retain(|_, remove| {
            remove
                .deletion_timestamp
                .map(|deletion_timestamp| {
                    deletion_timestamp > retention.deleted_file_retention_timestamp
                })
                .unwrap_or(true)
        });

        debug!(
            "Pruned {} expired txn actions and {} expired remove actions before checkpoint write",
            txns_before.saturating_sub(self.txns.len()),
            removes_before.saturating_sub(self.removes.len()),
        );

        Ok(())
    }

    // TODO: This batch iterator removes the single-RecordBatch peak during checkpoint writes.
    // It is only a partial mitigation: we still materialize the full reconciled table state
    // in memory before writing.
    // The long-term fix should eliminate the full-state maps and emit checkpoint rows directly
    // from reconciliation.
    // The target design is a streaming pipeline:
    //   log batches -> dedup/reconcile state -> selected checkpoint batches -> parquet writer.
    // Keep only the minimum state required for deduplication and protocol/metadata finalization.
    // Avoid building a full Vec<CheckpointActionRow> or a full map of final actions when a
    // smaller incremental state machine can produce the same checkpoint contents.
    // Writer-side batching and flushing are still useful, but they do not solve the root cause
    // if reconciliation remains fully materialized.
    fn into_checkpoint_batch_iter(
        self,
        batch_size: usize,
    ) -> DeltaResult<(CheckpointBatchIter, i64)> {
        let protocol = self.protocol.ok_or_else(|| {
            DeltaTableError::generic("Cannot create checkpoint without protocol action")
        })?;
        let metadata = self.metadata.ok_or_else(|| {
            DeltaTableError::generic("Cannot create checkpoint without metadata action")
        })?;
        if batch_size == 0 {
            return Err(DeltaTableError::generic(
                "checkpoint batch size must be positive",
            ));
        }

        let add_count = i64::try_from(self.adds.len())
            .map_err(|_| DeltaTableError::generic("add action count overflow"))?;

        Ok((
            CheckpointBatchIter {
                batch_size,
                leading_rows: VecDeque::from([
                    CheckpointActionRow {
                        protocol: Some(protocol),
                        ..Default::default()
                    },
                    CheckpointActionRow {
                        metadata: Some(metadata),
                        ..Default::default()
                    },
                ]),
                txns: self
                    .txns
                    .into_iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter(),
                removes: self
                    .removes
                    .into_iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter(),
                adds: self
                    .adds
                    .into_iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter(),
            },
            add_count,
        ))
    }
}

struct CheckpointBatchIter {
    batch_size: usize,
    leading_rows: VecDeque<CheckpointActionRow>,
    txns: std::collections::btree_map::IntoIter<String, Transaction>,
    removes: std::collections::btree_map::IntoIter<String, Remove>,
    adds: std::collections::btree_map::IntoIter<String, Add>,
}

impl CheckpointBatchIter {
    fn next_batch(&mut self) -> DeltaResult<Option<RecordBatch>> {
        let mut rows = Vec::with_capacity(self.batch_size);

        while rows.len() < self.batch_size {
            if let Some(row) = self.leading_rows.pop_front() {
                rows.push(row);
                continue;
            }
            if let Some((_, txn)) = self.txns.next() {
                rows.push(CheckpointActionRow {
                    txn: Some(txn),
                    ..Default::default()
                });
                continue;
            }
            if let Some((_, remove)) = self.removes.next() {
                rows.push(CheckpointActionRow {
                    remove: Some(remove),
                    ..Default::default()
                });
                continue;
            }
            if let Some((_, add)) = self.adds.next() {
                rows.push(CheckpointActionRow {
                    add: Some(add),
                    ..Default::default()
                });
                continue;
            }
            break;
        }

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(encode_checkpoint_rows(&rows)?))
        }
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
    let fields = checkpoint_fields();
    serde_arrow::to_record_batch(&fields, rows).map_err(DeltaTableError::generic_err)
}

fn decode_checkpoint_rows(batch: &RecordBatch) -> DeltaResult<Vec<CheckpointActionRow>> {
    serde_arrow::from_record_batch(batch).map_err(DeltaTableError::generic_err)
}

fn map_utf8_utf8_field(field_name: &str, nullable: bool, value_nullable: bool) -> FieldRef {
    let entry_struct = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("value", ArrowDataType::Utf8, value_nullable)),
        ]
        .into(),
    );

    Arc::new(Field::new(
        field_name,
        ArrowDataType::Map(
            Arc::new(Field::new("key_value", entry_struct, false)),
            false,
        ),
        nullable,
    ))
}

fn utf8_list_field(field_name: &str, nullable: bool) -> FieldRef {
    Arc::new(Field::new(
        field_name,
        ArrowDataType::List(Arc::new(Field::new("element", ArrowDataType::Utf8, true))),
        nullable,
    ))
}

fn checkpoint_fields() -> Vec<FieldRef> {
    let deletion_vector = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("storageType", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("pathOrInlineDv", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("offset", ArrowDataType::Int32, true)),
            Arc::new(Field::new("sizeInBytes", ArrowDataType::Int32, false)),
            Arc::new(Field::new("cardinality", ArrowDataType::Int64, false)),
        ]
        .into(),
    );
    let checkpoint_add = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("path", ArrowDataType::Utf8, false)),
            map_utf8_utf8_field("partitionValues", false, true),
            Arc::new(Field::new("size", ArrowDataType::Int64, false)),
            Arc::new(Field::new("modificationTime", ArrowDataType::Int64, false)),
            Arc::new(Field::new("dataChange", ArrowDataType::Boolean, false)),
            Arc::new(Field::new("stats", ArrowDataType::Utf8, true)),
            map_utf8_utf8_field("tags", true, true),
            Arc::new(Field::new("deletionVector", deletion_vector.clone(), true)),
            Arc::new(Field::new("baseRowId", ArrowDataType::Int64, true)),
            Arc::new(Field::new(
                "defaultRowCommitVersion",
                ArrowDataType::Int64,
                true,
            )),
            Arc::new(Field::new("clusteringProvider", ArrowDataType::Utf8, true)),
        ]
        .into(),
    );
    let checkpoint_remove = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("path", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("deletionTimestamp", ArrowDataType::Int64, true)),
            Arc::new(Field::new("dataChange", ArrowDataType::Boolean, false)),
            Arc::new(Field::new(
                "extendedFileMetadata",
                ArrowDataType::Boolean,
                true,
            )),
            map_utf8_utf8_field("partitionValues", true, false),
            Arc::new(Field::new("size", ArrowDataType::Int64, true)),
            Arc::new(Field::new("stats", ArrowDataType::Utf8, true)),
            map_utf8_utf8_field("tags", true, false),
            Arc::new(Field::new("deletionVector", deletion_vector, true)),
            Arc::new(Field::new("baseRowId", ArrowDataType::Int64, true)),
            Arc::new(Field::new(
                "defaultRowCommitVersion",
                ArrowDataType::Int64,
                true,
            )),
        ]
        .into(),
    );
    let format = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("provider", ArrowDataType::Utf8, false)),
            map_utf8_utf8_field("options", false, true),
        ]
        .into(),
    );
    let metadata = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("id", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(Field::new("description", ArrowDataType::Utf8, true)),
            Arc::new(Field::new("format", format, false)),
            Arc::new(Field::new("schemaString", ArrowDataType::Utf8, false)),
            utf8_list_field("partitionColumns", false),
            Arc::new(Field::new("createdTime", ArrowDataType::Int64, true)),
            map_utf8_utf8_field("configuration", false, true),
        ]
        .into(),
    );
    let protocol = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("minReaderVersion", ArrowDataType::Int32, false)),
            Arc::new(Field::new("minWriterVersion", ArrowDataType::Int32, false)),
            utf8_list_field("readerFeatures", true),
            utf8_list_field("writerFeatures", true),
        ]
        .into(),
    );
    let txn = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("appId", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("version", ArrowDataType::Int64, false)),
            Arc::new(Field::new("lastUpdated", ArrowDataType::Int64, true)),
        ]
        .into(),
    );

    vec![
        Arc::new(Field::new("add", checkpoint_add, true)),
        Arc::new(Field::new("remove", checkpoint_remove, true)),
        Arc::new(Field::new("metaData", metadata, true)),
        Arc::new(Field::new("protocol", protocol, true)),
        Arc::new(Field::new("txn", txn, true)),
    ]
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
            .list(Some(&delta_log_root_path()))
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
        state.prune_expired_checkpoint_actions(Utc::now().timestamp_millis())?;

        // Design note:
        // - This write path is intentionally batch-oriented so checkpoint output does not require a
        //   single giant RecordBatch.
        // - The root-cause goal is stronger: make checkpoint creation fully streaming from log
        //   replay through parquet writing, so memory usage scales with batch size instead of table
        //   size.
        // - If needed, this path can also add row-group tuning and early flush thresholds, but the
        //   primary objective is to remove full-state materialization before write.
        const CHECKPOINT_WRITE_BATCH_SIZE: usize = 16_384;
        let (mut checkpoint_batches, checkpoint_add_count) =
            state.into_checkpoint_batch_iter(CHECKPOINT_WRITE_BATCH_SIZE)?;

        let Some(first_batch) = checkpoint_batches.next_batch()? else {
            return Err(DeltaTableError::generic("No checkpoint rows to write"));
        };
        ensure_schema_supported_for_parquet(&first_batch)?;
        let mut checkpoint_row_count = i64::try_from(first_batch.num_rows())
            .map_err(|_| DeltaTableError::generic("checkpoint action count overflow"))?;

        let cp_path = checkpoint_path(version);
        let object_store_writer = ParquetObjectWriter::new(store.clone(), cp_path.clone());
        let mut writer = AsyncArrowWriter::try_new(object_store_writer, first_batch.schema(), None)
            .map_err(DeltaTableError::generic_err)?;
        writer
            .write(&first_batch)
            .await
            .map_err(DeltaTableError::generic_err)?;
        while let Some(batch) = checkpoint_batches.next_batch()? {
            checkpoint_row_count =
                checkpoint_row_count
                    .checked_add(i64::try_from(batch.num_rows()).map_err(|_| {
                        DeltaTableError::generic("checkpoint action count overflow")
                    })?)
                    .ok_or_else(|| DeltaTableError::generic("checkpoint action count overflow"))?;
            writer
                .write(&batch)
                .await
                .map_err(DeltaTableError::generic_err)?;
        }
        let _ = writer.close().await.map_err(DeltaTableError::generic_err)?;
        let file_meta = store.head(&cp_path).await?;
        let last_checkpoint_path = last_checkpoint_path();
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
        .list(Some(&delta_log_root_path()))
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
        .list(Some(&delta_log_root_path()))
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

    latest.ok_or(crate::spec::DeltaError::MissingVersion)
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
    let log_path = delta_log_root_path();

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

    use datafusion::arrow::datatypes::DataType as ArrowDataType;

    use super::{
        checkpoint_fields, decode_checkpoint_rows, encode_checkpoint_rows,
        ReconciledCheckpointState,
    };
    use crate::spec::{
        Action, Add, CheckpointActionRow, DataType, DeletionVectorDescriptor, DeltaResult,
        Metadata, Protocol, Remove, StorageType, StructField, StructType, TableFeature,
        Transaction,
    };

    fn test_metadata(
        configuration: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> DeltaResult<Metadata> {
        Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)])?,
            Vec::new(),
            0,
            configuration
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    #[test]
    fn checkpoint_row_roundtrip_preserves_add_path() -> DeltaResult<()> {
        let rows = vec![CheckpointActionRow {
            add: Some(Add {
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
            }),
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
    fn checkpoint_row_roundtrip_preserves_shared_protocol_and_dv_models() -> DeltaResult<()> {
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::TimestampWithoutTimezone]),
            Some(vec![TableFeature::AppendOnly, TableFeature::ColumnMapping]),
        );
        let deletion_vector = DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "encoded-dv".to_string(),
            offset: Some(12),
            size_in_bytes: 34,
            cardinality: 56,
        };
        let rows = vec![
            CheckpointActionRow {
                protocol: Some(protocol.clone()),
                ..Default::default()
            },
            CheckpointActionRow {
                add: Some(Add {
                    path: "part-001.parquet".to_string(),
                    partition_values: HashMap::new(),
                    size: 10,
                    modification_time: 20,
                    data_change: true,
                    stats: None,
                    tags: None,
                    deletion_vector: Some(deletion_vector.clone()),
                    base_row_id: Some(1),
                    default_row_commit_version: Some(2),
                    clustering_provider: Some("liquid".to_string()),
                    commit_version: None,
                    commit_timestamp: None,
                }),
                ..Default::default()
            },
            CheckpointActionRow {
                remove: Some(Remove {
                    path: "part-001.parquet".to_string(),
                    data_change: true,
                    deletion_timestamp: Some(30),
                    extended_file_metadata: Some(true),
                    partition_values: Some(HashMap::new()),
                    size: Some(10),
                    stats: Some("{\"numRecords\":1}".to_string()),
                    tags: None,
                    deletion_vector: Some(deletion_vector),
                    base_row_id: Some(1),
                    default_row_commit_version: Some(2),
                }),
                ..Default::default()
            },
        ];

        let batch = encode_checkpoint_rows(&rows)?;
        let decoded = decode_checkpoint_rows(&batch)?;

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].protocol.as_ref(), Some(&protocol));
        assert_eq!(
            decoded[1]
                .add
                .as_ref()
                .and_then(|add| add.deletion_vector.as_ref())
                .map(|dv| (&dv.storage_type, dv.path_or_inline_dv.as_str(), dv.offset)),
            Some((&StorageType::Inline, "encoded-dv", Some(12)))
        );
        assert_eq!(
            decoded[2]
                .remove
                .as_ref()
                .and_then(|remove| remove.stats.as_deref()),
            Some("{\"numRecords\":1}")
        );

        Ok(())
    }

    #[test]
    fn checkpoint_row_roundtrip_preserves_remove_stats() -> DeltaResult<()> {
        let rows = vec![CheckpointActionRow {
            remove: Some(Remove {
                path: "part-000.parquet".to_string(),
                data_change: true,
                deletion_timestamp: Some(20),
                extended_file_metadata: Some(true),
                partition_values: Some(HashMap::new()),
                size: Some(10),
                stats: Some("{\"numRecords\":1}".to_string()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
            }),
            ..Default::default()
        }];

        let batch = encode_checkpoint_rows(&rows)?;
        let decoded = decode_checkpoint_rows(&batch)?;

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded
                .first()
                .and_then(|row| row.remove.as_ref())
                .and_then(|remove| remove.stats.as_deref()),
            Some("{\"numRecords\":1}")
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
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        assert!(!state.adds.contains_key("a.parquet"));
        assert!(state.removes.contains_key("a.parquet"));
    }

    #[test]
    fn checkpoint_pruning_drops_expired_remove_and_txn_actions() -> DeltaResult<()> {
        const DAY_MILLIS: i64 = 24 * 60 * 60 * 1000;
        let now = 10 * DAY_MILLIS;

        let mut state = ReconciledCheckpointState::default();
        state.apply_action(Action::Metadata(test_metadata([
            ("delta.deletedFileRetentionDuration", "interval 7 days"),
            ("delta.logRetentionDuration", "interval 30 days"),
        ])?));
        state.apply_action(Action::Remove(Remove {
            path: "expired.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(now - 8 * DAY_MILLIS),
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        state.apply_action(Action::Remove(Remove {
            path: "fresh.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(now - 6 * DAY_MILLIS),
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        state.apply_action(Action::Remove(Remove {
            path: "unknown-ts.parquet".to_string(),
            data_change: true,
            deletion_timestamp: None,
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        state.apply_action(Action::Txn(Transaction {
            app_id: "expired-app".to_string(),
            version: 1,
            last_updated: Some(now - 31 * DAY_MILLIS),
        }));
        state.apply_action(Action::Txn(Transaction {
            app_id: "fresh-app".to_string(),
            version: 2,
            last_updated: Some(now - 29 * DAY_MILLIS),
        }));
        state.apply_action(Action::Txn(Transaction {
            app_id: "legacy-app".to_string(),
            version: 3,
            last_updated: None,
        }));

        state.prune_expired_checkpoint_actions(now)?;

        assert!(!state.removes.contains_key("expired.parquet"));
        assert!(state.removes.contains_key("fresh.parquet"));
        assert!(state.removes.contains_key("unknown-ts.parquet"));
        assert!(!state.txns.contains_key("expired-app"));
        assert!(state.txns.contains_key("fresh-app"));
        assert!(state.txns.contains_key("legacy-app"));
        Ok(())
    }

    #[test]
    fn checkpoint_pruning_uses_latest_metadata_configuration() -> DeltaResult<()> {
        const DAY_MILLIS: i64 = 24 * 60 * 60 * 1000;
        let now = 3 * DAY_MILLIS;

        let mut state = ReconciledCheckpointState::default();
        state.apply_action(Action::Metadata(test_metadata([
            ("delta.deletedFileRetentionDuration", "interval 1 day"),
            ("delta.logRetentionDuration", "interval 1 day"),
        ])?));
        state.apply_action(Action::Remove(Remove {
            path: "older-remove.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(now - 2 * DAY_MILLIS),
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }));
        state.apply_action(Action::Metadata(test_metadata([
            ("delta.deletedFileRetentionDuration", "interval 30 days"),
            ("delta.logRetentionDuration", "interval 30 days"),
        ])?));

        state.prune_expired_checkpoint_actions(now)?;

        assert!(state.removes.contains_key("older-remove.parquet"));
        Ok(())
    }

    #[test]
    fn checkpoint_schema_keeps_protocol_and_metadata_fields() {
        let fields = checkpoint_fields();
        let metadata_has_configuration = fields
            .iter()
            .find(|field| field.name() == "metaData")
            .and_then(|field| match field.data_type() {
                ArrowDataType::Struct(fields) => {
                    Some(fields.iter().any(|field| field.name() == "configuration"))
                }
                _ => None,
            });
        let protocol_has_reader_features = fields
            .iter()
            .find(|field| field.name() == "protocol")
            .and_then(|field| match field.data_type() {
                ArrowDataType::Struct(fields) => {
                    Some(fields.iter().any(|field| field.name() == "readerFeatures"))
                }
                _ => None,
            });

        assert_eq!(metadata_has_configuration, Some(true));
        assert_eq!(protocol_has_reader_features, Some(true));
    }

    #[test]
    fn checkpoint_schema_keeps_remove_stats_field() {
        let fields = checkpoint_fields();
        let remove_has_stats = fields
            .iter()
            .find(|field| field.name() == "remove")
            .and_then(|field| match field.data_type() {
                ArrowDataType::Struct(fields) => {
                    Some(fields.iter().any(|field| field.name() == "stats"))
                }
                _ => None,
            });

        assert_eq!(remove_has_stats, Some(true));
    }
}
