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
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, FieldRef};
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use uuid::Uuid;

pub(crate) use crate::delta_log::{
    latest_replayable_version, load_replayed_table_header, load_replayed_table_state,
};
use crate::delta_log::{
    list_delta_log_entries_from, parse_checkpoint_version_from_location,
    parse_commit_version_from_location, read_last_checkpoint_version_from_store,
    resolve_commit_timestamp_from_actions,
};
use crate::kernel::log_segment::ReplayedTableHeader;
use crate::spec::{
    checkpoint_path, last_checkpoint_path, Action, Add, CheckpointActionRow,
    DeltaError as DeltaTableError, DeltaResult, LastCheckpointHint, Metadata, Protocol, Remove,
    TableProperties, Transaction,
};
use crate::storage::{get_actions, LogStore};

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
pub(crate) struct ReconciledCheckpointState {
    pub(crate) protocol: Option<Protocol>,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) txns: HashMap<String, Transaction>,
    // TODO: Use `(path, dvId)` once replay is deletion-vector aware.
    pub(crate) adds: HashMap<String, Add>,
    pub(crate) removes: HashMap<String, Remove>,
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
            // TODO: Preserve DomainMetadata so VersionChecksum can emit it.
            Action::CommitInfo(_)
            | Action::Cdc(_)
            | Action::DomainMetadata(_)
            | Action::CheckpointMetadata(_)
            | Action::Sidecar(_) => {}
        }
    }

    pub(crate) fn apply_checkpoint_row(&mut self, row: CheckpointActionRow) -> DeltaResult<()> {
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

    // TODO: Make checkpoint creation fully streaming. This iterator removes the
    // single-batch peak, but the reconciled state is still fully materialized.
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

#[derive(Debug, Default)]
pub(crate) struct ReconciledHeaderState {
    pub(crate) protocol: Option<Protocol>,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) txns: HashMap<String, Transaction>,
}

impl ReconciledHeaderState {
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
            Action::Add(_)
            | Action::Remove(_)
            | Action::CommitInfo(_)
            | Action::Cdc(_)
            | Action::DomainMetadata(_)
            | Action::CheckpointMetadata(_)
            | Action::Sidecar(_) => {}
        }
    }

    pub(crate) fn apply_checkpoint_row(&mut self, row: CheckpointActionRow) {
        if let Some(protocol) = row.protocol {
            self.protocol = Some(protocol);
        }
        if let Some(metadata) = row.metadata {
            self.metadata = Some(metadata);
        }
        if let Some(txn) = row.txn {
            self.txns.insert(txn.app_id.clone(), txn);
        }
    }

    pub(crate) fn from_header(header: &ReplayedTableHeader) -> Self {
        Self {
            protocol: Some(header.protocol.clone()),
            metadata: Some(header.metadata.clone()),
            txns: header.txns.as_ref().clone(),
        }
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
    let fields = checkpoint_fields()?;
    serde_arrow::to_record_batch(&fields, rows).map_err(DeltaTableError::generic_err)
}

pub(crate) fn decode_checkpoint_rows(batch: &RecordBatch) -> DeltaResult<Vec<CheckpointActionRow>> {
    serde_arrow::from_record_batch(batch).map_err(DeltaTableError::generic_err)
}

fn checkpoint_fields() -> DeltaResult<Vec<FieldRef>> {
    let schema = CheckpointActionRow::struct_type();
    schema
        .fields()
        .map(|field| {
            datafusion::arrow::datatypes::Field::try_from(field)
                .map(|f| Arc::new(f) as FieldRef)
                .map_err(|e| {
                    DeltaTableError::generic(format!(
                        "checkpoint schema should convert to Arrow: {e}"
                    ))
                })
        })
        .collect()
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
        let offset_version = read_last_checkpoint_version_from_store(store.clone()).await;
        let offset_version = offset_version
            .map(|v| v.min(version).saturating_sub(1))
            .unwrap_or(0);
        let log_entries = list_delta_log_entries_from(store.clone(), offset_version).await?;
        let mut commit_entries: Vec<(i64, ObjectMeta)> = Vec::new();
        let mut checkpoint_entries: Vec<(i64, ObjectMeta)> = Vec::new();
        for meta in log_entries {
            if let Some(v) = parse_commit_version_from_location(&meta.location) {
                if v <= version {
                    commit_entries.push((v, meta));
                }
                continue;
            }
            if let Some(v) = parse_checkpoint_version_from_location(&meta.location) {
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
            &commit_entries,
            start_commit_version,
            version,
        )
        .await?;
        state.prune_expired_checkpoint_actions(Utc::now().timestamp_millis())?;

        // Batching avoids one giant RecordBatch, but full-state materialization
        // is still the main memory cost here.
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

pub(crate) async fn replay_commit_actions(
    state: &mut ReconciledCheckpointState,
    root_store: std::sync::Arc<dyn ObjectStore>,
    commit_entries: &[(i64, ObjectMeta)],
    start_version: i64,
    end_version: i64,
) -> DeltaResult<BTreeMap<i64, i64>> {
    if start_version > end_version {
        return Ok(BTreeMap::new());
    }

    let mut expected_version = start_version;
    let mut commit_timestamps = BTreeMap::new();
    for (version, meta) in commit_entries {
        if *version < start_version || *version > end_version {
            continue;
        }
        if *version != expected_version {
            return Err(DeltaTableError::generic(format!(
                "Missing commit file while building checkpoint: expected version {expected_version}, found {version}"
            )));
        }
        let bytes = root_store.get(&meta.location).await?.bytes().await?;
        let actions = get_actions(*version, &bytes)?;
        let commit_timestamp = resolve_commit_timestamp_from_actions(
            *version,
            meta,
            state.protocol.as_ref(),
            state.metadata.as_ref(),
            &actions,
        )?;
        for action in actions {
            state.apply_action(action);
        }
        commit_timestamps.insert(*version, commit_timestamp);
        expected_version = expected_version.saturating_add(1);
    }

    if expected_version.saturating_sub(1) != end_version {
        return Err(DeltaTableError::generic(format!(
            "Missing commit file while building checkpoint: expected final version {end_version}, replay reached {}",
            expected_version.saturating_sub(1)
        )));
    }
    Ok(commit_timestamps)
}

pub(crate) async fn read_checkpoint_rows_from_parquet(
    root_store: std::sync::Arc<dyn ObjectStore>,
    meta: ObjectMeta,
) -> DeltaResult<Vec<CheckpointActionRow>> {
    let bytes = root_store.get(&meta.location).await?.bytes().await?;
    tokio::task::spawn_blocking(move || {
        // TODO: V2 checkpoints move add/remove rows into sidecars; full replay
        // needs to read those parquet files too.
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

pub(crate) async fn replay_commit_header_actions(
    state: &mut ReconciledHeaderState,
    root_store: std::sync::Arc<dyn ObjectStore>,
    commit_entries: &[(i64, ObjectMeta)],
    start_version: i64,
    end_version: i64,
) -> DeltaResult<BTreeMap<i64, i64>> {
    if start_version > end_version {
        return Ok(BTreeMap::new());
    }

    let mut expected_version = start_version;
    let mut commit_timestamps = BTreeMap::new();
    for (version, meta) in commit_entries {
        if *version < start_version || *version > end_version {
            continue;
        }
        if *version != expected_version {
            return Err(DeltaTableError::generic(format!(
                "Missing commit file while replaying table header: expected version {expected_version}, found {version}"
            )));
        }
        let bytes = root_store.get(&meta.location).await?.bytes().await?;
        let actions = get_actions(*version, &bytes)?;
        let commit_timestamp = resolve_commit_timestamp_from_actions(
            *version,
            meta,
            state.protocol.as_ref(),
            state.metadata.as_ref(),
            &actions,
        )?;
        for action in actions {
            state.apply_action(action);
        }
        commit_timestamps.insert(*version, commit_timestamp);
        expected_version = expected_version.saturating_add(1);
    }

    if expected_version.saturating_sub(1) != end_version {
        return Err(DeltaTableError::generic(format!(
            "Missing commit file while replaying table header: expected final version {end_version}, replay reached {}",
            expected_version.saturating_sub(1)
        )));
    }
    Ok(commit_timestamps)
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
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

    use super::{
        checkpoint_fields, decode_checkpoint_rows, encode_checkpoint_rows,
        replay_commit_header_actions, ReconciledCheckpointState, ReconciledHeaderState,
    };
    use crate::spec::{
        Action, Add, CheckpointActionRow, CommitInfo, DataType, DeletionVectorDescriptor,
        DeltaError, DeltaResult, Metadata, Protocol, Remove, StorageType, StructField, StructType,
        TableFeature, Transaction,
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

    fn commit_meta(version: i64, last_modified_millis: i64) -> DeltaResult<ObjectMeta> {
        let last_modified = DateTime::from_timestamp_millis(last_modified_millis)
            .ok_or_else(|| DeltaError::generic("test timestamp must be valid"))?;
        Ok(ObjectMeta {
            location: Path::from(format!("_delta_log/{version:020}.json")),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        })
    }

    async fn put_commit(
        store: &Arc<dyn ObjectStore>,
        version: i64,
        actions: &[Action],
    ) -> DeltaResult<()> {
        let mut bytes = Vec::new();
        for (index, action) in actions.iter().enumerate() {
            if index > 0 {
                bytes.push(b'\n');
            }
            serde_json::to_writer(&mut bytes, action)?;
        }
        store
            .put(
                &Path::from(format!("_delta_log/{version:020}.json")),
                bytes.into(),
            )
            .await?;
        Ok(())
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
        #[expect(clippy::expect_used)]
        let fields = checkpoint_fields().expect("checkpoint fields should build");
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
        #[expect(clippy::expect_used)]
        let fields = checkpoint_fields().expect("checkpoint fields should build");
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

    #[test]
    fn checkpoint_schema_reuses_shared_payload_types() {
        #[expect(clippy::expect_used)]
        let fields = checkpoint_fields().expect("checkpoint fields should build");
        #[expect(clippy::expect_used)]
        let expected_add =
            ArrowDataType::try_from(&crate::spec::DataType::from(crate::spec::add_struct_type()))
                .expect("shared add schema should convert to Arrow");
        #[expect(clippy::expect_used)]
        let expected_metadata = ArrowDataType::try_from(&crate::spec::DataType::from(
            crate::spec::metadata_struct_type(),
        ))
        .expect("shared metadata schema should convert to Arrow");

        let add_type = fields
            .iter()
            .find(|field| field.name() == "add")
            .map(|field| field.data_type().clone());
        let metadata_type = fields
            .iter()
            .find(|field| field.name() == "metaData")
            .map(|field| field.data_type().clone());

        assert_eq!(add_type, Some(expected_add));
        assert_eq!(metadata_type, Some(expected_metadata));
    }

    #[tokio::test]
    async fn replay_commit_header_actions_prefers_in_commit_timestamp() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 7, None, Some(vec![TableFeature::InCommitTimestamp]));
        let metadata = test_metadata([("delta.enableInCommitTimestamps", "true")])?;
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: Some(123),
                    ..Default::default()
                }),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
        )
        .await?;

        let commit_meta = commit_meta(0, 9_999)?;
        let timestamps = replay_commit_header_actions(
            &mut ReconciledHeaderState::default(),
            store,
            &[(0, commit_meta)],
            0,
            0,
        )
        .await?;

        assert_eq!(timestamps.get(&0), Some(&123));
        Ok(())
    }

    #[tokio::test]
    async fn replay_commit_header_actions_falls_back_to_mtime_before_enablement() -> DeltaResult<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = test_metadata([])?;
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
        )
        .await?;

        let commit_meta = commit_meta(0, 4_567)?;
        let timestamps = replay_commit_header_actions(
            &mut ReconciledHeaderState::default(),
            store,
            &[(0, commit_meta)],
            0,
            0,
        )
        .await?;

        assert_eq!(timestamps.get(&0), Some(&4_567));
        Ok(())
    }

    #[tokio::test]
    async fn replay_commit_header_actions_ignores_pre_enable_ict_before_upgrade() -> DeltaResult<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pre_enable_protocol = Protocol::new(1, 2, None, None);
        let pre_enable_metadata = test_metadata([])?;
        let enabled_protocol =
            Protocol::new(1, 7, None, Some(vec![TableFeature::InCommitTimestamp]));
        let enabled_metadata = test_metadata([
            ("delta.enableInCommitTimestamps", "true"),
            ("delta.inCommitTimestampEnablementVersion", "1"),
            ("delta.inCommitTimestampEnablementTimestamp", "300"),
        ])?;
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: Some(10_000),
                    ..Default::default()
                }),
                Action::Protocol(pre_enable_protocol),
                Action::Metadata(pre_enable_metadata),
            ],
        )
        .await?;
        put_commit(
            &store,
            1,
            &[
                Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: Some(300),
                    ..Default::default()
                }),
                Action::Protocol(enabled_protocol),
                Action::Metadata(enabled_metadata),
            ],
        )
        .await?;

        let timestamps = replay_commit_header_actions(
            &mut ReconciledHeaderState::default(),
            store,
            &[(0, commit_meta(0, 4_567)?), (1, commit_meta(1, 9_999)?)],
            0,
            1,
        )
        .await?;

        assert_eq!(timestamps.get(&0), Some(&4_567));
        assert_eq!(timestamps.get(&1), Some(&300));
        Ok(())
    }
}
