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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/transaction/mod.rs>

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::future::BoxFuture;
use log::*;
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use crate::delta_log::cleanup::cleanup_expired_delta_log_files;
use crate::delta_log::{resolve_effective_protocol_and_metadata, resolve_version_timestamp};
use crate::kernel::checkpoints::{
    create_checkpoint_for, create_log_compaction_for, should_create_compaction,
};
use crate::kernel::transaction::conflict_checker::{TransactionInfo, WinningCommitSummary};
use crate::kernel::{DeltaOperation, DeltaSnapshotConfig};
use crate::spec::{
    checksum_path, temp_commit_path, Action, CommitAction, DeltaError, DeltaResult, Metadata,
    TableFeature, Transaction, VersionChecksum,
};
pub use crate::spec::{CommitConflictError, TransactionError};
use crate::storage::{CommitOrBytes, LogStoreRef, ObjectStoreRef};
use crate::table::DeltaSnapshot;

mod conflict_checker;
mod protocol;

use conflict_checker::ConflictChecker;
pub use protocol::INSTANCE as PROTOCOL;

pub(crate) const DEFAULT_RETRIES: usize = 15;

#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitMetrics {
    pub num_retries: u64,
}

#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub num_retries: u64,
    pub new_checkpoint_created: bool,
    pub num_log_files_cleaned_up: u64,
}

/// Metrics serialized as `commitInfo.operationMetrics`. `None` fields are omitted.
#[derive(Default, Debug, PartialEq, Clone)]
pub struct OperationMetrics {
    pub num_files: Option<u64>,
    pub num_output_rows: Option<u64>,
    pub num_output_bytes: Option<u64>,
    pub execution_time_ms: Option<u64>,
    pub scan_time_ms: Option<u64>,
    pub rewrite_time_ms: Option<u64>,
    pub write_time_ms: Option<u64>,
    pub num_removed_files: Option<u64>,
    pub num_added_files: Option<u64>,
    pub num_output_files: Option<u64>,
    pub num_added_bytes: Option<u64>,
    pub num_removed_bytes: Option<u64>,
    pub num_deleted_rows: Option<u64>,
    pub num_updated_rows: Option<u64>,
    pub num_copied_rows: Option<u64>,
    pub num_touched_rows: Option<u64>,
    pub num_source_rows: Option<u64>,
    pub num_target_rows_inserted: Option<u64>,
    pub num_target_rows_updated: Option<u64>,
    pub num_target_rows_deleted: Option<u64>,
    pub num_target_rows_copied: Option<u64>,
    pub num_target_files_added: Option<u64>,
    pub num_target_files_removed: Option<u64>,
    pub num_target_bytes_added: Option<u64>,
    pub num_target_bytes_removed: Option<u64>,
    pub num_deletion_vectors_added: Option<u64>,
    pub num_deletion_vectors_updated: Option<u64>,
    pub num_deletion_vectors_removed: Option<u64>,
    pub extra: HashMap<String, Value>,
}

impl Serialize for OperationMetrics {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_map().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OperationMetrics {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        HashMap::<String, Value>::deserialize(deserializer).map(Self::from)
    }
}

impl OperationMetrics {
    pub fn into_map(self) -> HashMap<String, Value> {
        let mut out = self.extra;
        macro_rules! insert_opt {
            ($key:literal, $field:expr) => {
                if let Some(v) = $field {
                    out.insert($key.to_string(), Value::from(v));
                }
            };
        }
        insert_opt!("numFiles", self.num_files);
        insert_opt!("numOutputRows", self.num_output_rows);
        insert_opt!("numOutputBytes", self.num_output_bytes);
        insert_opt!("executionTimeMs", self.execution_time_ms);
        insert_opt!("scanTimeMs", self.scan_time_ms);
        insert_opt!("rewriteTimeMs", self.rewrite_time_ms);
        insert_opt!("writeTimeMs", self.write_time_ms);
        insert_opt!("numRemovedFiles", self.num_removed_files);
        insert_opt!("numAddedFiles", self.num_added_files);
        insert_opt!("numOutputFiles", self.num_output_files);
        insert_opt!("numAddedBytes", self.num_added_bytes);
        insert_opt!("numRemovedBytes", self.num_removed_bytes);
        insert_opt!("numDeletedRows", self.num_deleted_rows);
        insert_opt!("numUpdatedRows", self.num_updated_rows);
        insert_opt!("numCopiedRows", self.num_copied_rows);
        insert_opt!("numTouchedRows", self.num_touched_rows);
        insert_opt!("numSourceRows", self.num_source_rows);
        insert_opt!("numTargetRowsInserted", self.num_target_rows_inserted);
        insert_opt!("numTargetRowsUpdated", self.num_target_rows_updated);
        insert_opt!("numTargetRowsDeleted", self.num_target_rows_deleted);
        insert_opt!("numTargetRowsCopied", self.num_target_rows_copied);
        insert_opt!("numTargetFilesAdded", self.num_target_files_added);
        insert_opt!("numTargetFilesRemoved", self.num_target_files_removed);
        insert_opt!("numTargetBytesAdded", self.num_target_bytes_added);
        insert_opt!("numTargetBytesRemoved", self.num_target_bytes_removed);
        insert_opt!("numDeletionVectorsAdded", self.num_deletion_vectors_added);
        insert_opt!(
            "numDeletionVectorsUpdated",
            self.num_deletion_vectors_updated
        );
        insert_opt!(
            "numDeletionVectorsRemoved",
            self.num_deletion_vectors_removed
        );
        out
    }

    pub fn merge(&mut self, other: Self) {
        fn merge_opt(target: &mut Option<u64>, source: Option<u64>) {
            if let Some(source) = source {
                let merged = target.unwrap_or_default().saturating_add(source);
                *target = Some(merged);
            }
        }

        merge_opt(&mut self.num_files, other.num_files);
        merge_opt(&mut self.num_output_rows, other.num_output_rows);
        merge_opt(&mut self.num_output_bytes, other.num_output_bytes);
        merge_opt(&mut self.execution_time_ms, other.execution_time_ms);
        merge_opt(&mut self.scan_time_ms, other.scan_time_ms);
        merge_opt(&mut self.rewrite_time_ms, other.rewrite_time_ms);
        merge_opt(&mut self.write_time_ms, other.write_time_ms);
        merge_opt(&mut self.num_removed_files, other.num_removed_files);
        merge_opt(&mut self.num_added_files, other.num_added_files);
        merge_opt(&mut self.num_output_files, other.num_output_files);
        merge_opt(&mut self.num_added_bytes, other.num_added_bytes);
        merge_opt(&mut self.num_removed_bytes, other.num_removed_bytes);
        merge_opt(&mut self.num_deleted_rows, other.num_deleted_rows);
        merge_opt(&mut self.num_updated_rows, other.num_updated_rows);
        merge_opt(&mut self.num_copied_rows, other.num_copied_rows);
        merge_opt(&mut self.num_touched_rows, other.num_touched_rows);
        merge_opt(&mut self.num_source_rows, other.num_source_rows);
        merge_opt(
            &mut self.num_target_rows_inserted,
            other.num_target_rows_inserted,
        );
        merge_opt(
            &mut self.num_target_rows_updated,
            other.num_target_rows_updated,
        );
        merge_opt(
            &mut self.num_target_rows_deleted,
            other.num_target_rows_deleted,
        );
        merge_opt(
            &mut self.num_target_rows_copied,
            other.num_target_rows_copied,
        );
        merge_opt(
            &mut self.num_target_files_added,
            other.num_target_files_added,
        );
        merge_opt(
            &mut self.num_target_files_removed,
            other.num_target_files_removed,
        );
        merge_opt(
            &mut self.num_target_bytes_added,
            other.num_target_bytes_added,
        );
        merge_opt(
            &mut self.num_target_bytes_removed,
            other.num_target_bytes_removed,
        );
        merge_opt(
            &mut self.num_deletion_vectors_added,
            other.num_deletion_vectors_added,
        );
        merge_opt(
            &mut self.num_deletion_vectors_updated,
            other.num_deletion_vectors_updated,
        );
        merge_opt(
            &mut self.num_deletion_vectors_removed,
            other.num_deletion_vectors_removed,
        );

        self.extra.extend(other.extra);
    }

    /// Derive operation-specific metrics from generic counters. Call once at commit time.
    pub fn finalize_for(&mut self, operation: &crate::kernel::DeltaOperation) {
        use crate::kernel::DeltaOperation;

        match operation {
            DeltaOperation::Delete { .. } => {
                if self.num_copied_rows.is_none() {
                    self.num_copied_rows = self.num_output_rows;
                }
                if self.num_deleted_rows.is_none() {
                    if let (Some(touched), Some(copied)) =
                        (self.num_touched_rows, self.num_copied_rows)
                    {
                        self.num_deleted_rows = Some(touched.saturating_sub(copied));
                    }
                }
                if self.rewrite_time_ms.is_none() {
                    self.rewrite_time_ms = self.write_time_ms;
                }
            }
            DeltaOperation::Merge { .. } => {
                if self.num_target_files_added.is_none() {
                    self.num_target_files_added = self.num_added_files;
                }
                if self.num_target_files_removed.is_none() {
                    self.num_target_files_removed = self.num_removed_files;
                }
                if self.num_target_bytes_added.is_none() {
                    self.num_target_bytes_added = self.num_added_bytes;
                }
                if self.num_target_bytes_removed.is_none() {
                    self.num_target_bytes_removed = self.num_removed_bytes;
                }
                if self.rewrite_time_ms.is_none() {
                    self.rewrite_time_ms = self.write_time_ms;
                }
            }
            DeltaOperation::FileSystemCheck { .. } => {
                self.num_added_files = None;
                self.num_added_bytes = None;
                self.num_output_rows = None;
                self.num_output_bytes = None;
                self.num_output_files = None;
            }
            // TODO: Restore should report numRestoredFiles / numRemovedFiles /
            // restoredFilesSize / removedFilesSize / numOfFilesAfterRestore /
            // tableSizeAfterRestore. Requires the restore exec to aggregate these
            // counts from the snapshot diff it produces.
            DeltaOperation::Restore { .. }
            | DeltaOperation::Write { .. }
            | DeltaOperation::Create { .. }
            | DeltaOperation::SetTableProperties { .. }
            | DeltaOperation::UnsetTableProperties { .. } => {} // TODO: When the following operations are implemented, extend this match:
                                                                //   - UPDATE: numAddedFiles, numRemovedFiles, numUpdatedRows, numCopiedRows,
                                                                //     executionTimeMs, scanTimeMs, rewriteTimeMs
                                                                //   - OPTIMIZE / ZORDER: numAdded/Removed files+bytes histograms,
                                                                //     partitionsOptimized, numBatches, filesAdded/filesRemoved quantiles
                                                                //   - VACUUM START/END: numFilesToDelete, sizeOfDataToDelete,
                                                                //     numDeletedFiles, numVacuumedDirectories
        }
    }
}

impl From<HashMap<String, Value>> for OperationMetrics {
    fn from(mut value: HashMap<String, Value>) -> Self {
        fn take_u64(map: &mut HashMap<String, Value>, key: &str) -> Option<u64> {
            match map.remove(key) {
                Some(Value::Number(n)) => n.as_u64(),
                Some(other) => {
                    map.insert(key.to_string(), other);
                    None
                }
                None => None,
            }
        }

        let num_files = take_u64(&mut value, "numFiles");
        let num_output_rows = take_u64(&mut value, "numOutputRows");
        let num_output_bytes = take_u64(&mut value, "numOutputBytes");
        let execution_time_ms = take_u64(&mut value, "executionTimeMs");
        let scan_time_ms = take_u64(&mut value, "scanTimeMs");
        let rewrite_time_ms = take_u64(&mut value, "rewriteTimeMs");
        let write_time_ms = take_u64(&mut value, "writeTimeMs");
        let num_removed_files = take_u64(&mut value, "numRemovedFiles");
        let num_added_files = take_u64(&mut value, "numAddedFiles");
        let num_output_files = take_u64(&mut value, "numOutputFiles");
        let num_added_bytes = take_u64(&mut value, "numAddedBytes");
        let num_removed_bytes = take_u64(&mut value, "numRemovedBytes");
        let num_deleted_rows = take_u64(&mut value, "numDeletedRows");
        let num_updated_rows = take_u64(&mut value, "numUpdatedRows");
        let num_copied_rows = take_u64(&mut value, "numCopiedRows");
        let num_touched_rows = take_u64(&mut value, "numTouchedRows");
        let num_source_rows = take_u64(&mut value, "numSourceRows");
        let num_target_rows_inserted = take_u64(&mut value, "numTargetRowsInserted");
        let num_target_rows_updated = take_u64(&mut value, "numTargetRowsUpdated");
        let num_target_rows_deleted = take_u64(&mut value, "numTargetRowsDeleted");
        let num_target_rows_copied = take_u64(&mut value, "numTargetRowsCopied");
        let num_target_files_added = take_u64(&mut value, "numTargetFilesAdded");
        let num_target_files_removed = take_u64(&mut value, "numTargetFilesRemoved");
        let num_target_bytes_added = take_u64(&mut value, "numTargetBytesAdded");
        let num_target_bytes_removed = take_u64(&mut value, "numTargetBytesRemoved");
        let num_deletion_vectors_added = take_u64(&mut value, "numDeletionVectorsAdded");
        let num_deletion_vectors_updated = take_u64(&mut value, "numDeletionVectorsUpdated");
        let num_deletion_vectors_removed = take_u64(&mut value, "numDeletionVectorsRemoved");

        Self {
            num_files,
            num_output_rows,
            num_output_bytes,
            execution_time_ms,
            scan_time_ms,
            rewrite_time_ms,
            write_time_ms,
            num_removed_files,
            num_added_files,
            num_output_files,
            num_added_bytes,
            num_removed_bytes,
            num_deleted_rows,
            num_updated_rows,
            num_copied_rows,
            num_touched_rows,
            num_source_rows,
            num_target_rows_inserted,
            num_target_rows_updated,
            num_target_rows_deleted,
            num_target_rows_copied,
            num_target_files_added,
            num_target_files_removed,
            num_target_bytes_added,
            num_target_bytes_removed,
            num_deletion_vectors_added,
            num_deletion_vectors_updated,
            num_deletion_vectors_removed,
            extra: value,
        }
    }
}

fn actions_to_log_bytes(actions: &[CommitAction]) -> Result<Bytes, TransactionError> {
    let mut buf: Vec<u8> = Vec::new();
    for (index, action) in actions.iter().enumerate() {
        if index > 0 {
            buf.push(b'\n');
        }
        serde_json::to_writer(&mut buf, action)
            .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
    }
    Ok(Bytes::from(buf))
}

#[derive(Debug)]
pub struct CommitData {
    pub actions: Vec<CommitAction>,
    pub operation: DeltaOperation,
}

impl CommitData {
    pub fn new(
        mut actions: Vec<CommitAction>,
        operation: DeltaOperation,
        mut app_metadata: HashMap<String, Value>,
        operation_metrics: OperationMetrics,
        app_transactions: Vec<Transaction>,
        user_metadata: Option<String>,
    ) -> Self {
        let is_blind_append = Self::is_blind_append(&actions, &operation);
        let mut commit_info = actions
            .iter()
            .find_map(|action| match action {
                CommitAction::CommitInfo(info) => Some(info.clone()),
                _ => None,
            })
            .unwrap_or_else(|| operation.get_commit_info());
        if let Some(value) = user_metadata {
            commit_info.user_metadata = Some(value);
        }
        if commit_info.in_commit_timestamp.is_none() {
            commit_info.in_commit_timestamp = commit_info
                .info
                .remove("inCommitTimestamp")
                .and_then(|value| value.as_i64());
        } else {
            commit_info.info.remove("inCommitTimestamp");
        }
        commit_info.is_blind_append = Some(is_blind_append);
        app_metadata
            .entry("clientVersion".to_string())
            .or_insert_with(|| {
                Value::String(format!("sail-delta-lake.{}", env!("CARGO_PKG_VERSION")))
            });
        // Merge operationMetrics into the final commitInfo.info.
        // If the caller also provided `operationMetrics` in app metadata, merge both.
        let mut merged_operation_metrics: HashMap<String, Value> = HashMap::new();
        if let Some(Value::Object(obj)) = commit_info.info.get("operationMetrics").cloned() {
            merged_operation_metrics.extend(obj);
        }
        if let Some(Value::Object(obj)) = app_metadata.get("operationMetrics").cloned() {
            merged_operation_metrics.extend(obj);
        }
        merged_operation_metrics.extend(operation_metrics.into_map());

        // Merge base info + app metadata (app metadata wins on conflicts).
        let mut merged_info = commit_info.info.clone();
        merged_info.extend(app_metadata.clone());
        if !merged_operation_metrics.is_empty() {
            merged_info.insert(
                "operationMetrics".to_string(),
                Value::Object(merged_operation_metrics.into_iter().collect()),
            );
        }
        commit_info.info = merged_info;
        actions.retain(|action| !matches!(action, CommitAction::CommitInfo(_)));
        actions.insert(0, CommitAction::CommitInfo(commit_info));

        for txn in &app_transactions {
            actions.push(CommitAction::Txn(txn.clone()));
        }

        Self { actions, operation }
    }

    fn commit_info(&self) -> Option<&crate::spec::CommitInfo> {
        self.actions.iter().find_map(|action| match action {
            CommitAction::CommitInfo(info) => Some(info),
            _ => None,
        })
    }

    fn version_checksum_txn_id(&self) -> Option<String> {
        self.commit_info().and_then(|info| {
            info.info
                .get("txnId")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
    }

    fn version_checksum_in_commit_timestamp(&self) -> Option<i64> {
        self.commit_info().and_then(|info| info.in_commit_timestamp)
    }

    fn is_blind_append(actions: &[CommitAction], operation: &DeltaOperation) -> bool {
        match operation {
            DeltaOperation::Write { predicate, .. } if predicate.is_none() => {
                actions.iter().all(|action| {
                    matches!(
                        action,
                        CommitAction::Add(_) | CommitAction::Txn(_) | CommitAction::CommitInfo(_)
                    )
                })
            }
            _ => false,
        }
    }
}

async fn write_tmp_commit(log_entry: Bytes, store: ObjectStoreRef) -> DeltaResult<CommitOrBytes> {
    let token = uuid::Uuid::new_v4().to_string();
    let path = temp_commit_path(&token);
    store.put(&path, log_entry.into()).await?;
    Ok(CommitOrBytes::TmpCommit(path))
}

async fn prepare_commit_or_bytes(
    log_store: &LogStoreRef,
    operation_id: Uuid,
    actions: &[CommitAction],
) -> DeltaResult<CommitOrBytes> {
    let log_entry = actions_to_log_bytes(actions)?;
    if ["LakeFSLogStore", "DefaultLogStore"].contains(&log_store.name().as_str()) {
        Ok(CommitOrBytes::LogBytes(log_entry))
    } else {
        write_tmp_commit(log_entry, log_store.object_store(Some(operation_id))).await
    }
}

async fn previous_effective_commit_timestamp(
    log_store: &LogStoreRef,
    snapshot: Option<&Arc<DeltaSnapshot>>,
) -> DeltaResult<Option<i64>> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    resolve_version_timestamp(
        log_store.as_ref(),
        snapshot.version(),
        snapshot.version_timestamp(snapshot.version()),
        snapshot.protocol(),
        snapshot.metadata(),
    )
    .await
    .map(Some)
}

fn finalized_commit_info(actions: &mut [CommitAction]) -> &mut crate::spec::CommitInfo {
    match actions.first_mut() {
        Some(CommitAction::CommitInfo(info)) => info,
        _ => unreachable!("commit actions must be normalized with commitInfo at index 0"),
    }
}

fn finalize_attempt_actions(
    base_actions: &[CommitAction],
    read_snapshot: Option<&Arc<DeltaSnapshot>>,
    version: i64,
    previous_commit_timestamp: Option<i64>,
    now_ms: i64,
) -> DeltaResult<Vec<CommitAction>> {
    let mut finalized_actions = base_actions.to_vec();
    let old_in_commit_timestamps_enabled = read_snapshot
        .map(|snapshot| snapshot.in_commit_timestamps_enabled())
        .unwrap_or(false);
    let finalized_actions_as_actions = finalized_actions
        .iter()
        .cloned()
        .map(Action::from)
        .collect::<Vec<_>>();
    let effective_protocol_and_metadata = resolve_effective_protocol_and_metadata(
        read_snapshot.map(|snapshot| snapshot.protocol()),
        read_snapshot.map(|snapshot| snapshot.metadata()),
        &finalized_actions_as_actions,
    );
    let new_in_commit_timestamps_enabled = effective_protocol_and_metadata
        .as_ref()
        .map(|(protocol, metadata)| {
            let table_properties =
                crate::spec::TableProperties::from(metadata.configuration().iter());
            protocol.is_in_commit_timestamps_enabled(&table_properties)
        })
        .unwrap_or(false);
    let mut in_commit_timestamp = None;

    {
        let commit_info = finalized_commit_info(&mut finalized_actions);
        commit_info.timestamp = Some(now_ms);
        commit_info.info.remove("inCommitTimestamp");
        if new_in_commit_timestamps_enabled {
            let min_timestamp = previous_commit_timestamp
                .map(|timestamp| timestamp.saturating_add(1))
                .unwrap_or(now_ms);
            in_commit_timestamp = Some(now_ms.max(min_timestamp));
            commit_info.in_commit_timestamp = in_commit_timestamp;
        } else {
            commit_info.in_commit_timestamp = None;
        }
    }

    if read_snapshot.is_some()
        && !old_in_commit_timestamps_enabled
        && new_in_commit_timestamps_enabled
    {
        if let Some(in_commit_timestamp) = in_commit_timestamp {
            if let Some(metadata) = finalized_actions
                .iter_mut()
                .find_map(|action| match action {
                    CommitAction::Metadata(metadata) => Some(metadata),
                    _ => None,
                })
            {
                *metadata = metadata
                    .clone()
                    .add_config_key(
                        "delta.inCommitTimestampEnablementVersion".to_string(),
                        version.to_string(),
                    )
                    .add_config_key(
                        "delta.inCommitTimestampEnablementTimestamp".to_string(),
                        in_commit_timestamp.to_string(),
                    );
            }
        }
    }

    Ok(finalized_actions)
}

fn table_property_enabled(metadata: &Metadata, key: &str) -> bool {
    metadata
        .configuration()
        .get(key)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn protocol_supports_legacy_change_data_feed(protocol: &crate::spec::Protocol) -> bool {
    matches!(protocol.min_writer_version(), 4..=6)
}

fn protocol_has_writer_feature(protocol: &crate::spec::Protocol, feature: &TableFeature) -> bool {
    protocol.min_writer_version() >= 7 && protocol.has_writer_feature(feature)
}

fn protocol_has_reader_writer_feature(
    protocol: &crate::spec::Protocol,
    feature: &TableFeature,
) -> bool {
    protocol.min_reader_version() >= 3
        && protocol.min_writer_version() >= 7
        && protocol.has_reader_feature(feature)
        && protocol.has_writer_feature(feature)
}

fn validate_effective_commit_target(
    read_snapshot: Option<&Arc<DeltaSnapshot>>,
    actions: &[CommitAction],
) -> DeltaResult<()> {
    let actions_as_actions = actions
        .iter()
        .cloned()
        .map(Action::from)
        .collect::<Vec<_>>();
    let (protocol, metadata) = resolve_effective_protocol_and_metadata(
        read_snapshot.map(|snapshot| snapshot.protocol()),
        read_snapshot.map(|snapshot| snapshot.metadata()),
        &actions_as_actions,
    )
    .ok_or_else(|| {
        DeltaError::generic("Cannot validate commit without effective protocol and metadata")
    })?;

    PROTOCOL.can_write_to_protocol(&protocol)?;
    PROTOCOL.check_can_write_timestamp_ntz_to_protocol(&protocol, &metadata.parse_schema()?)?;

    if actions_as_actions
        .iter()
        .any(|action| matches!(action, Action::DomainMetadata(_)))
        && !protocol_has_writer_feature(&protocol, &TableFeature::DomainMetadata)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::DomainMetadata).into());
    }

    if actions_as_actions
        .iter()
        .any(|action| matches!(action, Action::Cdc(_)))
        && !protocol_supports_legacy_change_data_feed(&protocol)
        && !protocol_has_writer_feature(&protocol, &TableFeature::ChangeDataFeed)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::ChangeDataFeed).into());
    }

    if actions_as_actions.iter().any(|action| match action {
        Action::Add(add) => add.deletion_vector.is_some(),
        Action::Remove(remove) => remove.deletion_vector.is_some(),
        _ => false,
    }) && !protocol_has_reader_writer_feature(&protocol, &TableFeature::DeletionVectors)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::DeletionVectors).into());
    }

    // TODO(cdf-writes): Data-changing operations still do not emit AddCDCFile actions. Commit-time
    // protocol checks currently reject changeDataFeed tables before these writes can land, but once
    // the feature is enabled here we need an operation-aware validation instead of relying on that.
    if table_property_enabled(&metadata, "delta.enableChangeDataFeed")
        && !protocol_supports_legacy_change_data_feed(&protocol)
        && !protocol_has_writer_feature(&protocol, &TableFeature::ChangeDataFeed)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::ChangeDataFeed).into());
    }

    if table_property_enabled(&metadata, "delta.enableDeletionVectors")
        && !protocol_has_reader_writer_feature(&protocol, &TableFeature::DeletionVectors)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::DeletionVectors).into());
    }

    // TODO(row-tracking-writes): We still rely on commit-time protocol rejection for tables that
    // advertise rowTracking/domainMetadata. When row-tracking writes are implemented, this needs
    // to grow into explicit validation of baseRowId/defaultRowCommitVersion assignment.
    let row_tracking_requested = table_property_enabled(&metadata, "delta.enableRowTracking")
        || table_property_enabled(&metadata, "delta.rowTrackingSuspended");
    if row_tracking_requested && !protocol_has_writer_feature(&protocol, &TableFeature::RowTracking)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::RowTracking).into());
    }
    if row_tracking_requested
        && !protocol_has_writer_feature(&protocol, &TableFeature::DomainMetadata)
    {
        return Err(TransactionError::TableFeaturesRequired(TableFeature::DomainMetadata).into());
    }

    Ok(())
}

#[async_trait]
pub trait CustomExecuteHandler: Send + Sync {
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;

    async fn after_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;
}

#[derive(Clone, Debug, Copy)]
/// Properties for post commit hook.
pub struct PostCommitHookProperties {
    create_checkpoint: bool,
    /// Override the EnableExpiredLogCleanUp setting, if None config setting is used
    cleanup_expired_logs: Option<bool>,
}

#[derive(Clone, Debug)]
/// End user facing interface to be used by operations on the table.
/// Enable controlling commit behaviour and modifying metadata that is written during a commit.
pub struct CommitProperties {
    pub(crate) app_metadata: HashMap<String, Value>,
    pub(crate) operation_metrics: OperationMetrics,
    pub(crate) app_transaction: Vec<Transaction>,
    pub(crate) user_metadata: Option<String>,
    max_retries: usize,
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
}

impl Default for CommitProperties {
    fn default() -> Self {
        Self {
            app_metadata: Default::default(),
            operation_metrics: Default::default(),
            app_transaction: Vec::new(),
            user_metadata: None,
            max_retries: DEFAULT_RETRIES,
            create_checkpoint: true,
            cleanup_expired_logs: None,
        }
    }
}

impl CommitProperties {
    /// Attach operation metrics that will be merged into the Delta log `commitInfo` action
    /// under the `operationMetrics` key.
    pub(crate) fn with_operation_metrics(
        mut self,
        operation_metrics: impl Into<OperationMetrics>,
    ) -> Self {
        self.operation_metrics = operation_metrics.into();
        self
    }

    /// Set the user-defined commit metadata string written to `commitInfo.userMetadata`.
    pub(crate) fn with_user_metadata(mut self, user_metadata: Option<String>) -> Self {
        self.user_metadata = user_metadata;
        self
    }
}

// impl CommitProperties {
//     /// Specify metadata the be committed
//     pub fn with_metadata(
//         mut self,
//         metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
//     ) -> Self {
//         self.app_metadata = HashMap::from_iter(metadata);
//         self
//     }

//     /// Specify maximum number of times to retry the transaction before failing to commit
//     pub fn with_max_retries(mut self, max_retries: usize) -> Self {
//         self.max_retries = max_retries;
//         self
//     }

//     /// Specify if it should create a checkpoint when the commit interval condition is met
//     pub fn with_create_checkpoint(mut self, create_checkpoint: bool) -> Self {
//         self.create_checkpoint = create_checkpoint;
//         self
//     }

//     /// Add an additional application transaction to the commit
//     pub fn with_application_transaction(mut self, txn: Transaction) -> Self {
//         self.app_transaction.push(txn);
//         self
//     }

//     /// Override application transactions for the commit
//     pub fn with_application_transactions(mut self, txn: Vec<Transaction>) -> Self {
//         self.app_transaction = txn;
//         self
//     }

//     /// Specify if it should clean up the logs when the logRetentionDuration interval is met
//     pub fn with_cleanup_expired_logs(mut self, cleanup_expired_logs: Option<bool>) -> Self {
//         self.cleanup_expired_logs = cleanup_expired_logs;
//         self
//     }
// }

impl From<CommitProperties> for CommitBuilder {
    fn from(value: CommitProperties) -> Self {
        CommitBuilder {
            max_retries: value.max_retries,
            app_metadata: value.app_metadata,
            operation_metrics: value.operation_metrics,
            post_commit_hook: Some(PostCommitHookProperties {
                create_checkpoint: value.create_checkpoint,
                cleanup_expired_logs: value.cleanup_expired_logs,
            }),
            app_transaction: value.app_transaction,
            user_metadata: value.user_metadata,
            ..Default::default()
        }
    }
}

/// Prepare data to be committed to the Delta log and control how the commit is performed
pub struct CommitBuilder {
    actions: Vec<CommitAction>,
    app_metadata: HashMap<String, Value>,
    operation_metrics: OperationMetrics,
    app_transaction: Vec<Transaction>,
    user_metadata: Option<String>,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
}

impl Default for CommitBuilder {
    fn default() -> Self {
        CommitBuilder {
            actions: Vec::new(),
            app_metadata: HashMap::new(),
            operation_metrics: OperationMetrics::default(),
            app_transaction: Vec::new(),
            user_metadata: None,
            max_retries: DEFAULT_RETRIES,
            post_commit_hook: None,
            post_commit_hook_handler: None,
            operation_id: Uuid::new_v4(),
        }
    }
}

impl CommitBuilder {
    /// Actions to be included in the commit.
    ///
    /// Accepts [`CommitAction`] only — checkpoint-only actions (`Sidecar`,
    /// `CheckpointMetadata`) are rejected at compile time.
    pub fn with_actions(mut self, actions: Vec<CommitAction>) -> Self {
        self.actions = actions;
        self
    }

    // /// Metadata for the operation performed like metrics, user, and notebook
    // pub fn with_app_metadata(mut self, app_metadata: HashMap<String, Value>) -> Self {
    //     self.app_metadata = app_metadata;
    //     self
    // }

    // /// Maximum number of times to retry the transaction before failing to commit
    // pub fn with_max_retries(mut self, max_retries: usize) -> Self {
    //     self.max_retries = max_retries;
    //     self
    // }

    // /// Specify all the post commit hook properties
    // pub fn with_post_commit_hook(mut self, post_commit_hook: PostCommitHookProperties) -> Self {
    //     self.post_commit_hook = Some(post_commit_hook);
    //     self
    // }

    // /// Propagate operation id to log store
    // pub fn with_operation_id(mut self, operation_id: Uuid) -> Self {
    //     self.operation_id = operation_id;
    //     self
    // }

    // /// Set a custom execute handler, for pre and post execution
    // pub fn with_post_commit_hook_handler(
    //     mut self,
    //     handler: Option<Arc<dyn CustomExecuteHandler>>,
    // ) -> Self {
    //     self.post_commit_hook_handler = handler;
    //     self
    // }

    /// Prepare a Commit operation using the configured builder
    pub fn build(
        self,
        table_data: Option<Arc<DeltaSnapshot>>,
        log_store: LogStoreRef,
        operation: DeltaOperation,
    ) -> PreCommit {
        let data = CommitData::new(
            self.actions,
            operation,
            self.app_metadata,
            self.operation_metrics,
            self.app_transaction,
            self.user_metadata,
        );
        PreCommit {
            log_store,
            table_data,
            max_retries: self.max_retries,
            data,
            post_commit_hook: self.post_commit_hook,
            post_commit_hook_handler: self.post_commit_hook_handler,
            operation_id: self.operation_id,
        }
    }
}

/// Represents a commit that has not yet started but all details are finalized
pub struct PreCommit {
    log_store: LogStoreRef,
    table_data: Option<Arc<DeltaSnapshot>>,
    data: CommitData,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
}

impl std::future::IntoFuture for PreCommit {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.into_prepared_commit_future().await?.await?.await })
    }
}

impl PreCommit {
    /// Prepare the commit but do not finalize it
    pub fn into_prepared_commit_future(self) -> BoxFuture<'static, DeltaResult<PreparedCommit>> {
        let this = self;

        Box::pin(async move {
            if let Some(table_reference) = &this.table_data {
                // Convert CommitAction → Action only for the protocol-checker call site,
                // which still uses the broader Action type.
                let actions_for_check: Vec<Action> = this
                    .data
                    .actions
                    .iter()
                    .cloned()
                    .map(Action::from)
                    .collect();
                PROTOCOL.can_commit(
                    table_reference.as_ref(),
                    &actions_for_check,
                    &this.data.operation,
                )?;
            }

            Ok(PreparedCommit {
                log_store: this.log_store,
                table_data: this.table_data,
                max_retries: this.max_retries,
                data: this.data,
                post_commit: this.post_commit_hook,
                post_commit_hook_handler: this.post_commit_hook_handler,
                operation_id: this.operation_id,
            })
        })
    }
}

/// Represents a inflight commit
pub struct PreparedCommit {
    log_store: LogStoreRef,
    data: CommitData,
    table_data: Option<Arc<DeltaSnapshot>>,
    max_retries: usize,
    post_commit: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
}

// impl PreparedCommit<'_> {
//     /// The temporary commit file created
//     pub fn commit_or_bytes(&self) -> &CommitOrBytes {
//         &self.commit_or_bytes
//     }
// }

impl std::future::IntoFuture for PreparedCommit {
    type Output = DeltaResult<PostCommit>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut local_actions: Vec<_> = this.data.actions.to_vec();
            let creation_intent = this.table_data.is_none();
            let creation_protocol = local_actions.iter().find_map(|a| match a {
                CommitAction::Protocol(p) => Some(p.clone()),
                _ => None,
            });
            let creation_metadata = local_actions.iter().find_map(|a| match a {
                CommitAction::Metadata(m) => Some(m.clone()),
                _ => None,
            });
            let current_is_blind_append = local_actions
                .iter()
                .find_map(|action| match action {
                    CommitAction::CommitInfo(info) => info.is_blind_append,
                    _ => None,
                })
                .unwrap_or(false);
            let effective_max_retries = if creation_intent || current_is_blind_append {
                this.max_retries
            } else {
                0
            };
            let total_retries = effective_max_retries + 1;

            let mut read_snapshot: Option<Arc<DeltaSnapshot>> = this.table_data.clone();
            let mut creation_actions_stripped = false;
            for attempt_number in 1..=total_retries {
                let snapshot_version = read_snapshot.as_ref().map(|s| s.version()).unwrap_or(-1);
                let latest_version = match this.log_store.get_latest_version(snapshot_version).await
                {
                    Ok(v) => Some(v),
                    Err(DeltaError::MissingVersion) => None,
                    Err(err) => return Err(err),
                };

                if let Some(latest_version) = latest_version {
                    if read_snapshot.is_none() {
                        let snapshot = DeltaSnapshot::try_new(
                            this.log_store.as_ref(),
                            Default::default(),
                            Some(latest_version),
                            None,
                        )
                        .await?;
                        read_snapshot = Some(Arc::new(snapshot));
                    }

                    if let Some(snapshot) = &read_snapshot {
                        // For creation attempts where the table now exists, ensure protocol/metadata match
                        // and strip creation-only actions before retrying as an append.
                        if creation_intent {
                            if let Some(txn_protocol) = creation_protocol.as_ref() {
                                if txn_protocol != snapshot.protocol() {
                                    return Err(TransactionError::CommitConflict(
                                        CommitConflictError::ProtocolChanged(
                                            "protocol changed".into(),
                                        ),
                                    )
                                    .into());
                                }
                            }
                            let metadata_compatible =
                                creation_metadata.as_ref().is_none_or(|txn| {
                                    txn.parse_schema()
                                        .ok()
                                        .zip(snapshot.metadata().parse_schema().ok())
                                        .is_some_and(|(left, right)| left == right)
                                        && txn.partition_columns()
                                            == snapshot.metadata().partition_columns()
                                        && txn.configuration()
                                            == snapshot.metadata().configuration()
                                });
                            if !metadata_compatible {
                                return Err(TransactionError::CommitConflict(
                                    CommitConflictError::MetadataChanged,
                                )
                                .into());
                            }

                            if !creation_actions_stripped
                                && local_actions.iter().any(|action| {
                                    matches!(
                                        action,
                                        CommitAction::Protocol(_) | CommitAction::Metadata(_)
                                    )
                                })
                            {
                                local_actions.retain(|action| {
                                    !matches!(
                                        action,
                                        CommitAction::Protocol(_) | CommitAction::Metadata(_)
                                    )
                                });

                                for action in local_actions.iter_mut() {
                                    if let CommitAction::CommitInfo(info) = action {
                                        info.is_blind_append = Some(true);
                                    }
                                }
                                creation_actions_stripped = true;
                            }
                        }

                        if latest_version > snapshot.version() {
                            // If max_retries are set to 0, do not try to use the conflict checker to resolve the conflict
                            // and throw immediately
                            if effective_max_retries == 0 {
                                return Err(TransactionError::MaxCommitAttempts(
                                    effective_max_retries as i32,
                                )
                                .into());
                            }
                            warn!(
                                "Attempting to write a transaction {} but the underlying table has been updated to {latest_version} (log_store={})",
                                snapshot.version() + 1,
                                this.log_store.name()
                            );
                            // Need to check for conflicts with each version between the read_snapshot and the latest
                            for v in (snapshot.version() + 1)..=latest_version {
                                let summary = WinningCommitSummary::try_new(
                                    this.log_store.as_ref(),
                                    v - 1,
                                    v,
                                )
                                .await?;
                                let transaction_info = TransactionInfo::try_new(
                                    snapshot,
                                    &local_actions,
                                    this.data.operation.read_whole_table(),
                                )?;
                                let conflict_checker = ConflictChecker::new(
                                    transaction_info,
                                    summary,
                                    Some(&this.data.operation),
                                );

                                match conflict_checker.check_conflicts() {
                                    Ok(_) => {}
                                    Err(err) => {
                                        return Err(TransactionError::CommitConflict(err).into());
                                    }
                                }
                            }
                            // Update snapshot to latest version after successful conflict check
                            if let Some(snapshot) = &mut read_snapshot {
                                Arc::make_mut(snapshot)
                                    .update(this.log_store.as_ref(), Some(latest_version as u64))
                                    .await?;
                            }
                        }
                    }
                }
                let version: i64 = latest_version.map(|v| v + 1).unwrap_or(0);
                let previous_commit_timestamp =
                    previous_effective_commit_timestamp(&this.log_store, read_snapshot.as_ref())
                        .await?;
                let finalized_actions = finalize_attempt_actions(
                    &local_actions,
                    read_snapshot.as_ref(),
                    version,
                    previous_commit_timestamp,
                    Utc::now().timestamp_millis(),
                )?;
                validate_effective_commit_target(read_snapshot.as_ref(), &finalized_actions)?;
                let commit_or_bytes =
                    prepare_commit_or_bytes(&this.log_store, this.operation_id, &finalized_actions)
                        .await?;

                match this
                    .log_store
                    .write_commit_entry(version, commit_or_bytes.clone(), this.operation_id)
                    .await
                {
                    Ok(()) => {
                        return Ok(PostCommit {
                            version,
                            data: CommitData {
                                actions: finalized_actions,
                                operation: this.data.operation.clone(),
                            },
                            create_checkpoint: this
                                .post_commit
                                .map(|v| v.create_checkpoint)
                                .unwrap_or_default(),
                            cleanup_expired_logs: this
                                .post_commit
                                .map(|v| v.cleanup_expired_logs)
                                .unwrap_or_default(),
                            log_store: this.log_store,
                            table_data: read_snapshot,
                            custom_execute_handler: this.post_commit_hook_handler,
                            metrics: CommitMetrics {
                                num_retries: attempt_number as u64 - 1,
                            },
                        });
                    }
                    Err(TransactionError::VersionAlreadyExists(version)) => {
                        this.log_store
                            .abort_commit_entry(version, commit_or_bytes, this.operation_id)
                            .await?;
                        error!("The transaction {version} already exists, will retry!");
                        continue;
                    }
                    Err(err) => {
                        this.log_store
                            .abort_commit_entry(version, commit_or_bytes, this.operation_id)
                            .await?;
                        return Err(err.into());
                    }
                }
            }

            Err(TransactionError::MaxCommitAttempts(effective_max_retries as i32).into())
        })
    }
}

/// Represents items for the post commit hook
pub struct PostCommit {
    /// The winning version number of the commit
    pub version: i64,
    /// The data that was committed to the log store
    pub data: CommitData,
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
    log_store: LogStoreRef,
    table_data: Option<Arc<DeltaSnapshot>>,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
    metrics: CommitMetrics,
}

impl PostCommit {
    /// Build a version checksum incrementally from the previous version's CRC and the
    /// current commit's actions, without requiring a full file list.
    ///
    /// Returns `None` when the CRC should be skipped (unsupported features or deletion
    /// vectors in the commit). Falls back to [`Self::build_full_checksum_fallback`] when
    /// the previous CRC is missing, corrupt, or when a [`CommitAction::Remove`] action
    /// has no `size` field (which would otherwise produce an inaccurate `table_size_bytes`).
    async fn build_incremental_checksum(&self) -> Option<VersionChecksum> {
        let actions = &self.data.actions;

        // Resolve effective protocol and metadata from the commit actions directly.
        let base_protocol = self.table_data.as_ref().map(|s| s.protocol());
        let base_metadata = self.table_data.as_ref().map(|s| s.metadata());
        let protocol = actions
            .iter()
            .rev()
            .find_map(|a| match a {
                CommitAction::Protocol(p) => Some(p.clone()),
                _ => None,
            })
            .or_else(|| base_protocol.cloned());
        let metadata = actions
            .iter()
            .rev()
            .find_map(|a| match a {
                CommitAction::Metadata(m) => Some(m.clone()),
                _ => None,
            })
            .or_else(|| base_metadata.cloned());
        let (protocol, metadata) = match (protocol, metadata) {
            (Some(p), Some(m)) => (p, m),
            _ => {
                debug!(
                    "Skipping incremental CRC for version {}: protocol or metadata not available",
                    self.version
                );
                return None;
            }
        };

        // Skip CRC if protocol has unsupported features.
        if protocol
            .reader_features()
            .into_iter()
            .flatten()
            .chain(protocol.writer_features().into_iter().flatten())
            .any(|feature| matches!(feature, TableFeature::Unknown))
        {
            debug!(
                "Skipping incremental CRC for version {}: unknown table features",
                self.version
            );
            return None;
        }
        let reader_unsupported = PROTOCOL
            .unsupported_reader_features(&protocol)
            .map(|f| !f.is_empty())
            .unwrap_or(true);
        let writer_unsupported = PROTOCOL
            .unsupported_writer_features(&protocol)
            .map(|f| !f.is_empty())
            .unwrap_or(true);
        if reader_unsupported || writer_unsupported {
            debug!(
                "Skipping incremental CRC for version {}: unsupported protocol features",
                self.version
            );
            return None;
        }

        // Check for deletion vectors in commit actions — skip CRC if present.
        let has_dv = actions.iter().any(|a| match a {
            CommitAction::Add(add) => add.deletion_vector.is_some(),
            CommitAction::Remove(remove) => remove.deletion_vector.is_some(),
            _ => false,
        });
        if has_dv {
            debug!(
                "Skipping incremental CRC for version {}: commit contains deletion vectors",
                self.version
            );
            return None;
        }

        // Compute delta from commit actions.
        let mut delta_num_files: i64 = 0;
        let mut delta_size_bytes: i64 = 0;
        for action in actions {
            match action {
                CommitAction::Add(add) => {
                    delta_num_files = delta_num_files.checked_add(1)?;
                    delta_size_bytes = delta_size_bytes.checked_add(add.size)?;
                }
                CommitAction::Remove(remove) => {
                    delta_num_files = delta_num_files.checked_sub(1)?;
                    match remove.size {
                        Some(size) => {
                            delta_size_bytes = delta_size_bytes.checked_sub(size)?;
                        }
                        None => {
                            // Remove.size is optional per the Delta protocol; without it we
                            // cannot compute an accurate incremental table_size_bytes.
                            debug!(
                                "Incremental CRC: Remove action missing size at version {}; \
                                 falling back to full-snapshot CRC computation",
                                self.version
                            );
                            return self.build_full_checksum_fallback().await;
                        }
                    }
                }
                _ => {}
            }
        }

        // Collect txn and domain metadata updates from the commit.
        let commit_txns: Vec<Transaction> = actions
            .iter()
            .filter_map(|a| match a {
                CommitAction::Txn(txn) => Some(txn.clone()),
                _ => None,
            })
            .collect();
        let commit_domains: Vec<crate::spec::DomainMetadata> = actions
            .iter()
            .filter_map(|a| match a {
                CommitAction::DomainMetadata(dm) => Some(dm.clone()),
                _ => None,
            })
            .collect();

        // For version 0 (table creation), we don't need a previous CRC.
        if self.version == 0 {
            let mut set_transactions = commit_txns;
            set_transactions
                .sort_by(|a, b| a.app_id.cmp(&b.app_id).then(a.version.cmp(&b.version)));
            let mut domain_metadata: Vec<_> =
                commit_domains.into_iter().filter(|d| !d.removed).collect();
            domain_metadata.sort_by(|a, b| a.domain.cmp(&b.domain));

            return Some(VersionChecksum {
                txn_id: self.data.version_checksum_txn_id(),
                table_size_bytes: delta_size_bytes,
                num_files: delta_num_files,
                num_metadata: 1,
                num_protocol: 1,
                in_commit_timestamp_opt: self.data.version_checksum_in_commit_timestamp(),
                set_transactions: (!set_transactions.is_empty()).then_some(set_transactions),
                domain_metadata: (!domain_metadata.is_empty()).then_some(domain_metadata),
                metadata: metadata.clone(),
                protocol: protocol.clone(),
                file_size_histogram: None,
                all_files: None,
            });
        }

        // Read the previous version's CRC.
        let prev_version = self.version - 1;
        let store = self.log_store.object_store(None);
        let prev_crc_path = checksum_path(prev_version);
        let prev_crc_bytes = match store.get(&prev_crc_path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
                Err(err) => {
                    debug!(
                        "Incremental CRC: failed to read prev CRC bytes at version {prev_version}: {err}; \
                         falling back to full-snapshot CRC computation"
                    );
                    return self.build_full_checksum_fallback().await;
                }
            },
            Err(err) => {
                debug!(
                    "Incremental CRC: prev CRC not available at version {prev_version}: {err}; \
                     falling back to full-snapshot CRC computation"
                );
                return self.build_full_checksum_fallback().await;
            }
        };
        let prev_checksum: VersionChecksum = match serde_json::from_slice(&prev_crc_bytes) {
            Ok(c) => c,
            Err(err) => {
                debug!(
                    "Incremental CRC: failed to deserialize prev CRC at version {prev_version}: {err}; \
                     falling back to full-snapshot CRC computation"
                );
                return self.build_full_checksum_fallback().await;
            }
        };

        // Bail if previous CRC had deletion vectors (signalled by being skipped).
        // This is defensive — if the prev CRC exists, it was valid at that version.

        // Compute new totals.
        let num_files = prev_checksum.num_files.checked_add(delta_num_files)?;
        let table_size_bytes = prev_checksum
            .table_size_bytes
            .checked_add(delta_size_bytes)?;

        // Merge set_transactions: previous + commit updates (commit wins by app_id).
        let mut txn_map: HashMap<String, Transaction> = prev_checksum
            .set_transactions
            .unwrap_or_default()
            .into_iter()
            .map(|t| (t.app_id.clone(), t))
            .collect();
        for txn in commit_txns {
            txn_map.insert(txn.app_id.clone(), txn);
        }
        let mut set_transactions: Vec<Transaction> = txn_map.into_values().collect();
        set_transactions.sort_by(|a, b| a.app_id.cmp(&b.app_id).then(a.version.cmp(&b.version)));

        // Merge domain metadata: previous + commit updates (commit wins, removed=true removes).
        let mut domain_map: HashMap<String, crate::spec::DomainMetadata> = prev_checksum
            .domain_metadata
            .unwrap_or_default()
            .into_iter()
            .map(|d| (d.domain.clone(), d))
            .collect();
        for dm in commit_domains {
            if dm.removed {
                domain_map.remove(&dm.domain);
            } else {
                domain_map.insert(dm.domain.clone(), dm);
            }
        }
        let mut domain_metadata: Vec<_> = domain_map.into_values().collect();
        domain_metadata.sort_by(|a, b| a.domain.cmp(&b.domain));

        Some(VersionChecksum {
            txn_id: self.data.version_checksum_txn_id(),
            table_size_bytes,
            num_files,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt: self.data.version_checksum_in_commit_timestamp(),
            set_transactions: (!set_transactions.is_empty()).then_some(set_transactions),
            domain_metadata: (!domain_metadata.is_empty()).then_some(domain_metadata),
            metadata: metadata.clone(),
            protocol: protocol.clone(),
            file_size_histogram: None,
            all_files: None,
        })
    }

    /// Full-snapshot fallback for CRC computation.
    ///
    /// Called when the incremental chain is broken (prev CRC missing or corrupt, or a
    /// Remove action has no size). When a pre-commit snapshot (`table_data`) is available
    /// it is updated by one version (reads only the new commit file).
    async fn build_full_checksum_fallback(&self) -> Option<VersionChecksum> {
        debug!(
            "CRC full-snapshot fallback: loading full snapshot for version {}",
            self.version
        );
        // Try to reuse the pre-commit snapshot.
        let snapshot = if let Some(prev) = self
            .table_data
            .as_ref()
            .filter(|s| s.load_config().require_files)
        {
            let mut snapshot = Arc::clone(prev);
            match Arc::make_mut(&mut snapshot)
                .update(self.log_store.as_ref(), Some(self.version as u64))
                .await
            {
                Ok(()) => snapshot,
                Err(e) => {
                    debug!(
                        "CRC full-snapshot fallback: failed to advance pre-commit snapshot to v{}: {e}; \
                         falling back to fresh replay",
                        self.version
                    );
                    match DeltaSnapshot::try_new(
                        self.log_store.as_ref(),
                        DeltaSnapshotConfig::default(),
                        Some(self.version),
                        None,
                    )
                    .await
                    {
                        Ok(s) => Arc::new(s),
                        Err(err) => {
                            debug!(
                                "CRC full-snapshot fallback: fresh replay also failed for version {}: {err}",
                                self.version
                            );
                            return None;
                        }
                    }
                }
            }
        } else {
            match DeltaSnapshot::try_new(
                self.log_store.as_ref(),
                DeltaSnapshotConfig::default(), // require_files: true
                Some(self.version),
                None,
            )
            .await
            {
                Ok(s) => Arc::new(s),
                Err(err) => {
                    debug!(
                        "CRC full-snapshot fallback: failed to load snapshot for version {}: {err}",
                        self.version
                    );
                    return None;
                }
            }
        };
        match snapshot.build_version_checksum(
            self.data.version_checksum_txn_id(),
            self.data.version_checksum_in_commit_timestamp(),
        ) {
            Ok(Some(checksum)) => {
                debug!(
                    "CRC full-snapshot fallback: succeeded for version {}",
                    self.version
                );
                Some(checksum)
            }
            Ok(None) => None,
            Err(err) => {
                debug!(
                    "CRC full-snapshot fallback: build_version_checksum failed for version {}: {err}",
                    self.version
                );
                None
            }
        }
    }

    async fn write_version_checksum_incremental(&self, operation_id: Uuid) {
        // Check table property via snapshot or commit actions.
        let write_checksum_enabled = if let Some(s) = self.table_data.as_ref() {
            s.table_properties().write_checksum_file_enabled()
        } else {
            // New table creation: check the metadata action in the commit for the property.
            self.data
                .actions
                .iter()
                .find_map(|a| match a {
                    CommitAction::Metadata(m) => {
                        let props = crate::spec::TableProperties::from(m.configuration().iter());
                        Some(props.write_checksum_file_enabled())
                    }
                    _ => None,
                })
                .unwrap_or(true) // Default: enabled for new tables.
        };
        if !write_checksum_enabled {
            debug!(
                "Skipping version checksum for version {} because delta.writeChecksumFile.enabled=false",
                self.version
            );
            return;
        }

        let checksum = match self.build_incremental_checksum().await {
            Some(c) => c,
            None => {
                debug!(
                    "Skipping version checksum for version {} (incremental CRC unavailable)",
                    self.version
                );
                return;
            }
        };

        let crc_path = checksum_path(self.version);
        let checksum_bytes = match serde_json::to_vec(&checksum) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "Failed to serialize version checksum for version {}: {}",
                    self.version, err
                );
                return;
            }
        };

        let put_result = self
            .log_store
            .object_store(Some(operation_id))
            .put_opts(
                &crc_path,
                Bytes::from(checksum_bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;

        match put_result {
            Ok(_) => {
                debug!(
                    "Wrote version checksum for version {} to {}",
                    self.version, crc_path
                );
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                warn!(
                    "Version checksum already exists for version {} at {}",
                    self.version, crc_path
                );
            }
            Err(err) => {
                warn!(
                    "Failed to write version checksum for version {} to {}: {}",
                    self.version, crc_path, err
                );
            }
        }
    }
}

fn should_create_checkpoint(version: i64, checkpoint_interval: i64) -> bool {
    version != 0 && version % checkpoint_interval == 0
}

/// A commit that successfully completed
pub struct FinalizedCommit {
    /// The new table state after a commit, if available.
    ///
    /// `None` when the post-commit state could not be loaded (e.g. transient I/O
    /// error after the commit entry was durably written). The commit itself
    /// succeeded regardless.
    pub snapshot: Option<Arc<DeltaSnapshot>>,

    /// Version of the finalized commit
    pub version: i64,

    /// Metrics associated with the commit operation
    pub metrics: Metrics,
}
impl FinalizedCommit {
    /// The new table state after a commit
    #[expect(dead_code)]
    pub fn snapshot(&self) -> Option<Arc<DeltaSnapshot>> {
        self.snapshot.clone()
    }
    /// Version of the finalized commit
    #[expect(dead_code)]
    pub fn version(&self) -> i64 {
        self.version
    }
}

impl std::future::IntoFuture for PostCommit {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let post_commit_operation_id = Uuid::new_v4();

            let state: Option<Arc<DeltaSnapshot>> = match DeltaSnapshot::try_new(
                this.log_store.as_ref(),
                DeltaSnapshotConfig {
                    require_files: false,
                    ..Default::default()
                },
                Some(this.version),
                None,
            )
            .await
            {
                Ok(s) => Some(Arc::new(s)),
                Err(e) => {
                    warn!(
                            "Post-commit: failed to load state for version {} (post-commit activities skipped): {e}",
                            this.version
                        );
                    None
                }
            };

            let cleanup_logs_setting = this.cleanup_expired_logs.unwrap_or_else(|| {
                state
                    .as_ref()
                    .map(|s| s.table_properties().enable_expired_log_cleanup())
                    .unwrap_or(false) // conservative: skip cleanup if state unavailable
            });
            let will_create_checkpoint = this.create_checkpoint
                && state.as_ref().is_some_and(|s| {
                    should_create_checkpoint(
                        this.version,
                        s.table_properties().checkpoint_interval().get() as i64,
                    )
                });

            let compaction_info = state
                .as_ref()
                .and_then(|s| s.table_properties().log_compaction_interval())
                .filter(|&interval| should_create_compaction(this.version, interval));

            this.write_version_checksum_incremental(post_commit_operation_id)
                .await;

            // --- Post-commit heavy work (checkpoint, cleanup, compaction) ---
            // These run inline so that callers observe completed artifacts (e.g.
            // checkpoint files) when the commit future resolves.

            // before hook — best-effort: the commit entry has already been durably written
            // to the log, so a hook failure here does not roll back the transaction.
            if let Some(handler) = &this.custom_execute_handler {
                if let Err(e) = handler
                    .before_post_commit_hook(
                        &this.log_store,
                        will_create_checkpoint,
                        post_commit_operation_id,
                    )
                    .await
                {
                    warn!(
                        "before_post_commit_hook failed for version {}: {e}",
                        this.version
                    );
                }
            }

            // Checkpoint — best-effort: checkpoint creation is a performance optimization
            // (the log can always be replayed from scratch). A failure is logged but does
            // not cause the commit to be reported as failed.
            let mut checkpoint_created = false;
            if will_create_checkpoint {
                info!("Creating checkpoint for version {}", this.version);
                match create_checkpoint_for(
                    this.version,
                    this.log_store.as_ref(),
                    post_commit_operation_id,
                )
                .await
                {
                    Ok(()) => checkpoint_created = true,
                    Err(e) => {
                        warn!(
                            "Failed to create checkpoint for version {}: {e}",
                            this.version
                        );
                    }
                }
            }

            // Log cleanup
            let mut num_log_files_cleaned_up: u64 = 0;
            if cleanup_logs_setting && checkpoint_created {
                if let Some(s) = state.as_ref() {
                    let retention_millis =
                        i64::try_from(s.table_properties().log_retention_duration().as_millis())
                            .unwrap_or(i64::MAX);
                    let cutoff_timestamp = (Utc::now().timestamp_millis() - retention_millis)
                        .div_euclid(24 * 60 * 60 * 1000)
                        * (24 * 60 * 60 * 1000);
                    match cleanup_expired_delta_log_files(
                        s.as_ref(),
                        this.log_store.as_ref(),
                        cutoff_timestamp,
                        Some(post_commit_operation_id),
                    )
                    .await
                    {
                        Ok(n) => num_log_files_cleaned_up = n as u64,
                        Err(e) => {
                            warn!(
                                "Failed to clean up expired log files for version {}: {e}",
                                this.version
                            );
                        }
                    }
                }
            }

            // Log compaction
            if let Some(compaction_interval) = compaction_info {
                let start_version = this.version + 1 - compaction_interval as i64;
                let retention_millis = i64::try_from(
                    state
                        .as_ref()
                        .map(|s| {
                            s.table_properties()
                                .deleted_file_retention_duration()
                                .as_millis()
                        })
                        .unwrap_or_default(),
                )
                .unwrap_or(i64::MAX);
                let min_file_retention_ts = Utc::now().timestamp_millis() - retention_millis;
                if let Err(e) = create_log_compaction_for(
                    start_version,
                    this.version,
                    this.log_store.as_ref(),
                    min_file_retention_ts,
                )
                .await
                {
                    warn!(
                        "Failed to create log compaction for versions {} to {}: {e}",
                        start_version, this.version
                    );
                }
            }

            // after hook
            if let Some(handler) = &this.custom_execute_handler {
                if let Err(e) = handler
                    .after_post_commit_hook(
                        &this.log_store,
                        checkpoint_created,
                        post_commit_operation_id,
                    )
                    .await
                {
                    warn!(
                        "after_post_commit_hook failed for version {}: {e}",
                        this.version
                    );
                }
            }

            Ok(FinalizedCommit {
                snapshot: state,
                version: this.version,
                metrics: Metrics {
                    num_retries: this.metrics.num_retries,
                    new_checkpoint_created: checkpoint_created,
                    num_log_files_cleaned_up,
                },
            })
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use url::Url;

    use super::*;
    use crate::schema::protocol_for_create;
    use crate::spec::{
        checksum_path, Action, CommitAction, CommitInfo, DataType, DeltaError, DomainMetadata,
        Metadata, Protocol, SaveMode, StructField, StructType, TableFeature, VersionChecksum,
    };
    use crate::storage::{default_logstore, get_actions, StorageConfig};

    fn test_log_store(store: Arc<dyn ObjectStore>) -> LogStoreRef {
        default_logstore(
            store.clone(),
            store,
            &Url::parse("memory:///").unwrap(),
            &StorageConfig,
        )
    }

    fn test_metadata(
        configuration: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Metadata {
        Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)]).unwrap(),
            Vec::new(),
            0,
            configuration
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
        .unwrap()
    }

    async fn read_commit_actions(log_store: &LogStoreRef, version: i64) -> Vec<Action> {
        let bytes = log_store.read_commit_entry(version).await.unwrap().unwrap();
        get_actions(version, &bytes).unwrap()
    }

    async fn read_version_checksum(log_store: &LogStoreRef, version: i64) -> VersionChecksum {
        let bytes = log_store
            .object_store(None)
            .get(&checksum_path(version))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn commit_info(actions: &[Action]) -> DeltaResult<&CommitInfo> {
        match actions.first() {
            Some(Action::CommitInfo(info)) => Ok(info),
            _ => Err(DeltaError::generic("expected commitInfo action at index 0")),
        }
    }

    fn commit_info_action(actions: &[CommitAction]) -> DeltaResult<&CommitInfo> {
        match actions.first() {
            Some(CommitAction::CommitInfo(info)) => Ok(info),
            _ => Err(DeltaError::generic("expected commitInfo action at index 0")),
        }
    }

    #[tokio::test]
    async fn commit_writes_commit_info_first_monotonic_ict_and_checksum() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store);
        let protocol = protocol_for_create(false, false, true, false, &HashMap::new())?;
        let metadata = test_metadata([("delta.enableInCommitTimestamps", "true")]);

        let created = CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store.clone(),
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: "memory:///".to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await?;
        let first_actions = read_commit_actions(&log_store, 0).await;
        let first_commit_info = commit_info(&first_actions)?;
        let first_ict = first_commit_info.in_commit_timestamp.ok_or_else(|| {
            DeltaError::generic("ICT-enabled create commit should write inCommitTimestamp")
        })?;
        assert!(matches!(first_actions.first(), Some(Action::CommitInfo(_))));
        assert_eq!(
            read_version_checksum(&log_store, 0)
                .await
                .in_commit_timestamp_opt,
            Some(first_ict)
        );

        let appended = CommitBuilder::default()
            .with_actions(vec![])
            .build(
                created.snapshot.clone(),
                log_store.clone(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;
        let second_actions = read_commit_actions(&log_store, appended.version).await;
        let second_commit_info = commit_info(&second_actions)?;
        let second_ict = second_commit_info.in_commit_timestamp.ok_or_else(|| {
            DeltaError::generic("ICT-enabled append commit should write inCommitTimestamp")
        })?;

        assert!(matches!(
            second_actions.first(),
            Some(Action::CommitInfo(_))
        ));
        assert!(second_ict > first_ict);
        assert_eq!(
            read_version_checksum(&log_store, appended.version)
                .await
                .in_commit_timestamp_opt,
            Some(second_ict)
        );
        Ok(())
    }

    #[tokio::test]
    async fn finalize_attempt_actions_backfills_enablement_metadata() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store);
        let protocol = protocol_for_create(false, false, false, false, &HashMap::new())?;
        let metadata = test_metadata([]);
        let created = CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store.clone(),
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: "memory:///".to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await?;
        let previous_timestamp = created
            .snapshot
            .as_ref()
            .unwrap()
            .version_timestamp(0)
            .ok_or_else(|| {
                DeltaError::generic("non-ICT tables still track pre-enable commit timestamps")
            })?;

        let upgrade_protocol = protocol_for_create(false, false, true, false, &HashMap::new())?;
        let upgrade_metadata = test_metadata([("delta.enableInCommitTimestamps", "true")]);
        let base_actions = CommitData::new(
            vec![
                CommitAction::Protocol(upgrade_protocol),
                CommitAction::Metadata(upgrade_metadata),
            ],
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
            HashMap::new(),
            OperationMetrics::default(),
            vec![],
            None,
        )
        .actions;

        let finalized_actions = finalize_attempt_actions(
            &base_actions,
            created.snapshot.as_ref(),
            1,
            Some(previous_timestamp),
            previous_timestamp.saturating_sub(10),
        )?;
        let commit_info = commit_info_action(&finalized_actions)?;
        let upgrade_timestamp = commit_info
            .in_commit_timestamp
            .ok_or_else(|| DeltaError::generic("upgrade commit should assign inCommitTimestamp"))?;
        assert_eq!(upgrade_timestamp, previous_timestamp + 1);

        let metadata = finalized_actions
            .iter()
            .find_map(|action| match action {
                CommitAction::Metadata(metadata) => Some(metadata),
                _ => None,
            })
            .ok_or_else(|| DeltaError::generic("upgrade commit should keep metadata action"))?;
        assert_eq!(
            metadata
                .configuration()
                .get("delta.inCommitTimestampEnablementVersion"),
            Some(&"1".to_string())
        );
        assert_eq!(
            metadata
                .configuration()
                .get("delta.inCommitTimestampEnablementTimestamp"),
            Some(&upgrade_timestamp.to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_commit_rejects_unsupported_reader_features() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store);
        // VacuumProtocolCheck is a reader-writer feature that we does not yet support.
        // Use it to verify that the commit pipeline correctly rejects unsupported features.
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::VacuumProtocolCheck]),
            Some(vec![TableFeature::VacuumProtocolCheck]),
        );
        let metadata = test_metadata([]);

        let result = CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store,
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: "memory:///".to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await;
        assert!(
            result.is_err(),
            "create commit should reject unsupported reader features"
        );
        let err = match result {
            Err(err) => err,
            Ok(_) => return,
        };

        assert!(matches!(
            err,
            DeltaError::Transaction(TransactionError::UnsupportedTableFeatures(features))
                if features.contains(&TableFeature::VacuumProtocolCheck)
        ));
    }

    #[tokio::test]
    async fn commit_rejects_timestamp_ntz_schema_without_protocol_feature() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store);
        let protocol = protocol_for_create(false, false, false, false, &HashMap::new())?;
        let metadata = test_metadata([]);
        let created = CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store.clone(),
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: "memory:///".to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await?;

        let updated_schema =
            StructType::try_new([StructField::not_null("ts", DataType::TIMESTAMP_NTZ)])?;
        let snap = created.snapshot.as_ref().unwrap();
        let updated_metadata = snap.metadata().clone().with_schema(&updated_schema)?;

        let result = CommitBuilder::default()
            .with_actions(vec![CommitAction::Metadata(updated_metadata)])
            .build(
                created.snapshot.clone(),
                log_store,
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "commit should reject timestamp_ntz without protocol support"
        );
        let err = match result {
            Err(err) => err,
            Ok(_) => return Ok(()),
        };

        assert!(matches!(
            err,
            DeltaError::Transaction(TransactionError::TableFeaturesRequired(
                TableFeature::TimestampWithoutTimezone
            ))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn commit_rejects_domain_metadata_actions_without_protocol_feature() -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store);
        let protocol = protocol_for_create(false, false, false, false, &HashMap::new())?;
        let metadata = test_metadata([]);
        let created = CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ])
            .build(
                None,
                log_store.clone(),
                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: "memory:///".to_string(),
                    protocol: Box::new(protocol),
                    metadata: Box::new(metadata),
                },
            )
            .await?;

        let result = CommitBuilder::default()
            .with_actions(vec![CommitAction::DomainMetadata(DomainMetadata {
                domain: "delta.rowTracking".to_string(),
                configuration: r#"{"rowIdHighWaterMark":1}"#.to_string(),
                removed: false,
            })])
            .build(
                created.snapshot.clone(),
                log_store,
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "commit should reject domain metadata without protocol support"
        );
        let err = match result {
            Err(err) => err,
            Ok(_) => return Ok(()),
        };

        assert!(matches!(
            err,
            DeltaError::Transaction(TransactionError::TableFeaturesRequired(
                TableFeature::DomainMetadata
            ))
        ));
        Ok(())
    }
}
