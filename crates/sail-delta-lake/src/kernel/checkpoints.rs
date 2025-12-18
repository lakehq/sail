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

use std::sync::LazyLock;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::FileMeta;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use regex::Regex;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::kernel::snapshot::stream::RecordBatchReceiverStreamBuilder;
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::storage::LogStore;

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

fn to_rb(data: FilteredEngineData) -> DeltaResult<RecordBatch> {
    let (underlying_data, selection_vector) = data.into_parts();
    let engine_data = ArrowEngineData::try_from_engine_data(underlying_data)?;
    let predicate = BooleanArray::from(selection_vector);
    let batch = filter_record_batch(engine_data.record_batch(), &predicate)?;
    Ok(batch)
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

        let mut table_root = self.log_store.config().location.clone();
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }

        let engine = self.log_store.engine(Some(self.operation_id));
        let version_u64 = version as u64;

        let snapshot = spawn_blocking(move || {
            KernelSnapshot::builder_for(table_root)
                .at_version(version_u64)
                .build(engine.as_ref())
        })
        .await
        .map_err(|e| DeltaTableError::generic(e.to_string()))??;

        let cp_writer = snapshot.checkpoint()?;
        let cp_url = cp_writer.checkpoint_path()?;
        let cp_path = Path::from_url_path(cp_url.path())?;

        // Prepare checkpoint data iterator (sync) in the kernel engine.
        let engine = self.log_store.engine(Some(self.operation_id));
        let mut cp_data = cp_writer.checkpoint_data(engine.as_ref())?;

        // Pull the first batch (for schema), but keep the iterator for the producer thread.
        let (first_batch, mut cp_data_after_first) = spawn_blocking(move || {
            let Some(first) = cp_data.next() else {
                return Err(DeltaTableError::generic("No checkpoint data".to_string()));
            };
            Ok::<_, DeltaTableError>((to_rb(first?)?, cp_data))
        })
        .await
        .map_err(|e| DeltaTableError::generic(e.to_string()))??;

        let checkpoint_schema: SchemaRef = first_batch.schema();

        // Start writer (consumer) immediately.
        let root_store = self.log_store.root_object_store(Some(self.operation_id));
        let object_store_writer = ParquetObjectWriter::new(root_store.clone(), cp_path.clone());
        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, checkpoint_schema.clone(), None)
                .map_err(DeltaTableError::generic_err)?;

        writer
            .write(&first_batch)
            .await
            .map_err(DeltaTableError::generic_err)?;

        // Stream remaining batches from a blocking producer thread to the async writer.
        let mut rb_builder = RecordBatchReceiverStreamBuilder::new(4);
        let tx = rb_builder.tx();
        let (cp_data_done_tx, cp_data_done_rx) = oneshot::channel();

        rb_builder.spawn_blocking(move || {
            for next in cp_data_after_first.by_ref() {
                let batch: DeltaResult<RecordBatch> =
                    next.map_err(DeltaTableError::from).and_then(to_rb);
                if tx.blocking_send(batch).is_err() {
                    break; // consumer dropped
                }
            }

            // Return the exhausted iterator (it contains kernel-side stats used by finalize).
            let _ = cp_data_done_tx.send(cp_data_after_first);
            Ok(())
        });

        let mut batch_stream = rb_builder.build();
        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            let batch = if batch.schema() != checkpoint_schema {
                cast_record_batch_relaxed_tz(&batch, &checkpoint_schema)?
            } else {
                batch
            };
            writer
                .write(&batch)
                .await
                .map_err(DeltaTableError::generic_err)?;
        }

        let _pq_meta = writer.close().await.map_err(DeltaTableError::generic_err)?;

        let file_meta = root_store.head(&cp_path).await?;
        let file_meta = FileMeta {
            location: cp_url,
            size: file_meta.size,
            last_modified: file_meta.last_modified.timestamp_millis(),
        };

        let cp_data_final = cp_data_done_rx
            .await
            .map_err(|_| DeltaTableError::generic("checkpoint producer dropped unexpectedly"))?;

        let engine = self.log_store.engine(Some(self.operation_id));
        spawn_blocking(move || cp_writer.finalize(engine.as_ref(), &file_meta, cp_data_final))
            .await
            .map_err(|e| DeltaTableError::generic(e.to_string()))??;

        Ok(())
    }
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
