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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::sync::mpsc;

use super::config::{PartitioningMode, WriterConfig};
use super::demux::{AutoPartitioner, ContiguousPartitioner, HashPartitioner, Partitioner};
use super::parquet::DeltaParquetFileWriter;
use super::path::{build_data_path, PartFileNamer, PartitionsExt};
use super::stats::create_add;
use crate::kernel::models::Add;
use crate::kernel::DeltaTableError;

/// Orchestrates partitioned, rolling parquet file writes and returns `Add` actions.
pub struct DeltaWriteOrchestrator {
    object_store: Arc<dyn ObjectStore>,
    table_path: Path,
    config: WriterConfig,
    partitioner: Box<dyn Partitioner>,
    partitions: HashMap<String, PartitionHandle>,
}

impl DeltaWriteOrchestrator {
    pub fn new(object_store: Arc<dyn ObjectStore>, table_path: Path, config: WriterConfig) -> Self {
        let partitioner: Box<dyn Partitioner> = match config.partitioning_mode {
            PartitioningMode::Contiguous => Box::new(ContiguousPartitioner),
            PartitioningMode::Hash => Box::new(HashPartitioner),
            PartitioningMode::Auto => Box::new(AutoPartitioner::default()),
        };

        Self {
            object_store,
            table_path,
            config,
            partitioner,
            partitions: HashMap::new(),
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        let file_schema = self.config.file_schema();

        let partitioned = self.partitioner.partition(
            file_schema,
            &self.config.partition_columns,
            &self.config.physical_partition_columns,
            batch,
        )?;

        for part in partitioned {
            let partition_key = part.partition_values.hive_partition_path();
            let handle = match self.partitions.entry(partition_key.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let partition_segments = part.partition_values.hive_partition_segments();
                    let handle = PartitionHandle::spawn(
                        Arc::clone(&self.object_store),
                        self.table_path.clone(),
                        self.config.clone(),
                        partition_segments,
                        part.partition_values.clone(),
                    )?;
                    entry.insert(handle)
                }
            };

            // Send batch to writer task; backpressure via bounded channel.
            handle.tx.send(part.record_batch).await.map_err(|_| {
                DeltaTableError::generic("partition writer task terminated".to_string())
            })?;
        }

        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<Add>, DeltaTableError> {
        let mut all_actions = Vec::new();
        for (_, handle) in self.partitions.drain() {
            let PartitionHandle { tx, join } = handle;
            // Dropping the sender closes the channel, allowing the task to finish.
            drop(tx);
            let actions = join
                .join_unwind()
                .await
                .map_err(|e| DeltaTableError::generic(format!("writer task join error: {e}")))??;
            all_actions.extend(actions);
        }

        Ok(all_actions)
    }
}

struct PartitionHandle {
    tx: mpsc::Sender<RecordBatch>,
    join: SpawnedTask<Result<Vec<Add>, DeltaTableError>>,
}

impl PartitionHandle {
    fn spawn(
        object_store: Arc<dyn ObjectStore>,
        table_path: Path,
        config: WriterConfig,
        partition_segments: Vec<String>,
        partition_values: IndexMap<String, Scalar>,
    ) -> Result<Self, DeltaTableError> {
        let (tx, mut rx) = mpsc::channel::<RecordBatch>(128);

        let join = SpawnedTask::spawn(async move {
            let mut writer = RollingPartitionWriter::try_new(
                object_store,
                table_path,
                config,
                partition_segments,
                partition_values,
            )?;

            while let Some(batch) = rx.recv().await {
                writer.write(&batch).await?;
            }

            writer.close().await
        });

        Ok(Self { tx, join })
    }
}

struct RollingPartitionWriter {
    object_store: Arc<dyn ObjectStore>,
    table_path: Path,
    config: WriterConfig,
    partition_segments: Vec<String>,
    partition_values: IndexMap<String, Scalar>,
    namer: PartFileNamer,
    current: Option<(String, Path, DeltaParquetFileWriter)>, // (relative_path, full_path, writer)
    adds: Vec<Add>,
}

impl RollingPartitionWriter {
    fn try_new(
        object_store: Arc<dyn ObjectStore>,
        table_path: Path,
        config: WriterConfig,
        partition_segments: Vec<String>,
        partition_values: IndexMap<String, Scalar>,
    ) -> Result<Self, DeltaTableError> {
        Ok(Self {
            object_store,
            table_path,
            config,
            partition_segments,
            partition_values,
            namer: PartFileNamer::new(),
            current: None,
            adds: Vec::new(),
        })
    }

    fn ensure_current_writer(&mut self) -> Result<(), DeltaTableError> {
        if self.current.is_some() {
            return Ok(());
        }

        let file_name = self.namer.next_file_name(&self.config.writer_properties);
        let (relative_path, full_path) =
            build_data_path(&self.table_path, &self.partition_segments, &file_name);

        let file_writer = DeltaParquetFileWriter::try_new(
            Arc::clone(&self.object_store),
            &full_path,
            self.config.file_schema(),
            self.config.writer_properties.clone(),
            self.config.objectstore_writer_buffer_size,
            self.config.skip_arrow_metadata,
        )?;

        self.current = Some((relative_path, full_path, file_writer));
        Ok(())
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        let max_offset = batch.num_rows();
        for offset in (0..max_offset).step_by(self.config.write_batch_size) {
            // We may flush mid-loop; ensure we always have an active writer.
            self.ensure_current_writer()?;

            let length = usize::min(self.config.write_batch_size, max_offset - offset);
            let slice = batch.slice(offset, length);

            let (_, _, writer) = self
                .current
                .as_mut()
                .ok_or_else(|| DeltaTableError::generic("writer not initialized".to_string()))?;

            if slice.schema() != *writer.schema() {
                return Err(DeltaTableError::generic(format!(
                    "Schema mismatch: expected {:?}, got {:?}",
                    writer.schema(),
                    slice.schema()
                )));
            }

            writer.write(&slice).await?;

            let estimated_size = writer
                .bytes_written()
                .saturating_add(writer.in_progress_size());
            if estimated_size >= self.config.target_file_size {
                self.flush_current().await?;
            }
        }

        Ok(())
    }

    async fn flush_current(&mut self) -> Result<(), DeltaTableError> {
        let (relative_path, _full_path, file_writer) = match self.current.take() {
            Some(v) => v,
            None => return Ok(()),
        };

        let bytes_counter = file_writer.bytes_counter();
        let metadata = file_writer.close().await?;

        // Skip empty files (keep behavior).
        if metadata.file_metadata().num_rows() == 0 {
            return Ok(());
        }

        let file_size = bytes_counter.bytes() as i64;

        let add_action = create_add(
            &self.partition_values,
            relative_path,
            file_size,
            &metadata,
            self.config.num_indexed_cols,
            &self.config.stats_columns,
        )?;
        self.adds.push(add_action);

        Ok(())
    }

    async fn close(mut self) -> Result<Vec<Add>, DeltaTableError> {
        // Flush any in-progress file.
        self.flush_current().await?;
        Ok(self.adds)
    }
}
