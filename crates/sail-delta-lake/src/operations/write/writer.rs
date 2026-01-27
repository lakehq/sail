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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/writer.rs>
// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/writer/record_batch.rs>

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use uuid::Uuid;

use super::async_utils::AsyncShareableBuffer;
use super::partitioning::partition_ranges;
use super::stats::create_add;
use crate::kernel::models::{Add, ScalarExt};
use crate::kernel::DeltaTableError;

/// Trait for creating hive partition paths from partition values
pub trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
    fn hive_partition_segments(&self) -> Vec<String>;
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        self.hive_partition_segments().join("/")
    }

    fn hive_partition_segments(&self) -> Vec<String> {
        if self.is_empty() {
            return vec![];
        }

        self.iter()
            .map(|(k, v)| {
                let value_str = v.serialize_encoded();
                format!("{k}={value_str}")
            })
            .collect()
    }
}

/// Configuration for the DeltaWriter
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Schema of the delta table
    pub table_schema: ArrowSchemaRef,
    /// Logical column names the table is partitioned by (used for metadata/actions)
    pub partition_columns: Vec<String>,
    /// Physical column names for partition columns in the input batch/schema
    pub physical_partition_columns: Vec<String>,
    /// Properties passed to underlying parquet writer
    pub writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk
    pub target_file_size: u64,
    /// Row chunks passed to parquet writer
    pub write_batch_size: usize,
    /// Number of indexed columns for statistics
    pub num_indexed_cols: i32,
    /// Specific columns to collect stats from
    pub stats_columns: Option<Vec<String>>,
}

impl WriterConfig {
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        physical_partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: u64,
        write_batch_size: usize,
        num_indexed_cols: i32,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        let writer_properties = writer_properties.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()
        });

        Self {
            table_schema,
            partition_columns,
            physical_partition_columns,
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
        }
    }

    /// Schema of files written to disk (without partition columns)
    pub fn file_schema(&self) -> ArrowSchemaRef {
        arrow_schema_without_partitions(&self.table_schema, &self.physical_partition_columns)
    }
}

/// Main DeltaWriter for writing data to Delta tables
pub struct DeltaWriter {
    /// Object store for writing files
    object_store: Arc<dyn ObjectStore>,
    /// Table root path
    table_path: Path,
    /// Writer configuration
    config: WriterConfig,
    /// Current active partition key (hive partition path)
    current_partition_key: Option<String>,
    /// Current active writer (at most one open writer per task)
    current_writer: Option<PartitionWriter>,
    /// Actions produced by completed partition writers
    completed_actions: Vec<Add>,
    /// Partition keys that have been closed (debug-only contract enforcement)
    #[cfg(debug_assertions)]
    closed_partition_keys: std::collections::HashSet<String>,
}

impl DeltaWriter {
    pub fn new(object_store: Arc<dyn ObjectStore>, table_path: Path, config: WriterConfig) -> Self {
        Self {
            object_store,
            table_path,
            config,
            current_partition_key: None,
            current_writer: None,
            completed_actions: Vec::new(),
            #[cfg(debug_assertions)]
            closed_partition_keys: std::collections::HashSet::new(),
        }
    }

    /// Write a record batch to the appropriate partition
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let file_schema = self.config.file_schema();
        let data_indices: Vec<usize> = file_schema
            .fields()
            .iter()
            .map(|f| {
                batch.schema().index_of(f.name()).map_err(|_| {
                    DeltaTableError::schema(format!("Column {} not found in batch", f.name()))
                })
            })
            .collect::<Result<_, _>>()?;

        let ranges = partition_ranges(
            &self.config.partition_columns,
            &self.config.physical_partition_columns,
            batch,
        )?;

        for range in ranges {
            let len = range.end.saturating_sub(range.start);
            if len == 0 {
                continue;
            }

            let partition_key = range.partition_values.hive_partition_path();
            self.switch_partition_if_needed(partition_key, range.partition_values)
                .await?;

            let slice = batch.slice(range.start, len);
            let record_batch = record_batch_without_partitions_projected(
                &slice,
                file_schema.clone(),
                &data_indices,
            )?;

            if let Some(writer) = self.current_writer.as_mut() {
                writer.write(&record_batch).await?;
            } else {
                return Err(DeltaTableError::generic(
                    "internal error: current writer not initialized".to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn switch_partition_if_needed(
        &mut self,
        partition_key: String,
        partition_values: IndexMap<String, Scalar>,
    ) -> Result<(), DeltaTableError> {
        if self.current_partition_key.as_deref() == Some(partition_key.as_str())
            && self.current_writer.is_some()
        {
            return Ok(());
        }

        // Close current writer if any
        if let Some(writer) = self.current_writer.take() {
            let actions = writer.close().await?;
            self.completed_actions.extend(actions);

            if let Some(old_key) = self.current_partition_key.take() {
                #[cfg(debug_assertions)]
                self.closed_partition_keys.insert(old_key);
            }
        }

        #[cfg(debug_assertions)]
        debug_assert!(
            self.config.partition_columns.is_empty()
                || !self.closed_partition_keys.contains(&partition_key),
            "input violated partition grouping contract: partition key re-appeared after being closed: {partition_key}"
        );

        self.current_partition_key = Some(partition_key);

        let config = PartitionWriterConfig::new(
            self.table_path.clone(),
            self.config.file_schema(),
            partition_values,
            self.config.writer_properties.clone(),
            self.config.target_file_size,
            self.config.write_batch_size,
        );

        let writer = PartitionWriter::try_with_config(
            self.object_store.clone(),
            config,
            self.config.num_indexed_cols,
            self.config.stats_columns.clone(),
        )?;

        self.current_writer = Some(writer);
        Ok(())
    }

    /// Close the writer and get the Add actions
    pub async fn close(mut self) -> Result<Vec<Add>, DeltaTableError> {
        if let Some(writer) = self.current_writer.take() {
            let actions = writer.close().await?;
            self.completed_actions.extend(actions);
        }

        Ok(self.completed_actions)
    }
}

/// Configuration for partition writers
#[derive(Debug, Clone)]
pub struct PartitionWriterConfig {
    /// Table root path
    pub table_path: Path,
    /// Schema of the data written to disk
    pub file_schema: ArrowSchemaRef,
    /// Partition path segments
    pub partition_segments: Vec<String>,
    /// Values for all partition columns
    pub partition_values: IndexMap<String, Scalar>,
    /// Properties passed to underlying parquet writer
    pub writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk
    pub target_file_size: u64,
    /// Row chunks passed to parquet writer
    pub write_batch_size: usize,
}

impl PartitionWriterConfig {
    pub fn new(
        table_path: Path,
        file_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
        writer_properties: WriterProperties,
        target_file_size: u64,
        write_batch_size: usize,
    ) -> Self {
        let partition_segments = partition_values.hive_partition_segments();

        Self {
            table_path,
            file_schema,
            partition_segments,
            partition_values,
            writer_properties,
            target_file_size,
            write_batch_size,
        }
    }
}

pub struct PartitionWriter {
    object_store: Arc<dyn ObjectStore>,
    writer_id: Uuid,
    config: PartitionWriterConfig,
    buffer: Option<AsyncShareableBuffer>,
    arrow_writer: Option<AsyncArrowWriter<AsyncShareableBuffer>>,
    part_counter: usize,
    files_written: Vec<Add>,
    #[allow(dead_code)]
    num_indexed_cols: i32,
    #[allow(dead_code)]
    stats_columns: Option<Vec<String>>,
}

impl PartitionWriter {
    pub fn try_with_config(
        object_store: Arc<dyn ObjectStore>,
        config: PartitionWriterConfig,
        num_indexed_cols: i32,
        stats_columns: Option<Vec<String>>,
    ) -> Result<Self, DeltaTableError> {
        let buffer = AsyncShareableBuffer::default();
        let arrow_writer = AsyncArrowWriter::try_new(
            buffer.clone(),
            config.file_schema.clone(),
            Some(config.writer_properties.clone()),
        )
        .map_err(|e| DeltaTableError::generic(format!("Failed to create arrow writer: {e}")))?;

        Ok(Self {
            object_store,
            writer_id: Uuid::new_v4(),
            config,
            buffer: Some(buffer),
            arrow_writer: Some(arrow_writer),
            part_counter: 0,
            files_written: Vec::new(),
            num_indexed_cols,
            stats_columns,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        if batch.schema() != self.config.file_schema {
            return Err(DeltaTableError::generic(format!(
                "Schema mismatch: expected {:?}, got {:?}",
                self.config.file_schema,
                batch.schema()
            )));
        }

        let max_offset = batch.num_rows();
        for offset in (0..max_offset).step_by(self.config.write_batch_size) {
            let length = usize::min(self.config.write_batch_size, max_offset - offset);
            let slice = batch.slice(offset, length);
            if let Some(writer) = self.arrow_writer.as_mut() {
                writer.write(&slice).await.map_err(|e| {
                    DeltaTableError::generic(format!("Failed to write batch slice: {e}"))
                })?;
            }

            // Check if need to flush after writing the slice
            let buffer_len = if let Some(buffer) = self.buffer.as_ref() {
                buffer.len().await
            } else {
                0
            };
            let in_progress_size = if let Some(writer) = self.arrow_writer.as_ref() {
                writer.in_progress_size()
            } else {
                0
            };
            let estimated_size: u64 = buffer_len as u64 + in_progress_size as u64;

            if estimated_size >= self.config.target_file_size {
                self.flush_writer().await?;
            }
        }

        Ok(())
    }

    /// Flush the current writer and create a new file
    async fn flush_writer(&mut self) -> Result<(), DeltaTableError> {
        let writer = self
            .arrow_writer
            .take()
            .ok_or_else(|| DeltaTableError::generic("Arrow writer not available".to_string()))?;
        let buffer = self
            .buffer
            .take()
            .ok_or_else(|| DeltaTableError::generic("Buffer not available".to_string()))?;

        let metadata = writer
            .close()
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to close arrow writer: {e}")))?;

        // Skip empty files
        if metadata.file_metadata().num_rows() == 0 {
            self.reset_writer()?;
            return Ok(());
        }

        let buffer_data = match buffer.into_inner().await {
            Some(buffer) => Bytes::from(buffer),
            None => return Ok(()), // Nothing to write
        };

        // Generate file path, returning both relative and full paths
        let (relative_path, full_path) = self.next_data_path();
        let file_size = buffer_data.len() as i64;

        // Write to object store
        self.object_store
            .put(&full_path, buffer_data.into())
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to write file: {e}")))?;

        // Create Add action with statistics
        let add_action = self.create_add_action(&relative_path, file_size, &metadata)?;
        self.files_written.push(add_action);

        self.reset_writer()?;

        Ok(())
    }

    /// Generate the next data file path, returning both relative and full paths
    fn next_data_path(&mut self) -> (String, Path) {
        self.part_counter += 1;

        let column_path = ColumnPath::new(Vec::new());
        let compression_suffix = match self.config.writer_properties.compression(&column_path) {
            Compression::UNCOMPRESSED => "",
            Compression::SNAPPY => ".snappy",
            Compression::GZIP(_) => ".gz",
            Compression::LZO => ".lzo",
            Compression::BROTLI(_) => ".br",
            Compression::LZ4 => ".lz4",
            Compression::ZSTD(_) => ".zstd",
            Compression::LZ4_RAW => ".lz4raw",
        };

        let file_name = format!(
            "part-{:05}-{}-c000{}.parquet",
            self.part_counter, self.writer_id, compression_suffix
        );

        let mut full_path = self.config.table_path.clone();
        for segment in &self.config.partition_segments {
            full_path = full_path.child(segment.as_str());
        }
        full_path = full_path.child(file_name.as_str());

        let relative_path = if self.config.partition_segments.is_empty() {
            file_name
        } else {
            format!("{}/{}", self.config.partition_segments.join("/"), file_name)
        };

        (relative_path, full_path)
    }

    /// Create Add action for the written file
    fn create_add_action(
        &self,
        path: &str,
        file_size: i64,
        metadata: &ParquetMetaData,
    ) -> Result<Add, DeltaTableError> {
        create_add(
            &self.config.partition_values,
            path.to_string(),
            file_size,
            metadata,
            self.num_indexed_cols,
            &self.stats_columns,
        )
    }

    /// Reset the writer for the next file
    fn reset_writer(&mut self) -> Result<(), DeltaTableError> {
        let buffer = AsyncShareableBuffer::default();
        let arrow_writer = AsyncArrowWriter::try_new(
            buffer.clone(),
            self.config.file_schema.clone(),
            Some(self.config.writer_properties.clone()),
        )
        .map_err(|e| DeltaTableError::generic(format!("Failed to create new arrow writer: {e}")))?;
        self.buffer = Some(buffer);
        self.arrow_writer = Some(arrow_writer);
        Ok(())
    }

    /// Close the writer and return all Add actions
    pub async fn close(mut self) -> Result<Vec<Add>, DeltaTableError> {
        self.flush_writer().await?;
        Ok(self.files_written)
    }
}

fn record_batch_without_partitions_projected(
    record_batch: &RecordBatch,
    file_schema: ArrowSchemaRef,
    data_indices: &[usize],
) -> Result<RecordBatch, DeltaTableError> {
    let projected = record_batch
        .project(data_indices)
        .map_err(|e| DeltaTableError::generic(format!("Failed to project record batch: {e}")))?;

    RecordBatch::try_new(file_schema, projected.columns().to_vec())
        .map_err(|e| DeltaTableError::generic(format!("Failed to build record batch: {e}")))
}

/// Create Arrow schema without partition columns
fn arrow_schema_without_partitions(
    arrow_schema: &ArrowSchemaRef,
    partition_columns: &[String],
) -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(
        arrow_schema
            .fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::{DeltaWriter, WriterConfig};
    use crate::kernel::DeltaTableError;

    fn make_batch(values: Vec<i32>, parts: Vec<&str>) -> Result<RecordBatch, DeltaTableError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("part", DataType::Utf8, false),
        ]));
        let value_arr: ArrayRef = Arc::new(Int32Array::from(values));
        let part_arr: ArrayRef = Arc::new(StringArray::from(parts));

        RecordBatch::try_new(schema, vec![value_arr, part_arr]).map_err(|e| {
            DeltaTableError::generic(format!("Failed to build test record batch: {e}"))
        })
    }

    fn make_writer() -> Result<DeltaWriter, DeltaTableError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_path = Path::from("delta_table");

        let schema = make_batch(vec![1], vec!["a"])?.schema();
        let config = WriterConfig::new(
            schema,
            vec!["part".to_string()],
            vec!["part".to_string()],
            None,
            1024 * 1024,
            1024,
            32,
            None,
        );
        Ok(DeltaWriter::new(object_store, table_path, config))
    }

    #[tokio::test]
    async fn streaming_writer_splits_by_partition_ranges() -> Result<(), DeltaTableError> {
        let mut writer = make_writer()?;
        let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "a", "b", "b", "c"])?;
        writer.write(&batch).await?;

        let adds = writer.close().await?;
        assert_eq!(adds.len(), 3);

        let paths = adds.iter().map(|a| a.path.as_str()).collect::<Vec<_>>();
        assert!(paths.iter().any(|p| p.contains("part=a/")));
        assert!(paths.iter().any(|p| p.contains("part=b/")));
        assert!(paths.iter().any(|p| p.contains("part=c/")));
        Ok(())
    }

    #[tokio::test]
    async fn streaming_writer_keeps_partition_open_across_batches() -> Result<(), DeltaTableError> {
        let mut writer = make_writer()?;

        let batch1 = make_batch(vec![1, 2], vec!["a", "a"])?;
        writer.write(&batch1).await?;

        // batch2 starts with the same partition, then switches.
        let batch2 = make_batch(vec![3, 4], vec!["a", "b"])?;
        writer.write(&batch2).await?;

        let adds = writer.close().await?;
        assert_eq!(adds.len(), 2);

        let paths = adds.iter().map(|a| a.path.as_str()).collect::<Vec<_>>();
        assert!(paths.iter().any(|p| p.contains("part=a/")));
        assert!(paths.iter().any(|p| p.contains("part=b/")));
        Ok(())
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "input violated partition grouping contract")]
    async fn debug_contract_panics_on_partition_key_regression() {
        #[allow(clippy::unwrap_used)]
        let mut writer = make_writer().unwrap();

        // Key re-appears after switching away: a -> b -> a
        #[allow(clippy::unwrap_used)]
        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "a"]).unwrap();
        let _ = writer.write(&batch).await;
    }
}
