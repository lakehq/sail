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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
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
    /// Column names for columns the table is partitioned by
    pub partition_columns: Vec<String>,
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
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
        }
    }

    /// Schema of files written to disk (without partition columns)
    pub fn file_schema(&self) -> ArrowSchemaRef {
        arrow_schema_without_partitions(&self.table_schema, &self.partition_columns)
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
    /// Partition writers for individual partitions
    partition_writers: HashMap<String, PartitionWriter>,
}

impl DeltaWriter {
    pub fn new(object_store: Arc<dyn ObjectStore>, table_path: Path, config: WriterConfig) -> Self {
        Self {
            object_store,
            table_path,
            config,
            partition_writers: HashMap::new(),
        }
    }

    /// Write a record batch to the appropriate partition
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        // Divide the batch by partition values
        let partition_results = self.divide_by_partition_values(batch)?;

        for result in partition_results {
            self.write_partition(result.record_batch, &result.partition_values)
                .await?;
        }

        Ok(())
    }

    /// Write a batch to a specific partition
    async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
    ) -> Result<(), DeltaTableError> {
        let partition_key = partition_values.hive_partition_path();

        let record_batch =
            record_batch_without_partitions(&record_batch, &self.config.partition_columns)?;

        match self.partition_writers.get_mut(&partition_key) {
            Some(writer) => {
                writer.write(&record_batch).await?;
            }
            None => {
                let config = PartitionWriterConfig::new(
                    self.table_path.clone(),
                    self.config.file_schema(),
                    partition_values.clone(),
                    self.config.writer_properties.clone(),
                    self.config.target_file_size,
                    self.config.write_batch_size,
                );

                let mut writer = PartitionWriter::try_with_config(
                    self.object_store.clone(),
                    config,
                    self.config.num_indexed_cols,
                    self.config.stats_columns.clone(),
                )?;

                writer.write(&record_batch).await?;
                self.partition_writers.insert(partition_key, writer);
            }
        }

        Ok(())
    }

    /// Divide record batch by partition values
    fn divide_by_partition_values(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<PartitionResult>, DeltaTableError> {
        divide_by_partition_values(
            arrow_schema_without_partitions(
                &self.config.table_schema,
                &self.config.partition_columns,
            ),
            self.config.partition_columns.clone(),
            batch,
        )
    }

    /// Close the writer and get the Add actions
    pub async fn close(self) -> Result<Vec<Add>, DeltaTableError> {
        let mut all_actions = Vec::new();

        for (_, writer) in self.partition_writers.into_iter() {
            let actions = writer.close().await?;
            all_actions.extend(actions);
        }

        Ok(all_actions)
    }
}

/// Result of partitioning a record batch
#[derive(Debug)]
pub struct PartitionResult {
    pub record_batch: RecordBatch,
    pub partition_values: IndexMap<String, Scalar>,
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

/// Remove partition columns from a record batch
fn record_batch_without_partitions(
    record_batch: &RecordBatch,
    partition_columns: &[String],
) -> Result<RecordBatch, DeltaTableError> {
    let mut non_partition_columns = Vec::new();

    for (i, field) in record_batch.schema().fields().iter().enumerate() {
        if !partition_columns.contains(field.name()) {
            non_partition_columns.push(i);
        }
    }

    record_batch
        .project(&non_partition_columns)
        .map_err(|e| DeltaTableError::generic(format!("Failed to project record batch: {e}")))
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/writer/record_batch.rs>
/// Partition a RecordBatch along partition columns
pub(crate) fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    partition_columns: Vec<String>,
    values: &RecordBatch,
) -> Result<Vec<PartitionResult>, DeltaTableError> {
    let mut partitions = Vec::new();

    if partition_columns.is_empty() {
        partitions.push(PartitionResult {
            partition_values: IndexMap::new(),
            record_batch: values.clone(),
        });
        return Ok(partitions);
    }

    let schema = values.schema();

    // Since DeltaProjectExec moves partition columns to the end, we can rely on their positions.
    let num_cols = schema.fields().len();
    let num_part_cols = partition_columns.len();
    let projection: Vec<usize> = (num_cols - num_part_cols..num_cols).collect();

    let sort_columns = values
        .project(&projection)
        .map_err(|e| DeltaTableError::generic(e.to_string()))?;

    let indices = lexsort_to_indices(sort_columns.columns());
    let sorted_partition_columns = (num_cols - num_part_cols..num_cols)
        .map(|idx| {
            let col = values.column(idx);
            compute::take(col, &indices, None).map_err(|e| DeltaTableError::generic(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let partition_ranges =
        datafusion::arrow::compute::partition(sorted_partition_columns.as_slice())
            .map_err(|e| DeltaTableError::generic(e.to_string()))?;

    for range in partition_ranges.ranges().iter() {
        // get row indices for current partition
        let idx: UInt32Array = (range.start..range.end)
            .map(|i| Some(indices.value(i)))
            .collect();

        let partition_key_iter = sorted_partition_columns
            .iter()
            .map(|col| {
                Scalar::from_array(&col.slice(range.start, range.end - range.start), 0)
                    .ok_or_else(|| DeltaTableError::generic("failed to parse partition value"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let partition_values = partition_columns
            .clone()
            .into_iter()
            .zip(partition_key_iter)
            .collect();
        let batch_data = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let col_idx = schema.index_of(f.name()).map_err(|_| {
                    DeltaTableError::schema(format!("Column {} not found in batch", f.name()))
                })?;
                let col = values.column(col_idx);
                compute::take(col.as_ref(), &idx, None)
                    .map_err(|e| DeltaTableError::generic(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        partitions.push(PartitionResult {
            partition_values,
            record_batch: RecordBatch::try_new(arrow_schema.clone(), batch_data)
                .map_err(|e| DeltaTableError::generic(e.to_string()))?,
        });
    }

    Ok(partitions)
}

fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    #[allow(clippy::unwrap_used)]
    let converter = RowConverter::new(fields).unwrap();
    #[allow(clippy::unwrap_used)]
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}
