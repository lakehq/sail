use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::Add;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use uuid::Uuid;

use super::async_utils::AsyncShareableBuffer;
use super::stats::create_add;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/writer.rs>
const DEFAULT_TARGET_FILE_SIZE: usize = 104_857_600; // 100MB
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;

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
                let value_str = if v.is_null() {
                    "__HIVE_DEFAULT_PARTITION__".to_string()
                } else {
                    v.serialize_encoded()
                };
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
    pub target_file_size: usize,
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
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
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
            target_file_size: target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            write_batch_size: write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE),
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
        let partition_columns = &self.config.partition_columns;
        if partition_columns.is_empty() {
            return Ok(vec![PartitionResult {
                record_batch: batch.clone(),
                partition_values: IndexMap::new(),
            }]);
        }

        let partition_col_indices = partition_columns
            .iter()
            .map(|name| {
                batch
                    .schema()
                    .index_of(name)
                    .map_err(|e| DeltaTableError::Arrow { source: e })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let partition_arrays = partition_col_indices
            .iter()
            .map(|i| batch.column(*i).clone())
            .collect::<Vec<_>>();

        // Build a map from partition key to row indices
        let mut partitions: HashMap<String, Vec<u32>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let key: String = partition_arrays
                .iter()
                .map(|arr| {
                    Scalar::from_array(arr.as_ref(), row_idx)
                        .map(|s| s.serialize())
                        .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string())
                })
                .collect::<Vec<_>>()
                .join("\0");
            partitions.entry(key).or_default().push(row_idx as u32);
        }

        let mut results = Vec::with_capacity(partitions.len());

        for (_, indices) in partitions {
            let indices_arr = UInt32Array::from(indices);

            let schema = batch.schema();
            let mut new_columns = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                let new_col = compute::take(column.as_ref(), &indices_arr, None)
                    .map_err(|e| DeltaTableError::Arrow { source: e })?;
                new_columns.push(new_col);
            }
            let partitioned_batch = RecordBatch::try_new(schema.clone(), new_columns)
                .map_err(|e| DeltaTableError::Arrow { source: e })?;

            // Get partition values from the first row of the group
            let first_row_idx = indices_arr.value(0) as usize;
            let mut partition_values = IndexMap::new();
            for (i, col_name) in partition_columns.iter().enumerate() {
                let arr = &partition_arrays[i];
                // FIXME: Assuming partition columns are not null.
                let scalar = Scalar::from_array(arr.as_ref(), first_row_idx)
                    .expect("Partition column value should not be null");
                partition_values.insert(col_name.clone(), scalar);
            }

            results.push(PartitionResult {
                record_batch: partitioned_batch,
                partition_values,
            });
        }

        Ok(results)
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
    pub target_file_size: usize,
    /// Row chunks passed to parquet writer
    pub write_batch_size: usize,
}

impl PartitionWriterConfig {
    pub fn new(
        table_path: Path,
        file_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
        writer_properties: WriterProperties,
        target_file_size: usize,
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
                writer
                    .write(&slice)
                    .await
                    .map_err(|e| DeltaTableError::generic(format!("Failed to write batch: {e}")))?;
            }

            // Check if we need to flush
            let buffer_size = if let Some(buffer) = self.buffer.as_ref() {
                buffer.len().await
            } else {
                0
            };
            if buffer_size >= self.config.target_file_size {
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
        if metadata.num_rows == 0 {
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
        metadata: &parquet::format::FileMetaData,
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
