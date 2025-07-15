use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionState, TaskContext};
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::Scalar;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add, StructType};
use deltalake::logstore::LogStoreRef;
use deltalake::protocol::SaveMode;
use deltalake::table::state::DeltaTableState;
use deltalake::DeltaTable;
use futures::future::BoxFuture;
use futures::StreamExt;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::async_utils::AsyncShareableBuffer;
use super::WriteError;
use crate::kernel::models::actions::MetadataExt;

// Default constants
const DEFAULT_TARGET_FILE_SIZE: usize = 104_857_600; // 100MB
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;

/// Trait for creating hive partition paths from partition values
pub trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        if self.is_empty() {
            return String::new();
        }

        self.iter()
            .map(|(k, v)| {
                let value_str = if v.is_null() {
                    "__HIVE_DEFAULT_PARTITION__".to_string()
                } else {
                    v.serialize()
                };
                format!("{k}={value_str}")
            })
            .collect::<Vec<_>>()
            .join("/")
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
        // For now, implement a simple non-partitioned case
        // TODO: Implement proper partitioning logic
        Ok(vec![PartitionResult {
            record_batch: batch.clone(),
            partition_values: IndexMap::new(),
        }])
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
    /// Partition path prefix
    pub prefix: Path,
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
        let part_path = partition_values.hive_partition_path();
        let prefix = Path::parse(part_path).unwrap_or_else(|_| Path::from(""));

        Self {
            table_path,
            file_schema,
            prefix,
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

        // Create Add action
        let add_action = self.create_add_action(relative_path.as_ref(), file_size, &metadata)?;
        self.files_written.push(add_action);

        self.reset_writer()?;

        Ok(())
    }

    /// Generate the next data file path, returning both relative and full paths
    fn next_data_path(&mut self) -> (Path, Path) {
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

        let relative_path = if self.config.prefix.as_ref().is_empty() {
            // For non-partitioned tables, put files directly in table root
            Path::from(file_name)
        } else {
            // For partitioned tables, include the partition path
            self.config.prefix.child(file_name)
        };

        let full_path = self.config.table_path.child(relative_path.to_string());
        (relative_path, full_path)
    }

    /// Create Add action for the written file
    fn create_add_action(
        &self,
        path: &str,
        file_size: i64,
        _metadata: &parquet::format::FileMetaData,
    ) -> Result<Add, DeltaTableError> {
        // Get current timestamp
        let modification_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before Unix epoch")
            .as_millis() as i64;

        // Convert partition values to the format expected by Add
        let partition_values = self
            .config
            .partition_values
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    if v.is_null() {
                        None
                    } else {
                        Some(v.serialize())
                    },
                )
            })
            .collect();

        Ok(Add {
            path: path.to_string(),
            size: file_size,
            partition_values,
            modification_time,
            data_change: true,
            stats: None, // TODO: Implement statistics collection
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        })
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

/// Write data into a Delta table using DataFusion DataFrame
pub struct WriteBuilder {
    /// Delta log store for handling metadata and transaction log
    log_store: LogStoreRef,
    /// A snapshot of the table's state (None for new tables)
    snapshot: Option<DeltaTableState>,
    /// Input DataFrame to write
    input_dataframe: Option<DataFrame>,
    /// DataFusion session state for plan execution
    session_state: Option<SessionState>,
    /// Save mode defines how to treat existing data
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// Size above which we will write a buffered parquet file to disk
    target_file_size: Option<usize>,
    /// Number of records to be written in single batch to underlying writer
    write_batch_size: Option<usize>,
    /// Parquet writer properties
    writer_properties: Option<WriterProperties>,
    /// Whether to use safe casting (return NULL on cast failure vs error)
    safe_cast: bool,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Table name (used when creating new tables)
    name: Option<String>,
    /// Table description (used when creating new tables)
    description: Option<String>,
    /// Table configuration (used when creating new tables)
    configuration: HashMap<String, Option<String>>,
}

impl WriteBuilder {
    /// Create a new WriteBuilder
    pub fn new(log_store: LogStoreRef, snapshot: Option<DeltaTableState>) -> Self {
        Self {
            log_store,
            snapshot,
            input_dataframe: None,
            session_state: None,
            mode: SaveMode::Append,
            partition_columns: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            safe_cast: true,
            commit_properties: CommitProperties::default(),
            name: None,
            description: None,
            configuration: HashMap::new(),
        }
    }

    /// Set the input DataFrame to write
    pub fn with_input_dataframe(mut self, dataframe: DataFrame) -> Self {
        self.input_dataframe = Some(dataframe);
        self
    }

    /// Set the session state for plan execution
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.session_state = Some(state);
        self
    }

    /// Set the save mode
    pub fn with_save_mode(mut self, mode: SaveMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set partition columns
    pub fn with_partition_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = Some(columns.into_iter().map(|c| c.into()).collect());
        self
    }

    /// Set target file size for parquet files
    pub fn with_target_file_size(mut self, size: usize) -> Self {
        self.target_file_size = Some(size);
        self
    }

    /// Set write batch size
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = Some(size);
        self
    }

    /// Set parquet writer properties
    pub fn with_writer_properties(mut self, props: WriterProperties) -> Self {
        self.writer_properties = Some(props);
        self
    }

    /// Set whether to use safe casting
    pub fn with_safe_cast(mut self, safe: bool) -> Self {
        self.safe_cast = safe;
        self
    }

    /// Set commit properties
    pub fn with_commit_properties(mut self, props: CommitProperties) -> Self {
        self.commit_properties = props;
        self
    }

    /// Set table name (for new table creation)
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set table description (for new table creation)
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set table configuration (for new table creation)
    pub fn with_configuration(
        mut self,
        configuration: impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>,
    ) -> Self {
        self.configuration = configuration
            .into_iter()
            .map(|(k, v)| (k.into(), v.map(|s| s.into())))
            .collect();
        self
    }

    /// Validate preconditions before execution
    fn validate(&self) -> Result<(), WriteError> {
        if self.input_dataframe.is_none() {
            return Err(WriteError::MissingDataFrame);
        }
        if self.session_state.is_none() {
            return Err(WriteError::MissingSessionState);
        }
        Ok(())
    }

    /// Get partition columns, validating against table schema if available
    fn get_partition_columns(&self) -> Result<Vec<String>, WriteError> {
        match (&self.partition_columns, &self.snapshot) {
            (Some(columns), Some(snapshot)) => {
                // Validate partition columns against existing table
                let table_partition_cols = snapshot.metadata().partition_columns().clone();
                if table_partition_cols != *columns {
                    return Err(WriteError::PartitionColumnMismatch {
                        expected: table_partition_cols,
                        got: columns.clone(),
                    });
                }
                Ok(columns.clone())
            }
            (Some(columns), None) => Ok(columns.clone()),
            (None, Some(snapshot)) => Ok(snapshot.metadata().partition_columns().clone()),
            (None, None) => Ok(vec![]),
        }
    }

    /// Execute the write operation
    async fn execute_write(self) -> Result<DeltaTable, WriteError> {
        // Validate preconditions
        self.validate()?;

        // Get partition columns first, before moving values out of self
        let partition_columns = self.get_partition_columns()?;

        // Extract needed values after getting partition columns
        let dataframe = self.input_dataframe.expect("Input dataframe is required");
        let session_state = self.session_state.expect("Session state is required");
        let log_store = self.log_store.clone();
        let snapshot = self.snapshot.clone();
        let mode = self.mode;
        let commit_properties = self.commit_properties.clone();

        // Convert DataFrame to ExecutionPlan
        let logical_plan = dataframe.logical_plan().clone();
        let execution_plan = session_state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| WriteError::PhysicalPlan { source: e })?;

        // dbg!("Physical plan inside WriteBuilder: {:?}", &execution_plan);

        // Get table path before moving log_store into tasks
        let table_path = Path::from(log_store.root_uri());

        // Execute the plan and write data
        let mut tasks = Vec::new();
        let partition_count = execution_plan
            .properties()
            .output_partitioning()
            .partition_count();

        for partition_id in 0..partition_count {
            let plan = execution_plan.clone();
            let task_ctx = Arc::new(TaskContext::from(&session_state));
            let object_store = log_store.object_store(None);
            let target_file_size = self.target_file_size;
            let write_batch_size = self.write_batch_size;
            let writer_properties = self.writer_properties.clone();
            let partition_columns = partition_columns.clone();
            let table_path = table_path.clone();

            // Convert schema properly
            let df_schema = dataframe.schema();
            let arrow_schema: ArrowSchemaRef = Arc::new(df_schema.as_arrow().clone());

            let task: JoinHandle<Result<Vec<Add>, WriteError>> = tokio::spawn(async move {
                let config = WriterConfig::new(
                    arrow_schema,
                    partition_columns,
                    writer_properties,
                    target_file_size,
                    write_batch_size,
                    32,   // Default num_indexed_cols
                    None, // Default stats_columns
                );

                let mut writer = DeltaWriter::new(object_store, table_path, config);
                let mut stream = plan
                    .execute(partition_id, task_ctx)
                    .map_err(|e| WriteError::PhysicalPlan { source: e })?;

                // dbg!("Starting to process stream for partition {}", partition_id);

                // let mut batch_count = 0;

                while let Some(batch_result) = stream.next().await {
                    // batch_count += 1;
                    // dbg!(batch_count);
                    let batch = batch_result.map_err(|e| WriteError::PhysicalPlan { source: e })?;
                    writer
                        .write(&batch)
                        .await
                        .map_err(|e| WriteError::DeltaWriter { source: e })?;
                }

                // dbg!(
                //     "Finished processing stream for partition {}. Total batches: {}",
                //     partition_id,
                //     batch_count
                // );

                let add_actions = writer
                    .close()
                    .await
                    .map_err(|e| WriteError::DeltaWriter { source: e })?;
                Ok(add_actions)
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let mut all_actions = Vec::new();
        for task in tasks {
            let add_actions = task
                .await
                .map_err(|e| WriteError::WriteTask { source: e })??;

            for add_action in add_actions {
                all_actions.push(Action::Add(add_action));
            }
        }

        // Handle different save modes
        match mode {
            SaveMode::Overwrite => {
                // For overwrite mode, we need to add Remove actions for existing files
                if let Some(ref snapshot) = snapshot {
                    let existing_files = snapshot
                        .file_actions()
                        .map_err(|e| WriteError::DeltaWriter { source: e })?;
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("System time before Unix epoch")
                        .as_millis() as i64;

                    for file in existing_files {
                        all_actions.push(Action::Remove(deltalake::kernel::Remove {
                            path: file.path.clone(),
                            deletion_timestamp: Some(current_time),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(file.partition_values.clone()),
                            size: Some(file.size),
                            deletion_vector: file.deletion_vector.clone(),
                            tags: None,
                            base_row_id: file.base_row_id,
                            default_row_commit_version: file.default_row_commit_version,
                        }));
                    }
                }
            }
            SaveMode::ErrorIfExists => {
                // Check if table exists and has data
                if let Some(ref snapshot) = snapshot {
                    let existing_files = snapshot
                        .file_actions()
                        .map_err(|e| WriteError::DeltaWriter { source: e })?;
                    if !existing_files.is_empty() {
                        return Err(WriteError::AlreadyExists(
                            "Table already exists and contains data".to_string(),
                        ));
                    }
                }
            }
            SaveMode::Append => {}
            SaveMode::Ignore => {
                // If table exists, do nothing
                if snapshot.is_some() {
                    return Ok(DeltaTable::new(log_store, Default::default()));
                }
            }
        }

        // Determine the operation type
        let operation = if snapshot.is_some() {
            // Existing table - this is a write operation
            deltalake::protocol::DeltaOperation::Write {
                mode,
                partition_by: if partition_columns.is_empty() {
                    None
                } else {
                    Some(partition_columns.clone())
                },
                predicate: None,
            }
        } else {
            // New table - this is a create operation
            let df_schema = dataframe.schema();
            let arrow_schema = df_schema.as_arrow();
            let delta_schema: StructType =
                arrow_schema
                    .try_into_kernel()
                    .map_err(|e| WriteError::SchemaValidation {
                        message: format!("Failed to convert schema: {e}"),
                    })?;

            let configuration_map: HashMap<String, String> = self
                .configuration
                .clone()
                .into_iter()
                .filter_map(|(k, v)| v.map(|val| (k, val)))
                .collect();

            let mut metadata = crate::kernel::models::actions::new_metadata(
                &delta_schema,
                partition_columns.clone(),
                configuration_map,
            )
            .map_err(|e| WriteError::SchemaValidation {
                message: format!("Failed to create metadata: {e}"),
            })?;

            // Update metadata with optional fields
            if let Some(name) = &self.name {
                metadata =
                    metadata
                        .with_name(name.clone())
                        .map_err(|e| WriteError::SchemaValidation {
                            message: format!("Failed to set table name: {e}"),
                        })?;
            }

            if let Some(description) = &self.description {
                metadata = metadata
                    .with_description(description.clone())
                    .map_err(|e| WriteError::SchemaValidation {
                        message: format!("Failed to set table description: {e}"),
                    })?;
            }

            deltalake::protocol::DeltaOperation::Create {
                mode,
                location: log_store.root_uri().to_string(),
                protocol: deltalake::kernel::Protocol::default(),
                metadata,
            }
        };

        // Create the table if it doesn't exist
        let mut table = if let Some(snapshot) = snapshot {
            let mut table = DeltaTable::new(log_store.clone(), Default::default());
            table.state = Some(snapshot);
            table
        } else {
            // For new tables, we need to add Protocol and Metadata actions
            let protocol = deltalake::kernel::Protocol::default();
            let df_schema = dataframe.schema();
            let arrow_schema = df_schema.as_arrow();
            let delta_schema: StructType =
                arrow_schema
                    .try_into_kernel()
                    .map_err(|e| WriteError::SchemaValidation {
                        message: format!("Failed to convert schema: {e}"),
                    })?;

            let configuration_map: HashMap<String, String> = self
                .configuration
                .clone()
                .into_iter()
                .filter_map(|(k, v)| v.map(|val| (k, val)))
                .collect();

            let mut metadata = crate::kernel::models::actions::new_metadata(
                &delta_schema,
                partition_columns.clone(),
                configuration_map,
            )
            .map_err(|e| WriteError::SchemaValidation {
                message: format!("Failed to create metadata: {e}"),
            })?;

            // Update metadata with optional fields
            if let Some(name) = &self.name {
                metadata =
                    metadata
                        .with_name(name.clone())
                        .map_err(|e| WriteError::SchemaValidation {
                            message: format!("Failed to set table name: {e}"),
                        })?;
            }

            if let Some(description) = &self.description {
                metadata = metadata
                    .with_description(description.clone())
                    .map_err(|e| WriteError::SchemaValidation {
                        message: format!("Failed to set table description: {e}"),
                    })?;
            }

            // Add Protocol and Metadata actions for new tables
            all_actions.insert(0, Action::Protocol(protocol));
            all_actions.insert(1, Action::Metadata(metadata));

            DeltaTable::new(log_store.clone(), Default::default())
        };

        // dbg!(&all_actions);

        // Commit the transaction
        if !all_actions.is_empty() {
            let commit_result = CommitBuilder::from(commit_properties)
                .with_actions(all_actions)
                .build(
                    table
                        .snapshot()
                        .ok()
                        .map(|s| s as &dyn deltalake::kernel::transaction::TableReference),
                    log_store,
                    operation,
                )
                .await
                .map_err(|e| WriteError::CommitFailed {
                    message: format!("Failed to commit transaction: {e}"),
                })?;

            // Update the table with the new state after commit
            table.state = Some(commit_result.snapshot().clone());
        }

        Ok(table)
    }
}

impl std::future::IntoFuture for WriteBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.execute_write().await.map_err(|e| e.into()) })
    }
}
