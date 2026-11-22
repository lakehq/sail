// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, exec_err};
use futures::{Stream, StreamExt};
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::formats::parquet::writer::file::ParquetFileWriter;
use crate::formats::parquet::writer::partition::{PartitionRouter, PartitionedWriter};
use crate::formats::parquet::writer::resources::{ActiveWriter, EncodeResources, EncodeSettings};
use crate::formats::parquet::writer::sorting::lex_ordering_to_sorting_columns;
use crate::listing::commit::{WrittenFile, delete_files, manifest_batch};
use crate::listing::part_file::part_file_name;

/// The largest number of rows that are written to a column
/// before the limits of its data page are checked.
const MAX_WRITE_BATCH_SIZE: usize = 1024;

/// The limit on the rows of a data page that the Arrow writer is given when
/// the rows of a page are not limited. The header of a data page counts
/// the values of the page with a 32-bit signed integer, so a page cannot
/// have more rows than this.
const UNLIMITED_PAGE_ROWS: usize = i32::MAX as usize;

/// The description of the work of one Parquet writer task.
pub struct ParquetWriteTask {
    pub store: Arc<dyn ObjectStore>,
    /// The root directory of the table.
    pub table_path: Path,
    pub write_id: Uuid,
    /// The index of the writer partition of the task.
    pub partition: usize,
    /// The number of writer partitions.
    pub partitions: usize,
    pub attempt: usize,
    /// The file name suffix such as `.zstd.parquet`.
    pub file_suffix: String,
    /// The columns whose values determine the directory of each row.
    /// These columns are not written to the files.
    pub partition_columns: Vec<String>,
    pub options: TableParquetOptions,
    /// The ordering of the input to record in the file metadata.
    pub sorting: Option<LexOrdering>,
    /// The capacity of the upload buffer of each file.
    pub buffer_size: usize,
    /// The number of rows after which the task continues with a new file.
    /// The limit is checked when a row group is completed,
    /// so a file can have more rows than this.
    pub max_rows_per_file: usize,
    /// The number of files that the task keeps open at the same time
    /// when the data has partition columns.
    pub max_open_files: usize,
    /// How the files of the task are encoded.
    pub encode: EncodeSettings,
}

/// The metrics of one Parquet writer task.
pub struct ParquetWriteMetrics {
    rows_written: Count,
    bytes_written: Count,
    files_written: Count,
    elapsed_compute: Time,
}

impl ParquetWriteMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows_written: MetricBuilder::new(metrics).counter("rows_written", partition),
            bytes_written: MetricBuilder::new(metrics).counter("bytes_written", partition),
            files_written: MetricBuilder::new(metrics).counter("files_written", partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
        }
    }

    fn record(&self, file: &WrittenFile) {
        self.rows_written.add(file.num_rows as usize);
        self.bytes_written.add(file.size as usize);
        self.files_written.add(1);
    }
}

/// The files that one attempt of a writer task has completed.
///
/// The attempt removes these files itself when it fails or is canceled,
/// so that the table has no files of an attempt that did not report them.
/// The file names are unique to the attempt, so this never affects the files
/// of another attempt.
struct AttemptFiles {
    store: Arc<dyn ObjectStore>,
    state: Mutex<AttemptFilesState>,
}

#[derive(Default)]
struct AttemptFilesState {
    completed: Vec<Path>,
    /// Whether the attempt has ended without delivering its manifest.
    abandoned: bool,
}

impl AttemptFiles {
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            state: Mutex::new(AttemptFilesState::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, AttemptFilesState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Completes a file.
    ///
    /// The file is completed in a separate task, since the requests that make
    /// the file visible are not canceled when the caller is. The task keeps
    /// running when the caller is canceled, and it removes the file if the
    /// attempt has been abandoned by the time the file is visible.
    async fn close(self: &Arc<Self>, writer: ParquetFileWriter) -> Result<WrittenFile> {
        self.start_close(writer)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    /// Starts to complete a file as [`Self::close`] does, without waiting for it.
    fn start_close(self: &Arc<Self>, writer: ParquetFileWriter) -> JoinHandle<Result<WrittenFile>> {
        let files = Arc::clone(self);
        tokio::spawn(async move {
            let file = writer.close().await?;
            let abandoned = {
                let mut state = files.state();
                if !state.abandoned {
                    state.completed.push(file.location.clone());
                }
                state.abandoned
            };
            if abandoned {
                files.delete(vec![file.location.clone()]).await;
            }
            Ok(file)
        })
    }

    async fn delete(&self, files: Vec<Path>) {
        if let Err(e) = delete_files(&self.store, files).await {
            log::warn!("failed to delete the files of an abandoned write attempt: {e}");
        }
    }
}

/// Abandons the attempt of a writer task when dropped, unless disarmed.
struct AbandonGuard {
    files: Arc<AttemptFiles>,
    armed: bool,
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let completed = {
            let mut state = self.files.state();
            state.abandoned = true;
            std::mem::take(&mut state.completed)
        };
        if completed.is_empty() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let files = Arc::clone(&self.files);
            runtime.spawn(async move { files.delete(completed).await });
        }
    }
}

/// Creates the files of a writer task. All files have the same schema.
pub(super) struct FileFactory {
    files: Arc<AttemptFiles>,
    store: Arc<dyn ObjectStore>,
    write_id: Uuid,
    partition: usize,
    attempt: usize,
    file_suffix: String,
    schema: SchemaRef,
    props: WriterProperties,
    skip_arrow_metadata: bool,
    buffer_size: usize,
    max_rows_per_file: u64,
    resources: Arc<EncodeResources>,
}

impl FileFactory {
    fn try_new(
        task: ParquetWriteTask,
        files: Arc<AttemptFiles>,
        input_schema: &SchemaRef,
        schema: SchemaRef,
        resources: Arc<EncodeResources>,
    ) -> Result<Self> {
        let mut options = task.options;
        let skip_arrow_metadata = options.global.skip_arrow_metadata;
        if !skip_arrow_metadata {
            options.arrow_schema(&schema);
        }
        // A limit of zero means that the rows of a data page are not limited,
        // and the Arrow writer does not accept zero for either of the two values.
        // The limits of a data page are checked each time a number of rows has been
        // written to a column. That number is at most the limit on the rows
        // of a page, so that a page does not get more rows than a small limit allows.
        let page_rows = match options.global.data_page_row_count_limit {
            0 => UNLIMITED_PAGE_ROWS,
            rows => rows,
        };
        options.global.data_page_row_count_limit = page_rows;
        options.global.write_batch_size = page_rows.min(MAX_WRITE_BATCH_SIZE);
        let mut builder = WriterPropertiesBuilder::try_from(&options)?;
        if let Some(sorting) = task
            .sorting
            .as_ref()
            .and_then(|ordering| sorting_columns(ordering, input_schema, &schema))
        {
            builder = builder.set_sorting_columns(Some(sorting));
        }
        Ok(Self {
            files,
            store: task.store,
            write_id: task.write_id,
            partition: task.partition,
            attempt: task.attempt,
            file_suffix: task.file_suffix,
            schema,
            props: builder.build(),
            skip_arrow_metadata,
            buffer_size: task.buffer_size,
            max_rows_per_file: task.max_rows_per_file as u64,
            resources,
        })
    }

    fn create(&self, directory: &Path, counter: u32) -> Result<ParquetFileWriter> {
        let name = part_file_name(
            self.partition,
            &self.write_id,
            counter,
            self.attempt,
            &self.file_suffix,
        );
        ParquetFileWriter::try_new(
            Arc::clone(&self.store),
            directory.clone().join(name),
            Arc::clone(&self.schema),
            self.props.clone(),
            self.skip_arrow_metadata,
            self.buffer_size,
            Arc::clone(&self.resources),
        )
    }

    /// The time that the task has spent on encoding.
    pub fn elapsed_compute(&self) -> &Time {
        self.resources.elapsed_compute()
    }
}

/// An ordering that cannot be expressed as Parquet sorting columns
/// (e.g. an ordering on an expression) is not recorded in the file.
fn sorting_columns(
    ordering: &LexOrdering,
    input_schema: &SchemaRef,
    schema: &SchemaRef,
) -> Option<Vec<SortingColumn>> {
    lex_ordering_to_sorting_columns(ordering, input_schema, schema)
        .ok()
        .filter(|columns| !columns.is_empty())
}

/// Writes the files of a writer task in one directory.
/// At most one file is open at a time.
pub(super) struct DirectoryWriter {
    directory: Path,
    writer: Option<Box<ParquetFileWriter>>,
    /// The file before the open file, while it is being completed.
    closing: Option<ClosingFile>,
    next_counter: u32,
}

/// A file that has all its rows and is being completed.
struct ClosingFile {
    task: JoinHandle<Result<WrittenFile>>,
    /// The memory that the file used when it received its last row.
    memory_size: usize,
}

impl DirectoryWriter {
    pub fn new(directory: Path) -> Self {
        Self {
            directory,
            writer: None,
            closing: None,
            next_counter: 0,
        }
    }

    fn open(&mut self, factory: &FileFactory) -> Result<&mut ParquetFileWriter> {
        match &mut self.writer {
            Some(writer) => Ok(writer),
            writer @ None => {
                let file = factory.create(&self.directory, self.next_counter)?;
                self.next_counter += 1;
                Ok(writer.insert(Box::new(file)))
            }
        }
    }

    pub async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        let writer = self.open(factory)?;
        let flushed = writer.write(batch).await?;
        if flushed && writer.rows() >= factory.max_rows_per_file {
            // The file is completed while the next file receives rows, so that
            // the task does not stop reading its input until the row groups
            // of the file are encoded. Only one file is completed at a time.
            self.wait_for_closing(written).await?;
            if let Some(writer) = self.writer.take() {
                self.closing = Some(ClosingFile {
                    memory_size: writer.memory_size(),
                    task: factory.files.start_close(*writer),
                });
            }
        }
        Ok(())
    }

    /// Whether the directory has received a file.
    fn has_files(&self) -> bool {
        self.next_counter > 0
    }

    pub fn memory_size(&self) -> usize {
        self.writer.as_ref().map_or(0, |x| x.memory_size())
            + self.closing.as_ref().map_or(0, |x| x.memory_size)
    }

    /// Waits for the file that is being completed.
    async fn wait_for_closing(&mut self, written: &mut Vec<WrittenFile>) -> Result<()> {
        if let Some(closing) = self.closing.take() {
            let file = closing
                .task
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))??;
            written.push(file);
        }
        Ok(())
    }

    pub async fn close(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        self.wait_for_closing(written).await?;
        if let Some(writer) = self.writer.take() {
            written.push(factory.files.close(*writer).await?);
        }
        Ok(())
    }
}

/// Returns whether the data type is or contains a union type,
/// which is the type that has no Parquet schema in the Arrow writer.
fn contains_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_union(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_union(field.data_type())),
        DataType::Dictionary(_, value) => contains_union(value),
        _ => false,
    }
}

/// The files that a writer task is writing.
enum TaskOutput {
    Directory(DirectoryWriter),
    Partitioned(Box<PartitionedWriter>),
}

impl TaskOutput {
    async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        match self {
            Self::Directory(x) => x.write(factory, batch, written).await,
            Self::Partitioned(x) => x.write(factory, batch, written).await,
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            Self::Directory(x) => x.memory_size(),
            Self::Partitioned(x) => x.memory_size(),
        }
    }

    async fn close(&mut self, factory: &FileFactory, written: &mut Vec<WrittenFile>) -> Result<()> {
        match self {
            Self::Directory(x) => x.close(factory, written).await,
            Self::Partitioned(x) => x.close(factory, written).await,
        }
    }
}

/// Writes one input partition.
///
/// The stream has a single item, which is the manifest of the task.
/// The files of the task are kept only if the stream is read to its end
/// after the manifest. If the task fails, or if the stream is dropped before
/// that, the files that the task has completed are deleted.
pub fn write_partition(
    task: ParquetWriteTask,
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    metrics: ParquetWriteMetrics,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    enum State {
        Write(
            Box<(
                ParquetWriteTask,
                SendableRecordBatchStream,
                MemoryReservation,
                ParquetWriteMetrics,
            )>,
        ),
        Written(AbandonGuard),
        Failed,
    }

    let state = State::Write(Box::new((task, input, reservation, metrics)));
    futures::stream::unfold(state, |state| async move {
        match state {
            State::Write(args) => {
                let (task, input, reservation, metrics) = *args;
                let files = Arc::new(AttemptFiles::new(Arc::clone(&task.store)));
                let guard = AbandonGuard {
                    files: Arc::clone(&files),
                    armed: true,
                };
                match write_files(task, files, input, reservation, metrics).await {
                    Ok(manifest) => Some((Ok(manifest), State::Written(guard))),
                    Err(e) => Some((Err(e), State::Failed)),
                }
            }
            State::Written(mut guard) => {
                // The reader has received the manifest and asked for more.
                guard.armed = false;
                None
            }
            State::Failed => None,
        }
    })
}

/// Writes the files of one input partition and returns the manifest of the task.
async fn write_files(
    task: ParquetWriteTask,
    files: Arc<AttemptFiles>,
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    metrics: ParquetWriteMetrics,
) -> Result<RecordBatch> {
    // The task shares the threads with the other writer tasks from here on,
    // until it has written its files, has failed, or is canceled.
    let _active_writer = ActiveWriter::new(&task.encode.active_writers);
    let partition = task.partition;
    let partitions = task.partitions;
    let attempt = task.attempt;
    let input_schema = input.schema();
    let (mut output, schema) = if task.partition_columns.is_empty() {
        let output = DirectoryWriter::new(task.table_path.clone());
        (TaskOutput::Directory(output), Arc::clone(&input_schema))
    } else {
        let router = PartitionRouter::try_new(&input_schema, &task.partition_columns)?;
        let schema = Arc::clone(router.file_schema());
        let output = PartitionedWriter::new(router, task.table_path.clone(), task.max_open_files);
        (TaskOutput::Partitioned(Box::new(output)), schema)
    };
    // The input that waits to be encoded is counted separately from the memory
    // of the encoders, and without a limit, since its size is limited by the
    // read-ahead budget of the task.
    let resources = Arc::new(EncodeResources::new(
        task.encode.clone(),
        reservation.new_empty(),
        metrics.elapsed_compute.clone(),
    ));
    // A Parquet file takes its number of rows from its columns,
    // so rows cannot be written to files without columns.
    let rows_not_supported = if !schema.fields().is_empty() {
        None
    } else if task.partition_columns.is_empty() {
        Some("Parquet write does not support data without columns")
    } else {
        Some("Parquet write does not support data whose columns are all partition columns")
    };
    let factory = FileFactory::try_new(task, files, &input_schema, schema, resources)?;
    let mut written = vec![];
    while let Some(batch) = input.next().await.transpose()? {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(message) = rows_not_supported {
            return exec_err!("{message}");
        }
        output.write(&factory, &batch, &mut written).await?;
        reservation.try_resize(output.memory_size())?;
    }
    if let TaskOutput::Directory(output) = &mut output
        && partition == 0
        && !output.has_files()
        && rows_not_supported.is_none()
        && !input_schema
            .fields()
            .iter()
            .any(|field| contains_union(field.data_type()))
    {
        // Without partition columns, the first partition always produces a file,
        // so that the schema of the data can be read from the table even when
        // the result is empty. This is skipped for a schema that the Parquet
        // writer cannot represent and for a schema without columns, so that
        // an empty result can still be written.
        output.open(&factory)?;
    }
    output.close(&factory, &mut written).await?;
    reservation.free();
    for file in &written {
        metrics.record(file);
    }
    manifest_batch(partition, partitions, attempt, &written)
}
