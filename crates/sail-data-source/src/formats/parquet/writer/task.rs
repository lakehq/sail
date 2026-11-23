// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result, exec_err};
use futures::{Stream, StreamExt};
use object_store::path::Path;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::formats::parquet::writer::file::{MAX_UPLOAD_CONCURRENCY, ParquetFileWriter};
use crate::formats::parquet::writer::partition::{PartitionRouter, PartitionedWriter};
use crate::formats::parquet::writer::resources::{EncodeBudget, EncodeResources};
use crate::formats::parquet::writer::schema::check_file_columns;
use crate::formats::parquet::writer::sorting::lex_ordering_to_sorting_columns;
use crate::listing::commit::{WriteCancellation, manifest_batch};
use crate::listing::part_file::part_file_name;
use crate::listing::staging::{Publisher, StagedFile};

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
    /// Uploads the files of the task to the object store of the table.
    pub publisher: Publisher,
    /// The root directory of the table.
    pub table_path: Path,
    pub write_id: Uuid,
    /// The index of the writer partition of the task.
    pub partition: usize,
    /// The number of writer partitions.
    pub partitions: usize,
    /// The file name suffix such as `.zstd.parquet`.
    pub file_suffix: String,
    /// The columns whose values determine the directory of each row.
    /// These columns are not written to the files.
    pub partition_columns: Vec<String>,
    pub options: TableParquetOptions,
    /// The ordering that the input is required to have.
    pub ordering: Option<LexOrdering>,
    /// Whether the ordering of the input is recorded in the file metadata.
    pub write_sorting_columns: bool,
    /// The size in bytes of the parts in which each file is uploaded.
    pub buffer_size: usize,
    /// The maximum number of rows in each file, or zero when there is no limit.
    /// The task continues with a new file when a file has this number of rows.
    pub max_records_per_file: u64,
    /// The size in bytes at which the task continues with a new file,
    /// or zero when there is no limit. The size is checked when a row group
    /// has all its rows, so a file can be larger than this.
    pub max_file_size: u64,
    /// The number of partition directories that the task writes to at the same
    /// time when the data has partition columns. The rows of the other directories
    /// are sorted and written when the input has ended.
    pub max_open_files: usize,
    /// The context of the task, which has the memory pool and the temporary files
    /// for the rows that are sorted.
    pub context: Arc<TaskContext>,
    /// The memory that the files of the task share with the other files
    /// of the process for the rows that are not encoded.
    pub budget: Arc<EncodeBudget>,
}

/// The metrics of one Parquet writer task.
pub struct ParquetWriteMetrics {
    rows_written: Count,
    bytes_written: Count,
    files_written: Count,
    /// The rows that were sorted by their partition key before they were written.
    rows_sorted: Count,
    elapsed_compute: Time,
}

impl ParquetWriteMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows_written: MetricBuilder::new(metrics).counter("rows_written", partition),
            bytes_written: MetricBuilder::new(metrics).counter("bytes_written", partition),
            files_written: MetricBuilder::new(metrics).counter("files_written", partition),
            rows_sorted: MetricBuilder::new(metrics).counter("rows_sorted", partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
        }
    }

    fn record(&self, file: &StagedFile) {
        self.rows_written.add(file.num_rows as usize);
        self.bytes_written.add(file.size as usize);
        self.files_written.add(1);
    }
}

/// The files that a writer task has staged and not reported.
///
/// The task owns these files until its manifest is taken. It removes them
/// when it fails or is canceled before that. None of the files is visible
/// to the readers of the table in either case. The files stay in the object
/// store as staged files when the process is stopped, or when the manifest
/// was taken and no commit of the write receives it.
struct TaskFiles {
    publisher: Publisher,
    table_path: Path,
    write_id: Uuid,
    state: Mutex<TaskFilesState>,
    /// Canceled when the task has ended without returning its manifest.
    abandoned: CancellationToken,
}

#[derive(Default)]
struct TaskFilesState {
    staged: Vec<StagedFile>,
    /// Whether the task has ended without returning its manifest.
    abandoned: bool,
}

impl TaskFiles {
    fn new(publisher: Publisher, table_path: Path, write_id: Uuid) -> Self {
        Self {
            publisher,
            table_path,
            write_id,
            state: Mutex::new(TaskFilesState::default()),
            abandoned: CancellationToken::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, TaskFilesState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Encodes the remaining rows of a file and uploads its remaining data.
    ///
    /// This is done in a separate task, since some of the requests of an upload
    /// are not canceled when the caller is. When the writer task is abandoned
    /// while the rows are encoded, the task drops the file, which ends the jobs
    /// of the file and abandons its upload. Once the last data of the file
    /// is being uploaded, the task keeps running, and it removes the staged
    /// file if the writer task has been abandoned by the time the file is staged.
    async fn close(self: &Arc<Self>, writer: ParquetFileWriter) -> Result<StagedFile> {
        self.start_close(writer)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    /// Starts to upload the remaining data of a file as [`Self::close`] does,
    /// without waiting for it.
    fn start_close(self: &Arc<Self>, writer: ParquetFileWriter) -> JoinHandle<Result<StagedFile>> {
        let files = Arc::clone(self);
        tokio::spawn(async move {
            let mut writer = writer;
            tokio::select! {
                biased;
                () = files.abandoned.cancelled() => return exec_err!("the write is canceled"),
                flushed = writer.flush() => flushed?,
            }
            let file = writer.close().await?;
            let abandoned = {
                let mut state = files.state();
                if !state.abandoned {
                    state.staged.push(file.clone());
                }
                state.abandoned
            };
            if abandoned {
                files.publisher.abort(&file).await;
            }
            Ok(file)
        })
    }

    /// The manifest of the task was taken, so the commit of the write owns
    /// the files from here on.
    fn release(&self) {
        self.state().staged.clear();
    }
}

/// Abandons the files of a writer task when dropped,
/// unless the task has released them.
struct AbandonGuard(Arc<TaskFiles>);

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        let staged = {
            let mut state = self.0.state();
            state.abandoned = true;
            std::mem::take(&mut state.staged)
        };
        // The files that are being completed stop encoding.
        self.0.abandoned.cancel();
        if staged.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            log::warn!(
                "{} staged files of the write {} to {} are not removed, \
                since a writer task has ended outside of a runtime",
                staged.len(),
                self.0.write_id,
                self.0.table_path
            );
            return;
        };
        log::warn!(
            "removing {} staged files of the write {} to {} \
            after a writer task has ended without success",
            staged.len(),
            self.0.write_id,
            self.0.table_path
        );
        let files = Arc::clone(&self.0);
        runtime.spawn(async move { files.publisher.abort_all(&staged).await });
    }
}

/// Creates the files of a writer task. All files have the same schema.
pub(super) struct FileFactory {
    files: Arc<TaskFiles>,
    partition: usize,
    file_suffix: String,
    schema: SchemaRef,
    /// Whether the files are in partition directories.
    partitioned: bool,
    props: WriterProperties,
    skip_arrow_metadata: bool,
    buffer_size: usize,
    max_records_per_file: u64,
    max_file_size: u64,
    resources: Arc<EncodeResources>,
}

impl FileFactory {
    fn try_new(
        task: ParquetWriteTask,
        files: Arc<TaskFiles>,
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
        // written to a column, which is the number below for a column that has
        // values missing or repeated, and the limit on the rows of a page otherwise.
        // The number is at most the limit on the rows of a page, so that a page
        // does not get more rows than a small limit allows.
        let page_rows = match options.global.data_page_row_count_limit {
            0 => UNLIMITED_PAGE_ROWS,
            rows => rows,
        };
        options.global.data_page_row_count_limit = page_rows;
        options.global.write_batch_size = page_rows.min(MAX_WRITE_BATCH_SIZE);
        let mut builder = WriterPropertiesBuilder::try_from(&options)?;
        if let Some(sorting) = task
            .ordering
            .as_ref()
            .filter(|_| task.write_sorting_columns)
            .and_then(|ordering| sorting_columns(ordering, input_schema, &schema))
        {
            builder = builder.set_sorting_columns(Some(sorting));
        }
        Ok(Self {
            files,
            partition: task.partition,
            file_suffix: task.file_suffix,
            schema,
            partitioned: !task.partition_columns.is_empty(),
            props: builder.build(),
            skip_arrow_metadata,
            buffer_size: task.buffer_size,
            max_records_per_file: task.max_records_per_file,
            max_file_size: task.max_file_size,
            resources,
        })
    }

    fn create(&self, directory: &Path, counter: u32) -> Result<ParquetFileWriter> {
        let files = &self.files;
        let name = part_file_name(
            self.partition,
            &files.write_id,
            counter,
            &self.file_suffix,
            self.partitioned,
        );
        let location = directory.clone().join(name);
        let upload = files.publisher.upload(
            &files.table_path,
            &files.write_id,
            location.clone(),
            self.buffer_size,
            MAX_UPLOAD_CONCURRENCY,
        );
        ParquetFileWriter::try_new(
            upload,
            location,
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
pub(super) struct ClosingFile {
    task: JoinHandle<Result<StagedFile>>,
    /// The memory that the file used when it received its last row.
    memory_size: usize,
}

impl ClosingFile {
    /// Starts to complete the file.
    fn start(factory: &FileFactory, writer: ParquetFileWriter) -> Self {
        Self {
            memory_size: writer.memory_size(),
            task: factory.files.start_close(writer),
        }
    }

    /// The memory that the task counts for the file until the file is staged.
    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// Waits until the file is staged.
    pub async fn wait(self) -> Result<StagedFile> {
        self.task
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    }
}

impl DirectoryWriter {
    pub fn new(directory: Path) -> Self {
        Self::starting_at(directory, 0)
    }

    /// Returns the writer whose first file has the given number in its name.
    pub fn starting_at(directory: Path, counter: u32) -> Self {
        Self {
            directory,
            writer: None,
            closing: None,
            next_counter: counter,
        }
    }

    /// The number in the name of the next file.
    pub fn next_counter(&self) -> u32 {
        self.next_counter
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
        written: &mut Vec<StagedFile>,
    ) -> Result<()> {
        let mut batch = batch.clone();
        loop {
            let writer = self.open(factory)?;
            // The rows that the file cannot take are written to the next file,
            // so that no file has more rows than the limit.
            let rest = match factory.max_records_per_file {
                0 => None,
                limit => {
                    let room = limit.saturating_sub(writer.rows());
                    ((batch.num_rows() as u64) > room).then(|| {
                        let room = room as usize;
                        let rest = batch.slice(room, batch.num_rows() - room);
                        batch = batch.slice(0, room);
                        rest
                    })
                }
            };
            let flushed = writer.write(&batch).await?;
            let full = (factory.max_records_per_file > 0
                && writer.rows() >= factory.max_records_per_file)
                || (flushed
                    && factory.max_file_size > 0
                    && writer.reaches_size(factory.max_file_size).await?);
            if full {
                // The file is completed while the next file receives rows, so that
                // the task does not stop reading its input until the row groups
                // of the file are encoded. Only one file is completed at a time.
                self.wait_for_closing(written).await?;
                if let Some(writer) = self.writer.take() {
                    self.closing = Some(ClosingFile::start(factory, *writer));
                }
            }
            match rest {
                Some(rest) => batch = rest,
                None => return Ok(()),
            }
        }
    }

    /// Whether the directory has received a file.
    /// This is for a writer whose first file has the number zero.
    fn has_files(&self) -> bool {
        self.next_counter > 0
    }

    pub fn memory_size(&self) -> usize {
        self.writer.as_ref().map_or(0, |x| x.memory_size())
            + self.closing.as_ref().map_or(0, |x| x.memory_size)
    }

    /// Adds the rows of the open file to the file as row groups, and waits for
    /// the file that is being completed, so that no file of the directory
    /// keeps rows or row groups in memory.
    pub async fn flush(&mut self, written: &mut Vec<StagedFile>) -> Result<()> {
        self.wait_for_closing(written).await?;
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    /// Waits for the file that is being completed.
    async fn wait_for_closing(&mut self, written: &mut Vec<StagedFile>) -> Result<()> {
        if let Some(closing) = self.closing.take() {
            written.push(closing.wait().await?);
        }
        Ok(())
    }

    pub async fn close(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<StagedFile>,
    ) -> Result<()> {
        self.wait_for_closing(written).await?;
        if let Some(writer) = self.writer.take() {
            written.push(factory.files.close(*writer).await?);
        }
        Ok(())
    }

    /// Starts to complete the files of the directory, and returns the files
    /// that are being completed. More rows for the directory go to a new file.
    pub fn start_close(&mut self, factory: &FileFactory) -> Vec<ClosingFile> {
        let closing = self.closing.take();
        let writer = self.writer.take();
        let writer = writer.map(|writer| ClosingFile::start(factory, *writer));
        closing.into_iter().chain(writer).collect()
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
        written: &mut Vec<StagedFile>,
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

    /// Adds the rows of all files to the files as row groups, so that the files
    /// keep no rows and no row groups in memory.
    async fn flush(&mut self, written: &mut Vec<StagedFile>) -> Result<()> {
        match self {
            Self::Directory(x) => x.flush(written).await,
            Self::Partitioned(x) => x.flush(written).await,
        }
    }

    /// Completes the files. The reservation is kept up to date while rows are still written.
    async fn close(
        &mut self,
        factory: &FileFactory,
        written: &mut Vec<StagedFile>,
        reservation: &MemoryReservation,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<()> {
        match self {
            Self::Directory(x) => x.close(factory, written).await,
            Self::Partitioned(x) => x.close(factory, written, reservation, pool).await,
        }
    }
}

/// Reserves the memory of files in the memory pool.
///
/// When the memory pool does not have the memory, the rows of the files are
/// added to the files as row groups, which have fewer rows than they would
/// have otherwise, and the files then use no memory that is reserved here.
/// The write fails only when the memory that the files have needed since
/// the last call is more than the memory pool has in total.
pub(super) async fn reserve_or_flush<F>(
    reservation: &MemoryReservation,
    pool: &Arc<dyn MemoryPool>,
    size: usize,
    flush: F,
) -> Result<bool>
where
    F: Future<Output = Result<()>>,
{
    let Err(error) = reservation.try_resize(size) else {
        return Ok(false);
    };
    if let MemoryLimit::Finite(limit) = pool.memory_limit()
        && size.saturating_sub(reservation.size()) > limit
    {
        return Err(error);
    }
    flush.await?;
    Ok(true)
}

/// Writes one input partition.
///
/// The stream has a single item, which is the manifest of the task.
/// The task has staged the files of the manifest, and none of them is visible
/// to the readers of the table. The commit of the write owns the files once
/// the stream is read to its end after the manifest, which shows that
/// the manifest was taken. If the task fails, or if the stream is dropped
/// before that, the task removes what it has staged. The task also fails
/// when the commit of the write runs in the same process and cancels the write.
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
        Ended,
    }

    let state = State::Write(Box::new((task, input, reservation, metrics)));
    futures::stream::unfold(state, |state| async move {
        match state {
            State::Write(args) => {
                let (task, input, reservation, metrics) = *args;
                let files = Arc::new(TaskFiles::new(
                    task.publisher.clone(),
                    task.table_path.clone(),
                    task.write_id,
                ));
                let guard = AbandonGuard(Arc::clone(&files));
                let canceled = WriteCancellation::of(&task.context);
                let written = write_files(task, files, input, reservation, metrics);
                let written = match canceled {
                    // The commit of the write runs in this process and stops the task.
                    Some(canceled) => tokio::select! {
                        biased;
                        () = canceled.canceled() => exec_err!("the write is canceled"),
                        written = written => written,
                    },
                    None => written.await,
                };
                match written {
                    Ok(manifest) => Some((Ok(manifest), State::Written(guard))),
                    Err(e) => Some((Err(e), State::Ended)),
                }
            }
            State::Written(guard) => {
                // The reader has taken the manifest and asked for more.
                guard.0.release();
                None
            }
            State::Ended => None,
        }
    })
}

/// Writes the files of one input partition and returns the manifest of the task.
async fn write_files(
    task: ParquetWriteTask,
    files: Arc<TaskFiles>,
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    metrics: ParquetWriteMetrics,
) -> Result<RecordBatch> {
    let partition = task.partition;
    let partitions = task.partitions;
    let input_schema = input.schema();
    let (mut output, schema) = if task.partition_columns.is_empty() {
        let output = DirectoryWriter::new(task.table_path.clone());
        (TaskOutput::Directory(output), Arc::clone(&input_schema))
    } else {
        let router = PartitionRouter::try_new(&input_schema, &task.partition_columns)?;
        let schema = Arc::clone(router.file_schema());
        let output = PartitionedWriter::new(
            router,
            task.table_path.clone(),
            task.max_open_files,
            task.ordering.clone(),
            Arc::clone(&task.context),
            metrics.rows_sorted.clone(),
        );
        (TaskOutput::Partitioned(Box::new(output)), schema)
    };
    // The plan has checked the columns. They are checked here as well,
    // since a task can be created without the plan.
    check_file_columns(schema.fields().iter().map(|field| field.as_ref()))?;
    // The memory that the files take from the encode budget, and the encoded data
    // that the object store has not stored, are counted separately from
    // the memory of the encoders.
    let pool = Arc::clone(task.context.memory_pool());
    let resources = Arc::new(EncodeResources::new(
        Arc::clone(&task.budget),
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
        let size = output.memory_size();
        if reserve_or_flush(&reservation, &pool, size, output.flush(&mut written)).await? {
            reservation.try_resize(output.memory_size())?;
        }
    }
    if let TaskOutput::Directory(output) = &mut output
        && partition == 0
        && !output.has_files()
        && rows_not_supported.is_none()
    {
        // Without partition columns, the first partition always produces a file,
        // so that the schema of the data can be read from the table even when
        // the result is empty. This is skipped for a schema without columns,
        // so that an empty result can still be written.
        output.open(&factory)?;
    }
    output
        .close(&factory, &mut written, &reservation, &pool)
        .await?;
    reservation.free();
    for file in &written {
        metrics.record(file);
    }
    manifest_batch(partition, partitions, &written)
}
