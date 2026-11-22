// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, RecordBatch,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, ArrowWriterOptions,
    compute_leaves,
};
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion_datasource::write::SharedBuffer;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinHandle;

use crate::formats::parquet::writer::resources::{Charge, EncodeResources};
use crate::listing::staging::{Publish, StagedUpload, upload_error};

/// The size of the encoded data of a file that is kept in memory
/// before the data is handed to the upload.
const UPLOAD_THRESHOLD: usize = 1024 * 1024;

/// The time for which a task encodes or copies data before it lets the other
/// tasks of the runtime run.
///
/// The jobs run on the threads of the runtime, where a task that does not
/// yield delays every other task. Measured with 48 files of 16 columns that
/// were written at the same time on 48 threads, as the time of the write and
/// the delay of a timer of 1 ms on the same runtime at the 99th percentile:
/// 6.32 s and 88 ms without yielding, 6.37 s and 52 ms with 20 ms,
/// 6.43 s and 27 ms with 10 ms, 6.58 s and 10 ms with 5 ms, and 7.05 s and
/// 4.5 ms with 1 ms. With the jobs on the threads for blocking tasks instead,
/// the write took 6.56 s, the timer was 486 ms late, and the process used
/// 1.8 times the memory.
const TIME_SLICE: Duration = Duration::from_millis(10);

/// The number of rows below which a batch is combined with the batches after it
/// before its rows are encoded or kept for the jobs.
///
/// A batch has a cost for each of its columns that does not depend on its rows:
/// an array with two rows used about 250 bytes of memory and took longer
/// to encode than a thousand rows of a larger array. Measured with 20 million
/// rows of 17 columns that 48 tasks wrote to 100 directories each, in batches
/// of two rows for each directory: 9.6 s, 437 s of CPU time and 55 GiB without
/// combining the batches, and 5.8 s, 257 s and 19 GiB with it. A column writer
/// takes at most this number of rows at a time, so larger batches are not combined.
const SMALL_BATCH_ROWS: usize = 1024;

/// The number of small batches that are combined at a time. This is the square
/// root of [`SMALL_BATCH_ROWS`]: batches of one row are combined to batches of
/// this number of rows, and this number of those are combined to a batch that
/// is no longer small. So a row is copied twice at most, and a file keeps
/// less than twice this number of small batches.
const SMALL_BATCH_COUNT: usize = 32;

/// Encodes a Parquet file.
///
/// The writer task divides its input into row groups, and it collects the rows
/// of a row group in slabs. It starts one job for each top-level column
/// of a slab. The job of a column continues with the column writers of the job
/// of the same column for the slab before it, and the job of the last slab
/// of a row group completes the column. The writer task does not wait for
/// the jobs, so the jobs of several slabs and row groups run at the same time
/// on the threads of the runtime. The row groups are added to the file
/// in order as they are completed.
///
/// The rows that are kept for the jobs are taken from the encode budget that
/// all files of the process share, and so are the row groups that are encoded
/// and not in the file. When the budget is exhausted, the writer task waits
/// for the jobs of its own file. When its file has no jobs, it encodes
/// the rows itself and adds their row group to the file before it continues.
/// It never waits for another file, so the files of a process cannot block
/// each other, whatever the files that do not receive rows have taken.
///
/// The writer task waits for the jobs of each slab when the size of a row
/// group is limited in bytes, since the size of the encoded rows is known
/// only then.
///
/// Batches of few rows are combined before they are encoded, whatever the budget
/// has. Each column writer then receives the same arrays in the same order as
/// it does in [`ArrowWriter`] for the combined batches, so the file has the same
/// content as the file that [`ArrowWriter`] writes for them, unless the size of
/// a row group is limited in bytes or a batch is larger than a slab.
pub(super) struct ColumnEncoder {
    writer: SerializedFileWriter<SharedBuffer>,
    /// The encoded data that has not been handed to the upload.
    buffer: SharedBuffer,
    upload: StagedUpload,
    /// The size of the data that is handed to the upload and not stored.
    uploading: Arc<AtomicUsize>,
    /// The encoded data that the object store has not stored, in the memory pool.
    /// The task does not wait for the data of the files that it completes,
    /// so the data is counted here and not with the memory of the file.
    unstored: Charge,
    /// The maximum size of the data that is handed to the upload at a time.
    upload_chunk_size: usize,
    schema: SchemaRef,
    /// The last rows of the row group that receives rows, when they have arrived
    /// in batches of few rows that are not combined yet.
    small: SmallBatches,
    /// The rows of the row group that receives rows which no job has received.
    pending: Slab,
    /// The row group that receives rows, once it has column writers.
    open: Option<RowGroup>,
    /// The row groups that have all their rows and are not in the file, in file order.
    sealed: VecDeque<RowGroup>,
    /// The number of row groups that have received column writers.
    started: usize,
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    max_row_group_row_count: usize,
    max_row_group_bytes: Option<usize>,
    /// The rows that the row group that receives rows takes before the size
    /// of its encoded rows is needed again, when the size of a row group
    /// is limited in bytes.
    measured: Option<MeasuredRows>,
    /// The number of leaf columns of each top-level column.
    leaf_counts: Vec<usize>,
    /// The size and the number of the rows that the file has received,
    /// without the batches of few rows that are not combined.
    input_bytes: u64,
    input_rows: u64,
    resources: Arc<EncodeResources>,
    /// Tells the jobs to stop. This is set when the encoder is dropped.
    stop: Arc<AtomicBool>,
    /// The time since which the writer task has encoded or copied data
    /// without letting other tasks run.
    slice: TimeSlice,
}

/// The time for which a task has encoded or copied data without letting
/// the other tasks of the runtime run.
struct TimeSlice(Instant);

impl TimeSlice {
    fn start() -> Self {
        Self(Instant::now())
    }

    /// Lets the other tasks of the runtime run if the time slice has ended.
    async fn end(&mut self) {
        if self.0.elapsed() >= TIME_SLICE {
            tokio::task::yield_now().await;
            self.0 = Instant::now();
        }
    }
}

/// The rows that a row group takes without waiting for the size of its encoded rows.
struct MeasuredRows {
    rows: usize,
    /// Whether the row group is expected to reach the limit on its size with the rows.
    last: bool,
}

/// Batches of few rows that are combined before they are encoded.
#[derive(Default)]
struct SmallBatches {
    batches: Vec<RecordBatch>,
    /// The number of batches at the end that are as the file has received them.
    /// The batches before them are combined batches.
    received: usize,
    rows: usize,
    /// The expected size of the values of the batches. The file has less than
    /// [`SMALL_BATCH_ROWS`] rows in these batches, and they are counted with
    /// the memory of the file and not in the budget.
    bytes: usize,
}

/// Rows of a row group that are handed to the jobs together.
#[derive(Default)]
struct Slab {
    batches: Vec<RecordBatch>,
    rows: usize,
    /// The size of the values of each top-level column.
    column_bytes: Vec<usize>,
    charge: Option<Charge>,
}

impl Slab {
    fn bytes(&self) -> usize {
        self.column_bytes.iter().sum()
    }
}

/// A row group that has column writers.
struct RowGroup {
    /// The state of each top-level column.
    columns: Vec<ColumnState>,
    /// The memory of the column writers of each top-level column,
    /// as of the last slab that the column has encoded.
    memory: Arc<[AtomicUsize]>,
    /// The number of rows that the columns have received.
    rows: usize,
}

/// The leaf columns of a top-level column of a row group.
enum ColumnState {
    /// The column writers have encoded all rows that they have received.
    Idle(Vec<ArrowColumnWriter>),
    /// The last job of the column.
    Running(Job),
}

/// What the job of a column returns.
enum ColumnOutput {
    /// The column writers, when the row group receives more rows, and the turn
    /// of the job to encode, when the next job of the column has started.
    Open(Vec<ArrowColumnWriter>, Option<OwnedSemaphorePermit>),
    /// The encoded leaf columns, and their size in the budget
    /// until the row group is in the file.
    Closed(Vec<ArrowColumnChunk>, Charge),
}

/// A task that is canceled when this is dropped.
struct Job {
    task: JoinHandle<Result<ColumnOutput>>,
    /// Whether the next job of the column has started.
    followed: Arc<AtomicBool>,
}

impl Drop for Job {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Job {
    async fn join(mut self) -> Result<ColumnOutput> {
        match (&mut self.task).await {
            Ok(output) => output,
            Err(e) => Err(ParquetError::General(match e.try_into_panic() {
                Ok(panic) => {
                    let message = panic
                        .downcast_ref::<&str>()
                        .map(|x| x.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    format!("the column encoder has panicked: {message}")
                }
                Err(_) => "the column encoder has been canceled".to_string(),
            })),
        }
    }
}

impl ColumnState {
    fn is_idle(&self) -> bool {
        matches!(self, Self::Idle(_))
    }

    fn is_finished(&self) -> bool {
        match self {
            Self::Idle(_) => true,
            Self::Running(job) => job.task.is_finished(),
        }
    }

    /// The column writers, after the jobs of the column have ended,
    /// and the turn to encode that the last job has passed on.
    async fn writers(self) -> Result<(Vec<ArrowColumnWriter>, Option<OwnedSemaphorePermit>)> {
        match self {
            Self::Idle(writers) => Ok((writers, None)),
            Self::Running(job) => match job.join().await? {
                ColumnOutput::Open(writers, turn) => Ok((writers, turn)),
                ColumnOutput::Closed(..) => Err(ParquetError::General(
                    "the column of a row group that receives rows is completed".to_string(),
                )),
            },
        }
    }
}

/// What the job of a column needs in addition to its input.
struct JobContext {
    resources: Arc<EncodeResources>,
    stop: Arc<AtomicBool>,
    /// The memory of the column writers of each top-level column of the row group.
    memory: Arc<[AtomicUsize]>,
    /// The index of the top-level column of the job.
    index: usize,
    /// Whether the next job of the column has started.
    followed: Arc<AtomicBool>,
}

impl ColumnEncoder {
    pub fn try_new(
        upload: StagedUpload,
        upload_chunk_size: usize,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        resources: Arc<EncodeResources>,
    ) -> Result<Self> {
        let max_row_group_row_count = props.max_row_group_row_count().unwrap_or(usize::MAX);
        let max_row_group_bytes = props.max_row_group_bytes();
        let buffer = SharedBuffer::new(UPLOAD_THRESHOLD);
        let options = ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(skip_arrow_metadata);
        let writer =
            ArrowWriter::try_new_with_options(buffer.clone(), Arc::clone(&schema), options)?;
        let (writer, row_group_writer_factory) = writer.into_serialized_writer()?;
        let mut leaf_counts = vec![0; schema.fields().len()];
        for leaf in 0..writer.schema_descr().num_columns() {
            let column = writer.schema_descr().get_column_root_idx(leaf);
            if let Some(count) = leaf_counts.get_mut(column) {
                *count += 1;
            }
        }
        Ok(Self {
            writer,
            buffer,
            uploading: upload.buffered(),
            upload,
            unstored: resources.charge_outside_budget(),
            upload_chunk_size: upload_chunk_size.max(1),
            schema,
            small: SmallBatches::default(),
            pending: Slab::default(),
            open: None,
            sealed: VecDeque::new(),
            started: 0,
            row_group_writer_factory,
            max_row_group_row_count: max_row_group_row_count.max(1),
            max_row_group_bytes,
            measured: None,
            leaf_counts,
            input_bytes: 0,
            input_rows: 0,
            resources,
            stop: Arc::new(AtomicBool::new(false)),
            slice: TimeSlice::start(),
        })
    }

    /// The size in bytes and the number of rows of the row groups in the file.
    pub fn written(&self) -> (usize, i64) {
        let rows = self
            .writer
            .flushed_row_groups()
            .iter()
            .map(|x| x.num_rows())
            .sum();
        (self.writer.bytes_written(), rows)
    }

    /// The size of the rows that the file has received.
    pub fn input_bytes(&self) -> u64 {
        self.input_bytes
    }

    /// The number of row groups that have all their rows.
    pub fn row_groups(&self) -> usize {
        self.writer.flushed_row_groups().len() + self.sealed.len()
    }

    /// The number of row groups that have rows and are not in the file.
    #[cfg(test)]
    pub fn row_groups_in_flight(&self) -> usize {
        let rows = self.pending.rows + self.small.rows;
        self.sealed.len() + usize::from(self.open.is_some() || rows > 0)
    }

    /// The memory used by the column writers and by the batches of few rows
    /// that are not combined. This does not include the memory that is taken
    /// from the encode budget, and the encoded data that the object store
    /// has not stored, which are in the memory pool already.
    pub fn memory_size(&self) -> usize {
        let columns: usize = self
            .open
            .iter()
            .chain(&self.sealed)
            .flat_map(|x| x.memory.iter())
            .map(|x| x.load(Ordering::Relaxed))
            .sum();
        columns + self.small.bytes
    }

    /// Counts the encoded data that the object store has not stored.
    /// The data that the object store stores later is counted until this is called again.
    fn count_unstored(&mut self) {
        let buffered = self.buffer.buffer.try_lock().map_or(0, |x| x.len());
        self.unstored
            .resize(buffered + self.uploading.load(Ordering::Relaxed));
    }

    /// Writes a batch to the file.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.slice = TimeSlice::start();
        // The row groups that have been completed since the last batch are added
        // to the file here, so that they do not wait for the next row group
        // to have all its rows.
        self.append_completed().await?;
        if batch.num_columns() != self.schema.fields().len() {
            return Err(ParquetError::ArrowError(format!(
                "Incompatible schema: writer has {} top-level fields but batch has {} columns",
                self.schema.fields().len(),
                batch.num_columns()
            )));
        }
        let mut rest = batch.clone();
        while rest.num_rows() > 0 {
            let mut rows = rest.num_rows().min(self.row_group_room());
            if let Some(max_bytes) = self.max_row_group_bytes {
                let (measured, last) = self
                    .measured
                    .as_ref()
                    .map_or((0, false), |x| (x.rows, x.last));
                let measured = if measured > 0 {
                    measured
                } else if last {
                    self.flush_small().await?;
                    self.seal()?;
                    continue;
                } else {
                    match self.measure(max_bytes).await? {
                        Some(measured) => self.measured.insert(measured).rows,
                        None => {
                            self.seal()?;
                            continue;
                        }
                    }
                };
                rows = rows.min(measured);
            }
            if rows < SMALL_BATCH_ROWS {
                let piece = rest.slice(0, rows);
                rest = rest.slice(rows, rest.num_rows() - rows);
                if let Some(measured) = &mut self.measured {
                    measured.rows = measured.rows.saturating_sub(rows);
                }
                self.add_small(piece).await?;
                continue;
            }
            self.flush_small().await?;
            let piece = rest.slice(0, rows);
            let column_bytes = column_sizes(&piece);
            // A batch that is larger than a slab is divided, so that a job
            // does not encode more than a slab at a time.
            let bytes: usize = column_bytes.iter().sum();
            let slab_bytes = self.resources.slab_bytes();
            let (piece, column_bytes) = if bytes > slab_bytes {
                rows = (rows as u128 * slab_bytes as u128 / bytes as u128).max(1) as usize;
                let piece = rest.slice(0, rows);
                let column_bytes = column_sizes(&piece);
                (piece, column_bytes)
            } else {
                (piece, column_bytes)
            };
            rest = rest.slice(rows, rest.num_rows() - rows);
            if let Some(measured) = &mut self.measured {
                measured.rows = measured.rows.saturating_sub(rows);
            }
            self.add(piece, column_bytes).await?;
        }
        Ok(())
    }

    /// Adds a batch of few rows to the row group that receives rows. The batch
    /// is combined with the batches of few rows before and after it.
    async fn add_small(&mut self, piece: RecordBatch) -> Result<()> {
        // The size of the values of each batch is not computed, since that
        // takes longer than the batch does once the batches are combined.
        self.small.bytes += match self.input_rows {
            0 => column_sizes(&piece).iter().sum(),
            rows => piece.num_rows() * (self.input_bytes / rows).max(1) as usize,
        };
        self.small.rows += piece.num_rows();
        self.small.batches.push(piece);
        self.small.received += 1;
        if self.small.rows >= SMALL_BATCH_ROWS || self.is_full() {
            return self.flush_small().await;
        }
        if self.small.received >= SMALL_BATCH_COUNT {
            let combined = self.small.batches.len().saturating_sub(self.small.received);
            if let Some(batch) = self.small.batches.get(combined..).and_then(combine) {
                self.small.batches.truncate(combined);
                self.small.batches.push(batch);
            }
            // The batches are not combined again if they cannot be combined.
            self.small.received = 0;
        }
        Ok(())
    }

    /// Adds the batches of few rows to the row group as one batch.
    async fn flush_small(&mut self) -> Result<()> {
        if self.small.batches.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.small).batches;
        let batches = match combine(&batches) {
            Some(batch) => vec![batch],
            // The batches have a data type that cannot be combined.
            None => batches,
        };
        for batch in batches {
            let column_bytes = column_sizes(&batch);
            self.add(batch, column_bytes).await?;
        }
        Ok(())
    }

    /// The number of rows that the row group that receives rows can still take.
    fn row_group_room(&self) -> usize {
        self.max_row_group_row_count
            .saturating_sub(self.row_group_rows())
            .max(1)
    }

    /// The number of rows that the row group that receives rows has.
    fn row_group_rows(&self) -> usize {
        self.open.as_ref().map_or(0, |x| x.rows) + self.pending.rows + self.small.rows
    }

    /// Returns the rows that the row group that receives rows takes next when
    /// the size of a row group is limited in bytes, or nothing when the row
    /// group has reached the limit.
    ///
    /// The size of the encoded rows is known only after they are encoded,
    /// so this waits for the jobs of the row group. The row group takes one
    /// row first. After that it takes as many rows as are expected to fit
    /// from the average size of its rows so far, and at most as many rows
    /// as it has, so that the average is never used for more rows than it
    /// was measured with. The row group ends without another measurement
    /// when it has taken the rows that are expected to fit.
    async fn measure(&mut self, max_bytes: usize) -> Result<Option<MeasuredRows>> {
        self.flush_small().await?;
        self.submit(false)?;
        self.join_open().await?;
        let Some(open) = &self.open else {
            return Ok(Some(MeasuredRows {
                rows: 1,
                last: false,
            }));
        };
        let estimated: usize = open
            .columns
            .iter()
            .map(|column| match column {
                ColumnState::Idle(writers) => writers
                    .iter()
                    .map(|x| x.get_estimated_total_bytes())
                    .sum::<usize>(),
                ColumnState::Running(_) => 0,
            })
            .sum();
        if estimated >= max_bytes {
            return Ok(None);
        }
        let fit = match estimated / open.rows.max(1) {
            0 => usize::MAX,
            row_bytes => (max_bytes - estimated) / row_bytes,
        };
        Ok((fit > 0).then_some(MeasuredRows {
            rows: fit.min(open.rows),
            last: fit <= open.rows,
        }))
    }

    /// Adds rows to the row group that receives rows.
    async fn add(&mut self, piece: RecordBatch, column_bytes: Vec<usize>) -> Result<()> {
        let bytes: usize = column_bytes.iter().sum();
        self.input_bytes += bytes as u64;
        self.input_rows += piece.num_rows() as u64;
        loop {
            if let Some(charge) = self.resources.try_charge(bytes) {
                self.pending.rows += piece.num_rows();
                if self.pending.column_bytes.is_empty() {
                    self.pending.column_bytes = column_bytes;
                } else {
                    for (total, bytes) in self.pending.column_bytes.iter_mut().zip(column_bytes) {
                        *total += bytes;
                    }
                }
                match &mut self.pending.charge {
                    Some(total) => total.merge(charge),
                    total @ None => *total = Some(charge),
                }
                self.pending.batches.push(piece);
                break;
            }
            // The budget is exhausted. The rows wait for the jobs of this file,
            // which return what they have taken from the budget.
            if !self.release().await? {
                // The file has nothing in the budget, so the rows are encoded here,
                // and their row group is added to the file before more rows are.
                self.encode(&piece)?;
                if self.is_full() {
                    self.complete().await?;
                }
                self.slice.end().await;
                return Ok(());
            }
        }
        if self.is_full() {
            self.seal()?;
        } else if self.pending.bytes() >= self.resources.slab_bytes() {
            self.submit(false)?;
        }
        Ok(())
    }

    /// Whether the row group that receives rows has all its rows.
    fn is_full(&self) -> bool {
        self.row_group_rows() >= self.max_row_group_row_count
    }

    /// Makes the file return a part of what it has taken from the budget,
    /// and waits for that. Returns whether the file had anything in the budget.
    async fn release(&mut self) -> Result<bool> {
        if self.pending.rows > 0 {
            self.submit(false)?;
            return Ok(true);
        }
        if !self.sealed.is_empty() {
            self.append_next().await?;
            return Ok(true);
        }
        if self
            .open
            .as_ref()
            .is_some_and(|x| !x.columns.iter().all(|x| x.is_idle()))
        {
            self.join_open().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns the row group that receives rows, with column writers.
    fn open(&mut self) -> Result<&mut RowGroup> {
        match &mut self.open {
            Some(open) => Ok(open),
            open @ None => {
                let mut writers = self
                    .row_group_writer_factory
                    .create_column_writers(self.started)?
                    .into_iter();
                self.started += 1;
                let columns: Vec<_> = self
                    .leaf_counts
                    .iter()
                    .map(|leaves| ColumnState::Idle(writers.by_ref().take(*leaves).collect()))
                    .collect();
                let memory = columns.iter().map(|_| AtomicUsize::new(0)).collect();
                Ok(open.insert(RowGroup {
                    columns,
                    memory,
                    rows: 0,
                }))
            }
        }
    }

    /// Encodes rows in the writer task. The columns of the row group
    /// that receives rows must have no jobs.
    fn encode(&mut self, piece: &RecordBatch) -> Result<()> {
        let elapsed_compute = self.resources.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        let schema = Arc::clone(&self.schema);
        let open = self.open()?;
        for (((field, array), column), memory) in schema
            .fields()
            .iter()
            .zip(piece.columns())
            .zip(&mut open.columns)
            .zip(open.memory.iter())
        {
            let ColumnState::Idle(writers) = column else {
                return Err(ParquetError::General(
                    "the column of a row group has a job".to_string(),
                ));
            };
            encode_array(field, array, writers)?;
            memory.store(
                writers.iter().map(|x| x.memory_size()).sum(),
                Ordering::Relaxed,
            );
        }
        open.rows += piece.num_rows();
        Ok(())
    }

    /// Completes the row group that receives rows in the writer task, and adds
    /// the row group to the file. The columns of the row group must have no jobs,
    /// and the row groups before it must be in the file.
    async fn complete(&mut self) -> Result<()> {
        self.measured = None;
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let mut chunks = vec![];
        {
            let _timer = self.resources.elapsed_compute().timer();
            for column in open.columns {
                let ColumnState::Idle(writers) = column else {
                    return Err(ParquetError::General(
                        "the column of a row group has a job".to_string(),
                    ));
                };
                for writer in writers {
                    chunks.push(writer.close()?);
                }
            }
        }
        self.append(chunks).await
    }

    /// Starts the jobs for the rows of the row group that no job has received.
    /// The jobs complete the columns of the row group if these are its last rows.
    fn submit(&mut self, last: bool) -> Result<()> {
        if self.pending.rows == 0 && !last {
            return Ok(());
        }
        let slab = std::mem::take(&mut self.pending);
        let resources = Arc::clone(&self.resources);
        let stop = Arc::clone(&self.stop);
        let schema = Arc::clone(&self.schema);
        let open = self.open()?;
        open.rows += slab.rows;
        let mut charge = slab.charge;
        let mut column_bytes = slab.column_bytes.into_iter();
        let columns = std::mem::take(&mut open.columns);
        for (index, (field, column)) in schema.fields().iter().zip(columns).enumerate() {
            let arrays: Vec<ArrayRef> = slab
                .batches
                .iter()
                .filter_map(|x| x.columns().get(index).cloned())
                .collect();
            let charge = charge
                .as_mut()
                .map(|x| x.split(column_bytes.next().unwrap_or(0)));
            if let ColumnState::Running(job) = &column {
                job.followed.store(true, Ordering::Release);
            }
            let followed = Arc::new(AtomicBool::new(false));
            let context = JobContext {
                resources: Arc::clone(&resources),
                stop: Arc::clone(&stop),
                memory: Arc::clone(&open.memory),
                index,
                followed: Arc::clone(&followed),
            };
            let job = encode_column(column, Arc::clone(field), arrays, charge, last, context);
            open.columns.push(ColumnState::Running(Job {
                task: tokio::spawn(job),
                followed,
            }));
        }
        Ok(())
    }

    /// Ends the row group that receives rows. The row group is added to the file
    /// when its columns are completed and all row groups before it are in the file.
    fn seal(&mut self) -> Result<()> {
        self.measured = None;
        if self.open.is_none() && self.pending.rows == 0 {
            return Ok(());
        }
        self.submit(true)?;
        if let Some(open) = self.open.take() {
            self.sealed.push_back(open);
        }
        Ok(())
    }

    /// Waits for the jobs of the row group that receives rows.
    async fn join_open(&mut self) -> Result<()> {
        let Some(open) = &mut self.open else {
            return Ok(());
        };
        if open.columns.iter().all(|x| x.is_idle()) {
            return Ok(());
        }
        let mut columns = Vec::with_capacity(open.columns.len());
        for column in std::mem::take(&mut open.columns) {
            let (writers, _) = column.writers().await?;
            columns.push(ColumnState::Idle(writers));
        }
        open.columns = columns;
        Ok(())
    }

    /// Waits for the first row group to be in the file if the file has
    /// row groups that have all their rows.
    pub async fn wait_for_row_group(&mut self) -> Result<()> {
        self.slice = TimeSlice::start();
        if self.writer.flushed_row_groups().is_empty() {
            self.append_next().await?;
        }
        Ok(())
    }

    /// Writes the remaining data and the footer, and uploads what remains.
    /// Returns the size of the file and how the file is published.
    pub async fn finish(&mut self) -> Result<(usize, Publish)> {
        self.slice = TimeSlice::start();
        self.flush_small().await?;
        self.seal()?;
        self.append_all().await?;
        self.writer.finish()?;
        upload_buffer(&self.buffer, &mut self.upload, self.upload_chunk_size, 0).await?;
        self.count_unstored();
        let publish = self.upload.finish().await.map_err(upload_error)?;
        self.count_unstored();
        Ok((self.writer.bytes_written(), publish))
    }

    /// Adds the row groups that have all their rows to the file,
    /// after waiting for their columns to be completed.
    async fn append_all(&mut self) -> Result<()> {
        while !self.sealed.is_empty() {
            self.append_next().await?;
        }
        Ok(())
    }

    /// Adds the row groups to the file that can be added without waiting for a job.
    async fn append_completed(&mut self) -> Result<()> {
        while self
            .sealed
            .front()
            .is_some_and(|x| x.columns.iter().all(|x| x.is_finished()))
        {
            self.append_next().await?;
        }
        Ok(())
    }

    /// Adds the first of the row groups that are not in the file to the file,
    /// after waiting for its columns to be completed.
    async fn append_next(&mut self) -> Result<()> {
        let Some(row_group) = self.sealed.pop_front() else {
            return Ok(());
        };
        let mut chunks = vec![];
        // The encoded columns stay in the budget until they are in the file.
        let mut charges = vec![];
        for column in row_group.columns {
            let ColumnState::Running(job) = column else {
                return Err(ParquetError::General(
                    "the column of a row group that has all its rows is not completed".to_string(),
                ));
            };
            match job.join().await? {
                ColumnOutput::Closed(column_chunks, charge) => {
                    chunks.extend(column_chunks);
                    charges.push(charge);
                }
                ColumnOutput::Open(..) => {
                    return Err(ParquetError::General(
                        "the column of a row group that has all its rows is not completed"
                            .to_string(),
                    ));
                }
            }
        }
        self.append(chunks).await?;
        drop(charges);
        Ok(())
    }

    /// Adds the columns of a row group to the file.
    async fn append(&mut self, chunks: Vec<ArrowColumnChunk>) -> Result<()> {
        let mut row_group_writer = self.writer.next_row_group()?;
        for chunk in chunks {
            {
                let _timer = self.resources.elapsed_compute().timer();
                chunk.append_to_row_group(&mut row_group_writer)?;
            }
            upload_buffer(
                &self.buffer,
                &mut self.upload,
                self.upload_chunk_size,
                UPLOAD_THRESHOLD,
            )
            .await?;
            // The columns of a large row group take long to copy.
            self.slice.end().await;
        }
        row_group_writer.close()?;
        self.count_unstored();
        Ok(())
    }
}

impl Drop for ColumnEncoder {
    fn drop(&mut self) {
        // A job that is canceled keeps running until it lets other tasks run,
        // so it is told to stop as well.
        self.stop.store(true, Ordering::Release);
    }
}

/// Encodes the arrays of a top-level column of a slab with the column writers
/// of the jobs before it.
async fn encode_column(
    column: ColumnState,
    field: FieldRef,
    arrays: Vec<ArrayRef>,
    charge: Option<Charge>,
    last: bool,
    context: JobContext,
) -> Result<ColumnOutput> {
    // The job continues in the turn of the job of the column before it,
    // if that job has passed its turn on. The columns of a slab are small
    // in a table with many columns, and the next slab of a column is encoded
    // fastest right after the slab before it, on the same thread.
    let (mut writers, turn) = column.writers().await?;
    let turn = match turn {
        Some(turn) => turn,
        None => context.resources.job().await?,
    };
    let mut slice = TimeSlice::start();
    for array in arrays {
        if context.stop.load(Ordering::Acquire) {
            return Err(ParquetError::General(
                "the file writer has been dropped".to_string(),
            ));
        }
        {
            let _timer = context.resources.elapsed_compute().timer();
            encode_array(&field, &array, &mut writers)?;
        }
        // The array is no longer kept for the job.
        drop(array);
        slice.end().await;
    }
    let report = |size: usize| {
        if let Some(memory) = context.memory.get(context.index) {
            memory.store(size, Ordering::Relaxed);
        }
    };
    if !last {
        report(writers.iter().map(|x| x.memory_size()).sum());
        drop(charge);
        let turn = context.followed.load(Ordering::Acquire).then_some(turn);
        return Ok(ColumnOutput::Open(writers, turn));
    }
    let chunks = {
        let _timer = context.resources.elapsed_compute().timer();
        writers
            .into_iter()
            .map(|writer| writer.close())
            .collect::<Result<Vec<_>>>()?
    };
    let size = chunks
        .iter()
        .map(|chunk| chunk.close().bytes_written as usize)
        .sum();
    let encoded = context.resources.charge(size);
    report(0);
    drop(charge);
    Ok(ColumnOutput::Closed(chunks, encoded))
}

/// The size of the values of each column of the batch.
fn column_sizes(batch: &RecordBatch) -> Vec<usize> {
    batch
        .columns()
        .iter()
        .map(|x| logical_size(x.as_ref()))
        .collect()
}

/// Combines the batches into one batch, or returns nothing if there is
/// no batch or if the batches cannot be combined.
fn combine(batches: &[RecordBatch]) -> Option<RecordBatch> {
    match batches {
        [] => None,
        [batch] => Some(batch.clone()),
        [first, ..] => concat_batches(&first.schema(), batches).ok(),
    }
}

/// Writes the leaf columns of an array of a top-level column to their column writers.
fn encode_array(
    field: &FieldRef,
    array: &ArrayRef,
    writers: &mut [ArrowColumnWriter],
) -> Result<()> {
    let leaves = compute_leaves(field.as_ref(), array)?;
    if leaves.len() != writers.len() {
        return Err(ParquetError::ArrowError(format!(
            "Incompatible schema: field '{}' produced {} leaf columns but writer expects {}",
            field.name(),
            leaves.len(),
            writers.len()
        )));
    }
    for (writer, leaf) in writers.iter_mut().zip(&leaves) {
        writer.write(leaf)?;
    }
    Ok(())
}

/// Hands the encoded data to the upload if the size of the data exceeds the threshold.
async fn upload_buffer(
    buffer: &SharedBuffer,
    upload: &mut StagedUpload,
    chunk_size: usize,
    threshold: usize,
) -> Result<()> {
    let data = {
        let mut buffer = buffer
            .buffer
            .try_lock()
            .ok_or_else(|| ParquetError::General("the file buffer is in use".to_string()))?;
        if buffer.len() <= threshold {
            return Ok(());
        }
        let capacity = buffer.capacity();
        Bytes::from(std::mem::replace(
            &mut *buffer,
            Vec::with_capacity(capacity),
        ))
    };
    let mut offset = 0;
    while offset < data.len() {
        let end = data.len().min(offset + chunk_size);
        // This waits when too many parts of the file are being uploaded.
        upload
            .put(data.slice(offset..end))
            .await
            .map_err(upload_error)?;
        offset = end;
    }
    Ok(())
}

/// The size of the values that the array refers to.
///
/// This is not the size of the buffers of the array, since an array can be
/// a slice of buffers that are much larger and that are shared with other arrays.
/// The values of a list or a map are counted for the rows of the array only.
pub(super) fn logical_size(array: &dyn Array) -> usize {
    let nulls = array.nulls().map_or(0, |_| array.len().div_ceil(8));
    match array.data_type() {
        DataType::List(_) => list_size(array.as_list::<i32>()) + nulls,
        DataType::LargeList(_) => list_size(array.as_list::<i64>()) + nulls,
        DataType::Map(_, _) => {
            let map = array.as_map();
            let (start, end) = offset_range(map.offsets());
            let entries = map.entries().slice(start, end - start);
            size_of_val(map.offsets().inner().as_ref()) + logical_size(&entries) + nulls
        }
        DataType::Struct(_) => {
            let columns = array.as_struct().columns();
            columns
                .iter()
                .map(|x| logical_size(x.as_ref()))
                .sum::<usize>()
                + nulls
        }
        _ => {
            let data = array.to_data();
            match data.get_slice_memory_size() {
                Ok(size) => size + view_data_size(&data),
                Err(_) => array.get_array_memory_size(),
            }
        }
    }
}

fn list_size<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> usize {
    let (start, end) = offset_range(list.offsets());
    let values = list.values().slice(start, end - start);
    size_of_val(list.offsets().inner().as_ref()) + logical_size(values.as_ref())
}

/// The range of the values of a list array in the array of its values.
fn offset_range<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> (usize, usize) {
    let start = offsets.first().map_or(0, |x| x.as_usize());
    let end = offsets.last().map_or(start, |x| x.as_usize());
    (start, end.max(start))
}

/// The size of the values of view arrays that are stored outside the views,
/// which the size of a slice of the array data does not include.
fn view_data_size(data: &ArrayData) -> usize {
    let own = match data.data_type() {
        DataType::Utf8View | DataType::BinaryView => data
            .buffer::<u128>(0)
            .iter()
            .take(data.len())
            .map(|view| *view as u32 as usize)
            .filter(|length| *length > 12)
            .sum(),
        _ => 0,
    };
    let children: usize = data.child_data().iter().map(view_data_size).sum();
    own + children
}
