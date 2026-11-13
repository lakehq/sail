//! A stream that is read by a known set of readers, each at its own pace.
//!
//! The stream is polled only when a reader asks for a batch that no reader has read yet,
//! by that reader itself, so that a reader never waits for another reader. The batches
//! that some reader has not read yet are kept, and the others are released. A reader that
//! is not created yet reads from the beginning, so it keeps every batch until it is
//! created or finishes; a reader that is never created keeps them until the tee is
//! dropped. The stream is dropped once every reader has finished or is dropped.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, AsArray, UInt64Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_plan::metrics::SpillMetrics;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use futures::task::ArcWake;
use futures::{Stream, StreamExt};

/// A function that opens the stream when a reader first asks for a batch.
pub type TeeSource = Box<dyn FnOnce() -> Result<SendableRecordBatchStream> + Send>;

/// A stream that is read by a fixed set of readers, which are numbered from zero.
///
/// The batches that some reader has not read yet are kept in memory, within an unbounded
/// memory pool. Under a memory limit they never take memory from the pool. With temporary
/// files, a kept batch is written to disk once a newer batch is kept while a reader still
/// owes it, and the newest kept batch stays in memory outside the pool. Without temporary
/// files, the kept batches stay in memory outside the pool.
///
/// A stream that an operator may read again from the beginning (a nested loop join
/// executes its build side again to spill it) keeps all its batches.
pub struct Tee {
    schema: SchemaRef,
    state: Mutex<TeeState>,
    wakers: Arc<Wakers>,
    /// Whether the memory pool has a limit.
    pool_finite: bool,
    /// Whether temporary files are enabled.
    spill_enabled: bool,
    spill: SpillManager,
    /// The size of the files that the kept batches are written to.
    file_size: usize,
    batch_size: usize,
}

struct TeeState {
    source: Source,
    /// The position in the stream of the first kept batch.
    base: usize,
    batches: VecDeque<Kept>,
    /// The state of each reader: the planned readers, and then the readers that read the
    /// stream again from the beginning.
    readers: Vec<Reader>,
    /// The file that the kept batches are written to, if the last kept batch is in it.
    writing: Option<Writing>,
    reservation: MemoryReservation,
    /// Whether every batch is kept until the stream is dropped, so that a reader can be
    /// created again.
    keep_all: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reader {
    /// The reader is not created yet.
    Unopened,
    Reading {
        position: usize,
    },
    Finished,
}

enum Source {
    Unopened(TeeSource),
    Ready(SendableRecordBatchStream),
    /// A reader is polling the stream.
    Polling,
    Finished,
    Failed(Arc<DataFusionError>),
    /// Every reader has finished or is dropped.
    Stopped,
}

enum Kept {
    Memory {
        batch: RecordBatch,
        size: usize,
    },
    /// The batch at the given index in a file.
    Spilled {
        segment: Arc<Segment>,
        index: usize,
    },
}

/// A file of consecutive kept batches, which is read once it is finished.
struct Segment {
    file: Mutex<Option<Arc<dyn SpillFile>>>,
}

/// Appends a batch to a file, or finishes the file and returns it.
type SegmentWriter =
    Box<dyn FnMut(Option<&RecordBatch>) -> Result<Option<Arc<dyn SpillFile>>> + Send>;

struct Writing {
    writer: SegmentWriter,
    segment: Arc<Segment>,
    /// The number of batches and bytes written.
    batches: usize,
    bytes: usize,
}

/// The tasks to wake when the stream has a new batch or ends, or when the reader that
/// polls the stream may stop polling it.
#[derive(Default)]
struct Wakers {
    wakers: Mutex<Vec<Waker>>,
    /// Whether the tasks are woken since this is last reset.
    woken: AtomicBool,
}

impl Wakers {
    fn register(&self, waker: &Waker) {
        if let Ok(mut wakers) = self.wakers.lock()
            && !wakers.iter().any(|x| x.will_wake(waker))
        {
            wakers.push(waker.clone());
        }
    }

    fn wake_all(&self) {
        self.wake_all_but(None);
    }

    /// Wakes the tasks, and returns whether a task other than the given one is woken.
    fn wake_all_but(&self, own: Option<&Waker>) -> bool {
        self.woken.store(true, Ordering::SeqCst);
        let wakers = match self.wakers.lock() {
            Ok(mut wakers) => std::mem::take(&mut *wakers),
            Err(_) => return false,
        };
        let mut others = false;
        for waker in wakers {
            others |= own.is_none_or(|own| !waker.will_wake(own));
            waker.wake();
        }
        others
    }
}

impl ArcWake for Wakers {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.wake_all();
    }
}

impl TeeState {
    fn head(&self) -> usize {
        self.base + self.batches.len()
    }

    /// Releases the batches that every reader has read or does not read.
    fn retire(&mut self) {
        if self.keep_all {
            return;
        }
        let mut floor = self.head();
        for reader in &self.readers {
            match reader {
                Reader::Unopened => floor = floor.min(self.base),
                Reader::Reading { position } => floor = floor.min(*position),
                Reader::Finished => {}
            }
        }
        while self.base < floor {
            let Some(kept) = self.batches.pop_front() else {
                break;
            };
            if let Kept::Memory { size, .. } = kept {
                self.reservation.shrink(size);
            }
            self.base += 1;
        }
        if self.batches.is_empty() {
            // No kept batch is in the file that is written.
            self.writing = None;
        }
    }

    /// Whether every reader has finished or is dropped.
    fn is_done(&self) -> bool {
        self.readers.iter().all(|x| matches!(x, Reader::Finished))
    }

    /// Whether a reader other than the given one still reads a new batch.
    fn is_read_by_others(&self, reader: usize) -> bool {
        self.readers
            .iter()
            .enumerate()
            .any(|(i, x)| i != reader && matches!(x, Reader::Unopened | Reader::Reading { .. }))
    }

    /// Stops the stream if every reader has finished, and returns the stream to drop.
    fn stop_if_done(&mut self) -> Option<Source> {
        if self.keep_all || !self.is_done() {
            return None;
        }
        self.batches.clear();
        self.writing = None;
        self.reservation.free();
        Some(std::mem::replace(&mut self.source, Source::Stopped))
    }
}

impl Tee {
    /// Creates a stream with the given schema and number of planned readers, which is
    /// opened when a reader first asks for a batch. A stream that keeps all its batches
    /// can be read again from the beginning by a reader that is created again.
    pub fn new(
        name: String,
        schema: SchemaRef,
        source: TeeSource,
        readers: usize,
        keep_all: bool,
        context: &TaskContext,
        metrics: SpillMetrics,
    ) -> Self {
        let spill = SpillManager::new(context.runtime_env(), metrics, Arc::clone(&schema))
            .with_compression_type(context.session_config().spill_compression());
        let reservation = MemoryConsumer::new(name)
            .with_can_spill(true)
            .register(context.memory_pool());
        let options = context.session_config().options();
        let batch_size = context.session_config().batch_size().max(1);
        let source: TeeSource = Box::new(move || {
            let stream = source()?;
            let schema = stream.schema();
            // A batch larger than the batch size, such as the output of a window over a
            // whole partition, is copied into batches of the batch size. An operator of a
            // reader can keep a slice of the batch, and account for the memory of the whole
            // batch.
            let stream = stream.flat_map(move |batch| match batch {
                Ok(batch) if batch.num_rows() > batch_size => {
                    futures::stream::iter(compact(batch, batch_size)).boxed()
                }
                other => futures::stream::once(async { other }).boxed(),
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as _)
        });
        Self {
            schema,
            state: Mutex::new(TeeState {
                source: Source::Unopened(source),
                base: 0,
                batches: VecDeque::new(),
                readers: vec![Reader::Unopened; readers],
                writing: None,
                reservation,
                keep_all,
            }),
            wakers: Arc::new(Wakers::default()),
            pool_finite: matches!(context.memory_pool().memory_limit(), MemoryLimit::Finite(_)),
            spill_enabled: context.runtime_env().disk_manager.tmp_files_enabled(),
            spill,
            file_size: options.execution.max_spill_file_size_bytes.get(),
            batch_size,
        }
    }

    fn state(&self) -> Result<MutexGuard<'_, TeeState>> {
        self.state
            .lock()
            .map_err(|_| internal_datafusion_err!("shared stream state is poisoned"))
    }

    /// Creates the given planned reader, which reads every batch of the stream. A reader
    /// that is created again reads the stream again from the beginning, if no batch has
    /// been released yet. This happens when a nested loop join executes its build side
    /// again to spill it, after collecting it in memory failed.
    pub fn reader(self: &Arc<Self>, reader: usize) -> Result<TeeReader> {
        let mut state = self.state()?;
        let id = match state.readers.get(reader) {
            Some(Reader::Unopened) => reader,
            Some(_) if state.base == 0 => {
                state.readers.push(Reader::Unopened);
                state.readers.len() - 1
            }
            Some(_) => {
                return internal_err!("a shared stream is read again after its rows are released");
            }
            None => return internal_err!("a shared stream has no reader {reader}"),
        };
        let position = state.base;
        state.readers[id] = Reader::Reading { position };
        Ok(TeeReader {
            tee: Arc::clone(self),
            id,
            position,
            finished: false,
            spilled: None,
            pending: VecDeque::new(),
            yielding: false,
        })
    }

    /// Keeps a new batch for the readers that have not read it.
    fn keep(&self, state: &mut TeeState, batch: RecordBatch) -> Result<()> {
        if !self.pool_finite {
            // An unbounded pool never refuses memory.
            let size = batch_memory_size(&batch);
            state.reservation.grow(size);
            return self.keep_in_memory(state, batch, size);
        }
        // Under a memory limit, the batches never take memory from the pool, which other
        // operators may need.
        if self.spill_enabled {
            // The newest kept batch stays in memory outside the pool: a reader that reads
            // side by side with the reader that produced it reads it there. An older batch
            // that a reader still owes is written to disk when the next batch is kept.
            self.demote_newest(state)?;
            state.batches.push_back(Kept::Memory { batch, size: 0 });
            Ok(())
        } else {
            self.keep_in_memory(state, batch, 0)
        }
    }

    /// Writes the newest kept batch to disk if it is in memory.
    fn demote_newest(&self, state: &mut TeeState) -> Result<()> {
        if !matches!(state.batches.back(), Some(Kept::Memory { .. })) {
            return Ok(());
        }
        let Some(Kept::Memory { batch, .. }) = state.batches.pop_back() else {
            return internal_err!("a shared stream batch is not in memory");
        };
        self.keep_on_disk(state, batch)
    }

    fn keep_in_memory(&self, state: &mut TeeState, batch: RecordBatch, size: usize) -> Result<()> {
        // The batches of a file are consecutive.
        self.finish_writing(state)?;
        state.batches.push_back(Kept::Memory { batch, size });
        Ok(())
    }

    fn keep_on_disk(&self, state: &mut TeeState, batch: RecordBatch) -> Result<()> {
        if state
            .writing
            .as_ref()
            .is_some_and(|x| x.bytes >= self.file_size)
        {
            self.finish_writing(state)?;
        }
        let writing = match &mut state.writing {
            Some(writing) => writing,
            None => {
                let mut file = self.spill.create_in_progress_file("SharedCte")?;
                let writer: SegmentWriter = Box::new(move |batch| match batch {
                    Some(batch) => {
                        file.append_batch(batch)?;
                        Ok(None)
                    }
                    None => file.finish(),
                });
                state.writing.insert(Writing {
                    writer,
                    segment: Arc::new(Segment {
                        file: Mutex::new(None),
                    }),
                    batches: 0,
                    bytes: 0,
                })
            }
        };
        (writing.writer)(Some(&batch))?;
        let index = writing.batches;
        writing.batches += 1;
        writing.bytes += batch_memory_size(&batch);
        let segment = Arc::clone(&writing.segment);
        state.batches.push_back(Kept::Spilled { segment, index });
        Ok(())
    }

    /// Finishes the file that is written, so that it can be read.
    fn finish_writing(&self, state: &mut TeeState) -> Result<()> {
        let Some(mut writing) = state.writing.take() else {
            return Ok(());
        };
        let file = (writing.writer)(None)?
            .ok_or_else(|| internal_datafusion_err!("a shared stream file is empty"))?;
        *writing
            .segment
            .file
            .lock()
            .map_err(|_| internal_datafusion_err!("shared stream file is poisoned"))? = Some(file);
        Ok(())
    }

    /// Opens the file of the given kept batch, which is finished if needed.
    fn open_segment(
        &self,
        state: &mut TeeState,
        segment: &Arc<Segment>,
    ) -> Result<SendableRecordBatchStream> {
        if state
            .writing
            .as_ref()
            .is_some_and(|x| Arc::ptr_eq(&x.segment, segment))
        {
            self.finish_writing(state)?;
        }
        let file = segment
            .file
            .lock()
            .map_err(|_| internal_datafusion_err!("shared stream file is poisoned"))?
            .clone()
            .ok_or_else(|| internal_datafusion_err!("a shared stream file is not finished"))?;
        self.spill.read_spill_as_stream(file, None)
    }

    /// Marks the reader as finished, and stops the stream if every reader has finished.
    fn finish_reader(&self, id: usize) {
        let source = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if let Some(reader) = state.readers.get_mut(id) {
                *reader = Reader::Finished;
            }
            state.retire();
            state.stop_if_done()
        };
        // The stream is dropped outside the lock.
        drop(source);
        // A reader that waits for the stream polls it if this reader was polling it.
        self.wakers.wake_all();
    }

    #[cfg(test)]
    fn kept(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let spilled = state
            .batches
            .iter()
            .filter(|x| matches!(x, Kept::Spilled { .. }))
            .count();
        (state.batches.len(), spilled)
    }

    #[cfg(test)]
    fn is_stopped(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        matches!(state.source, Source::Stopped)
    }
}

/// One of the readers of a [`Tee`].
pub struct TeeReader {
    tee: Arc<Tee>,
    id: usize,
    /// The position in the stream of the next batch to read.
    position: usize,
    finished: bool,
    /// The file that the reader reads, and the index of the next batch in it.
    spilled: Option<(Arc<Segment>, SendableRecordBatchStream, usize)>,
    /// The parts of a batch that is read from disk.
    pending: VecDeque<RecordBatch>,
    /// Whether the reader yields before it polls again, since it woke other readers.
    yielding: bool,
}

/// What a reader does next.
enum Step {
    Return(Poll<Option<Result<RecordBatch>>>),
    Continue,
}

impl TeeReader {
    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.spilled = None;
            self.tee.finish_reader(self.id);
        }
    }

    /// Moves to the next batch.
    fn advance(&mut self, state: &mut TeeState) {
        self.position += 1;
        if let Some(reader) = state.readers.get_mut(self.id) {
            *reader = Reader::Reading {
                position: self.position,
            };
        }
        state.retire();
    }

    fn fail(&mut self, error: DataFusionError) -> Poll<Option<Result<RecordBatch>>> {
        self.finish();
        Poll::Ready(Some(Err(error)))
    }

    /// Reads the batch at the given index of the file that the reader reads.
    fn read_spilled(&mut self, cx: &mut Context<'_>, index: usize) -> Result<Step> {
        let Some((_, stream, next)) = &mut self.spilled else {
            return internal_err!("a shared stream file is not open");
        };
        loop {
            match stream.poll_next_unpin(cx) {
                Poll::Pending => return Ok(Step::Return(Poll::Pending)),
                Poll::Ready(Some(Ok(batch))) => {
                    *next += 1;
                    // The batches before the index are read by the reader from the stream.
                    if *next <= index {
                        continue;
                    }
                    for batch in compact(batch, self.tee.batch_size) {
                        self.pending.push_back(batch?);
                    }
                    let tee = Arc::clone(&self.tee);
                    let mut state = tee.state()?;
                    self.advance(&mut state);
                    return Ok(Step::Continue);
                }
                Poll::Ready(Some(Err(error))) => return Err(error),
                Poll::Ready(None) => return internal_err!("a shared stream file ends early"),
            }
        }
    }

    fn step(&mut self, cx: &mut Context<'_>) -> Result<Step> {
        if let Some(batch) = self.pending.pop_front() {
            return Ok(Step::Return(Poll::Ready(Some(Ok(batch)))));
        }
        let tee = Arc::clone(&self.tee);
        let mut state = tee.state()?;
        if self.position < state.base {
            return internal_err!("a shared stream batch is released before it is read");
        }
        match state.batches.get(self.position - state.base) {
            Some(Kept::Memory { batch, .. }) => {
                let batch = batch.clone();
                self.advance(&mut state);
                return Ok(Step::Return(Poll::Ready(Some(Ok(batch)))));
            }
            Some(Kept::Spilled { segment, index, .. }) => {
                let (segment, index) = (Arc::clone(segment), *index);
                let open = self
                    .spilled
                    .as_ref()
                    .is_some_and(|(s, _, next)| Arc::ptr_eq(s, &segment) && *next <= index);
                if !open {
                    let stream = tee.open_segment(&mut state, &segment)?;
                    self.spilled = Some((segment, stream, 0));
                }
                drop(state);
                return self.read_spilled(cx, index);
            }
            None => {}
        }
        // The reader is at the head, and asks for a new batch.
        self.spilled = None;
        let mut stream = match std::mem::replace(&mut state.source, Source::Polling) {
            Source::Ready(stream) => stream,
            Source::Unopened(open) => match open() {
                Ok(stream) => stream,
                Err(error) => {
                    state.source = Source::Failed(Arc::new(error));
                    drop(state);
                    tee.wakers.wake_all();
                    return Ok(Step::Continue);
                }
            },
            Source::Polling => {
                // Another reader is polling the stream, and wakes this reader after that.
                tee.wakers.register(cx.waker());
                return Ok(Step::Return(Poll::Pending));
            }
            Source::Finished => {
                state.source = Source::Finished;
                drop(state);
                self.finish();
                return Ok(Step::Return(Poll::Ready(None)));
            }
            Source::Failed(error) => {
                state.source = Source::Failed(Arc::clone(&error));
                drop(state);
                return Ok(Step::Return(self.fail(DataFusionError::Shared(error))));
            }
            Source::Stopped => {
                state.source = Source::Stopped;
                return internal_err!("a shared stream is read after it is stopped");
            }
        };
        drop(state);
        // Any reader that waits for the stream is woken when the stream is ready, so that
        // it polls the stream if this reader does not poll it again.
        tee.wakers.register(cx.waker());
        tee.wakers.woken.store(false, Ordering::SeqCst);
        let waker = futures::task::waker(Arc::clone(&tee.wakers));
        let mut polling = PollingGuard {
            tee: &tee,
            armed: true,
        };
        let polled = stream.poll_next_unpin(&mut Context::from_waker(&waker));
        polling.armed = false;
        let mut state = tee.state()?;
        match polled {
            Poll::Pending => {
                state.source = Source::Ready(stream);
                drop(state);
                // A reader that starts to wait after the stream wakes it while it is
                // polled would not be woken by the stream again.
                if tee.wakers.woken.load(Ordering::SeqCst) {
                    tee.wakers.wake_all();
                }
                Ok(Step::Return(Poll::Pending))
            }
            Poll::Ready(Some(Ok(batch))) => {
                state.source = Source::Ready(stream);
                if batch.num_rows() == 0 {
                    return Ok(Step::Continue);
                }
                if state.keep_all || state.is_read_by_others(self.id) {
                    if let Err(error) = tee.keep(&mut state, batch.clone()) {
                        let error = Arc::new(error);
                        state.source = Source::Failed(Arc::clone(&error));
                        drop(state);
                        tee.wakers.wake_all();
                        return Ok(Step::Return(self.fail(DataFusionError::Shared(error))));
                    }
                } else {
                    // No other reader reads the batch.
                    state.base += 1;
                }
                self.advance(&mut state);
                let source = state.stop_if_done();
                drop(state);
                drop(source);
                // A task that wakes another task runs on before it, so the reader yields
                // once to let the woken readers run, as they would for a separate producer.
                self.yielding = tee.wakers.wake_all_but(Some(cx.waker()));
                Ok(Step::Return(Poll::Ready(Some(Ok(batch)))))
            }
            Poll::Ready(Some(Err(error))) => {
                state.source = Source::Failed(Arc::new(error));
                drop(state);
                drop(stream);
                tee.wakers.wake_all();
                Ok(Step::Continue)
            }
            Poll::Ready(None) => {
                state.source = Source::Finished;
                drop(state);
                drop(stream);
                tee.wakers.wake_all();
                Ok(Step::Continue)
            }
        }
    }
}

/// Fails the stream if polling it panics, so that the other readers do not wait for it.
struct PollingGuard<'a> {
    tee: &'a Tee,
    armed: bool,
}

impl Drop for PollingGuard<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Ok(mut state) = self.tee.state.lock()
        {
            state.source = Source::Failed(Arc::new(internal_datafusion_err!(
                "a shared stream panicked"
            )));
            drop(state);
            self.tee.wakers.wake_all();
        }
    }
}

impl Stream for TeeReader {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished && self.pending.is_empty() {
            return Poll::Ready(None);
        }
        if self.yielding {
            self.yielding = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        loop {
            match self.step(cx) {
                Ok(Step::Return(poll)) => return poll,
                Ok(Step::Continue) => continue,
                Err(error) => return self.fail(error),
            }
        }
    }
}

impl RecordBatchStream for TeeReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.tee.schema)
    }
}

impl Drop for TeeReader {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Returns the size of the values of a batch, whose arrays can be slices of larger arrays.
fn batch_memory_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| slice_size(&column.to_data()))
        .sum()
}

/// Returns the size of the values of an array, which can be a slice of a larger array, so
/// that the slices of an array add up to the size of the array rather than each counting
/// the whole array.
fn slice_size(data: &ArrayData) -> usize {
    let len = data.len();
    let nulls = data.nulls().map_or(0, |_| len.div_ceil(8));
    let child = |start: usize, length: usize| {
        data.child_data()
            .first()
            .map_or(0, |child| slice_size(&child.slice(start, length)))
    };
    match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let offsets = data.buffer::<i32>(0);
            let (start, end) = (offsets[0] as usize, offsets[len] as usize);
            nulls + (len + 1) * 4 + child(start, end - start)
        }
        DataType::LargeList(_) => {
            let offsets = data.buffer::<i64>(0);
            let (start, end) = (offsets[0] as usize, offsets[len] as usize);
            nulls + (len + 1) * 8 + child(start, end - start)
        }
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            nulls + child(data.offset() * size, len * size)
        }
        DataType::Utf8View | DataType::BinaryView => {
            // A view refers to bytes in a data buffer when its value is not inlined.
            let referenced = data.buffer::<u128>(0)[..len]
                .iter()
                .map(|view| *view as u32 as usize)
                .filter(|&length| length > 12)
                .sum::<usize>();
            nulls + len * 16 + referenced
        }
        DataType::Struct(_) => {
            // The children of a slice of a struct are slices themselves.
            nulls + data.child_data().iter().map(slice_size).sum::<usize>()
        }
        DataType::Dictionary(key, _) => {
            // The values are shared by the slices of the array.
            let values = data.child_data().first().map_or(0, slice_size);
            nulls + len * key.primitive_width().unwrap_or(0) + values
        }
        _ => data
            .get_slice_memory_size()
            .unwrap_or_else(|_| data.get_array_memory_size()),
    }
}

/// Copies a batch into new batches of at most the given size, which are made when they
/// are read. An operator that receives a batch larger than the size can keep slices of
/// it, and the arrays of a batch that is read from disk share the memory of the whole
/// file block, so that the operators would account for more memory than they keep.
fn compact(
    batch: RecordBatch,
    batch_size: usize,
) -> impl Iterator<Item = Result<RecordBatch>> + Send + 'static {
    let rows = batch.num_rows();
    (0..rows).step_by(batch_size).map(move |offset| {
        let length = batch_size.min(rows - offset);
        let indices = UInt64Array::from_iter_values(offset as u64..(offset + length) as u64);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                // An array of a type that cannot be copied keeps the memory of the batch.
                let column =
                    take(column, &indices, None).unwrap_or_else(|_| column.slice(offset, length));
                match column.data_type() {
                    DataType::Utf8View => Arc::new(column.as_string_view().gc()) as ArrayRef,
                    DataType::BinaryView => Arc::new(column.as_binary_view().gc()),
                    _ => column,
                }
            })
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(length));
        Ok(RecordBatch::try_new_with_options(
            batch.schema(),
            columns,
            &options,
        )?)
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_common::exec_datafusion_err;
    use futures::TryStreamExt;

    use super::*;

    const ROWS: usize = 1024;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn batch(i: usize) -> RecordBatch {
        let start = (i * ROWS) as i64;
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from_iter_values(
                start..start + ROWS as i64,
            ))],
        )
        .unwrap()
    }

    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn expected(batches: usize) -> Vec<i64> {
        (0..(batches * ROWS) as i64).collect()
    }

    /// What a test source does, and what happens to it.
    #[derive(Default)]
    struct Probe {
        /// The number of batches that the source has produced.
        pulled: AtomicUsize,
        opened: AtomicBool,
        dropped: AtomicBool,
    }

    struct ProbeGuard(Arc<Probe>);

    impl Drop for ProbeGuard {
        fn drop(&mut self) {
            self.0.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// A source of the given number of batches, which fails after `fail_after` batches
    /// or stays pending after `pend_after` batches, and yields between batches.
    fn source(
        batches: usize,
        fail_after: Option<usize>,
        pend_after: Option<usize>,
        probe: &Arc<Probe>,
    ) -> TeeSource {
        let probe = Arc::clone(probe);
        Box::new(move || {
            probe.opened.store(true, Ordering::SeqCst);
            let guard = ProbeGuard(Arc::clone(&probe));
            let stream = futures::stream::unfold(0usize, move |i| {
                let probe = Arc::clone(&guard.0);
                async move {
                    tokio::task::yield_now().await;
                    if Some(i) == pend_after {
                        futures::future::pending::<()>().await;
                    }
                    if Some(i) == fail_after {
                        return Some((Err(exec_datafusion_err!("body failed")), i + 1));
                    }
                    if i >= batches || fail_after.is_some_and(|f| i > f) {
                        return None;
                    }
                    probe.pulled.fetch_add(1, Ordering::SeqCst);
                    Some((Ok(batch(i)), i + 1))
                }
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema(), stream))
                as SendableRecordBatchStream)
        })
    }

    fn context(memory_limit: Option<usize>, disk: bool) -> TaskContext {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = memory_limit {
            runtime = runtime.with_memory_limit(limit, 1.0);
        }
        if !disk {
            runtime = runtime.with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            );
        }
        TaskContext::default().with_runtime(runtime.build_arc().unwrap())
    }

    fn tee(source: TeeSource, readers: usize, context: &TaskContext) -> (Arc<Tee>, SpillMetrics) {
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let tee = Arc::new(Tee::new(
            "test".to_string(),
            schema(),
            source,
            readers,
            false,
            context,
            metrics.clone(),
        ));
        (tee, metrics)
    }

    async fn next(reader: &mut TeeReader) -> Option<RecordBatch> {
        reader.next().await.map(|x| x.unwrap())
    }

    #[tokio::test]
    async fn nothing_is_produced_before_a_reader_asks() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(4, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        let b = tee.reader(1).unwrap();
        assert!(!probe.opened.load(Ordering::SeqCst));
        drop((a, b));
        assert!(!probe.opened.load(Ordering::SeqCst));
        assert!(tee.is_stopped());
    }

    #[tokio::test]
    async fn readers_side_by_side_keep_only_their_lag() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(16, None, None, &probe), 2, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        let (mut x, mut y) = (vec![], vec![]);
        for _ in 0..16 {
            x.push(next(&mut a).await.unwrap());
            assert_eq!(tee.kept().0, 1);
            y.push(next(&mut b).await.unwrap());
            assert_eq!(tee.kept().0, 0);
        }
        assert!(next(&mut a).await.is_none());
        assert!(next(&mut b).await.is_none());
        assert_eq!(values(&x), expected(16));
        assert_eq!(values(&y), expected(16));
        assert_eq!(probe.pulled.load(Ordering::SeqCst), 16);
    }

    #[tokio::test]
    async fn a_late_reader_reads_every_row() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, metrics) = tee(source(8, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        let x = a.try_collect::<Vec<_>>().await.unwrap();
        // The second reader is not created yet, so every batch is kept for it.
        assert_eq!(tee.kept(), (8, 0));
        let b = tee.reader(1).unwrap();
        let y = b.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&x), expected(8));
        assert_eq!(values(&y), expected(8));
        assert_eq!(tee.kept().0, 0);
        assert_eq!(probe.pulled.load(Ordering::SeqCst), 8);
        assert_eq!(metrics.spill_file_count.value(), 0);
    }

    #[tokio::test]
    async fn readers_that_stop_early_stop_production() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(1000, None, None, &probe), 3, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        // The third reader is created but never read.
        let c = tee.reader(2).unwrap();
        for _ in 0..2 {
            next(&mut a).await.unwrap();
        }
        next(&mut b).await.unwrap();
        drop(a);
        drop(b);
        assert_eq!(probe.pulled.load(Ordering::SeqCst), 2);
        assert!(!probe.dropped.load(Ordering::SeqCst));
        // The source is dropped once every reader is dropped.
        drop(c);
        assert!(probe.dropped.load(Ordering::SeqCst));
        assert!(tee.is_stopped());
        assert_eq!(tee.kept().0, 0);
    }

    #[tokio::test]
    async fn a_reader_gets_rows_before_the_source_ends() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(10, None, Some(1), &probe), 2, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        assert_eq!(values(&[next(&mut a).await.unwrap()]), expected(1));
        assert_eq!(values(&[next(&mut b).await.unwrap()]), expected(1));
        // The source stays pending after the first batch.
        let pending = tokio::time::timeout(Duration::from_millis(50), a.next()).await;
        assert!(pending.is_err());
    }

    #[tokio::test]
    async fn an_error_reaches_only_the_readers_that_read_up_to_it() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(10, Some(2), None, &probe), 3, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        let c = tee.reader(2).unwrap();
        next(&mut a).await.unwrap();
        next(&mut a).await.unwrap();
        assert!(a.next().await.unwrap().is_err());
        assert!(a.next().await.is_none());
        // The second reader stops before the error, and does not see it.
        next(&mut b).await.unwrap();
        drop(b);
        let result = c.try_collect::<Vec<_>>().await;
        assert!(result.unwrap_err().to_string().contains("body failed"));
    }

    #[tokio::test]
    async fn a_reader_reads_again_until_rows_are_released() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, _) = tee(source(4, None, None, &probe), 2, &context);
        assert!(tee.reader(2).is_err());
        let mut a = tee.reader(0).unwrap();
        let b = tee.reader(1).unwrap();
        next(&mut a).await.unwrap();
        // No row is released yet, since the second reader reads from the beginning.
        let again = tee.reader(0).unwrap();
        let x = again.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&x), expected(4));
        drop(b);
        let rest = a.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&rest), expected(4)[ROWS..].to_vec());
        assert!(tee.reader(0).is_err());
        assert_eq!(probe.pulled.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn a_stream_that_keeps_all_batches_is_read_again_from_the_beginning() {
        for keep_all in [true, false] {
            let probe = Arc::new(Probe::default());
            let context = context(Some(1024 * 1024), true);
            let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            let tee = Arc::new(Tee::new(
                "test".to_string(),
                schema(),
                source(4, None, None, &probe),
                2,
                keep_all,
                &context,
                metrics,
            ));
            let mut a = tee.reader(0).unwrap();
            next(&mut a).await.unwrap();
            next(&mut a).await.unwrap();
            drop(a);
            let b = tee.reader(1).unwrap();
            let y = b.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(values(&y), expected(4));
            if keep_all {
                assert_eq!(tee.kept(), (4, 3));
            }
            // The first reader is created again, as a nested loop join executes its build
            // side again.
            match tee.reader(0) {
                Ok(again) => {
                    assert!(keep_all);
                    let x = again.try_collect::<Vec<_>>().await.unwrap();
                    assert_eq!(values(&x), expected(4));
                }
                Err(_) => assert!(!keep_all),
            }
            assert_eq!(probe.pulled.load(Ordering::SeqCst), 4);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_read_every_row_in_order() {
        for round in 0..50 {
            let context = context(None, true);
            let tees = (0..4)
                .map(|_| {
                    let probe = Arc::new(Probe::default());
                    let (tee, _) = tee(source(64, None, None, &probe), 3, &context);
                    (tee, probe)
                })
                .collect::<Vec<_>>();
            let mut tasks = vec![];
            for (tee, _) in &tees {
                for i in 0..3 {
                    let reader = tee.reader(i).unwrap();
                    tasks.push(tokio::spawn(async move {
                        reader.try_collect::<Vec<_>>().await.unwrap()
                    }));
                }
            }
            let results =
                tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(tasks))
                    .await;
            assert!(results.is_ok(), "readers did not finish in round {round}");
            let results = results.unwrap();
            for result in results {
                assert_eq!(values(&result.unwrap()), expected(64));
            }
            for (tee, probe) in &tees {
                assert_eq!(probe.pulled.load(Ordering::SeqCst), 64);
                assert_eq!(tee.kept().0, 0);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fast_reader_does_not_wait_for_a_slow_reader() {
        for blocking in [false, true] {
            let probe = Arc::new(Probe::default());
            let context = context(None, true);
            let (tee, _) = tee(source(200, None, None, &probe), 2, &context);
            let fast = tee.reader(0).unwrap();
            let mut slow = tee.reader(1).unwrap();
            let slow = tokio::spawn(async move {
                let mut rows = 0;
                while let Some(batch) = slow.next().await {
                    rows += batch.unwrap().num_rows();
                    // The consumer of the slow reader takes 10 ms per batch.
                    if blocking {
                        std::thread::sleep(Duration::from_millis(10));
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                rows
            });
            let start = std::time::Instant::now();
            let fast = tokio::spawn(async move { fast.try_collect::<Vec<_>>().await.unwrap() });
            let rows = fast
                .await
                .unwrap()
                .iter()
                .map(|x| x.num_rows())
                .sum::<usize>();
            let elapsed = start.elapsed();
            assert_eq!(rows, 200 * ROWS);
            // The slow reader needs 2 s; the fast one finishes long before.
            assert!(
                elapsed < Duration::from_millis(500),
                "{blocking}: {elapsed:?}"
            );
            assert!(!slow.is_finished());
            assert_eq!(slow.await.unwrap(), 200 * ROWS);
        }
    }

    /// A source of the given number of batches that never yields, as a CPU-bound body.
    fn busy_source(batches: usize) -> TeeSource {
        Box::new(move || {
            let stream = futures::stream::iter((0..batches).map(|i| Ok(batch(i))));
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema(), stream)) as _)
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_readers_with_slow_readers_that_block_their_threads() {
        // Two partitions, each read by a fast reader and by a slow reader whose consumer
        // blocks its thread for 2 ms per batch, as a Python UDF does. The slow readers
        // occupy two of the four threads, so that the fast readers can run, as they can in
        // a runtime with more threads than blocking consumers.
        let context = context(None, true);
        let start = std::time::Instant::now();
        let mut fast = vec![];
        let mut slow = vec![];
        for _ in 0..2 {
            let probe = Arc::new(Probe::default());
            let _ = probe;
            let (tee, _) = tee(busy_source(400), 2, &context);
            let mut reader = tee.reader(1).unwrap();
            slow.push(tokio::spawn(async move {
                while let Some(batch) = reader.next().await {
                    batch.unwrap();
                    std::thread::sleep(Duration::from_millis(2));
                }
                start.elapsed()
            }));
            let reader = tee.reader(0).unwrap();
            fast.push(tokio::spawn(async move {
                reader.try_collect::<Vec<_>>().await.unwrap();
                start.elapsed()
            }));
        }
        let fast = futures::future::join_all(fast).await;
        let slow = futures::future::join_all(slow).await;
        let fast = fast.into_iter().map(|x| x.unwrap()).max().unwrap();
        let slow = slow.into_iter().map(|x| x.unwrap()).max().unwrap();
        assert!(
            fast * 2 < slow,
            "fast readers {fast:?}, slow readers {slow:?}"
        );
    }

    #[tokio::test]
    async fn kept_batches_are_written_to_disk_under_a_bounded_pool() {
        const LIMIT: usize = 1024 * 1024;
        let probe = Arc::new(Probe::default());
        // The pool has room for the batches, which are written to disk all the same, so
        // that they take no memory that other operators may need.
        let context = context(Some(LIMIT), true);
        let (tee, metrics) = tee(source(32, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        let x = a.try_collect::<Vec<_>>().await.unwrap();
        // The newest batch stays in memory outside the pool.
        assert_eq!(tee.kept(), (32, 31));
        assert_eq!(context.memory_pool().reserved(), 0);
        let other = MemoryConsumer::new("other").register(context.memory_pool());
        other.try_grow(LIMIT).unwrap();
        other.free();
        // The batches are written to one file.
        assert_eq!(metrics.spill_file_count.value(), 1);
        let b = tee.reader(1).unwrap();
        let y = b.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&x), expected(32));
        assert_eq!(values(&y), expected(32));
        assert_eq!(tee.kept().0, 0);
        assert_eq!(context.memory_pool().reserved(), 0);
    }

    #[tokio::test]
    async fn readers_side_by_side_write_nothing_to_disk_under_a_bounded_pool() {
        let probe = Arc::new(Probe::default());
        let context = context(Some(1024 * 1024), true);
        let (tee, metrics) = tee(source(32, None, None, &probe), 2, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        let (mut x, mut y) = (vec![], vec![]);
        for _ in 0..32 {
            x.push(next(&mut a).await.unwrap());
            assert_eq!(tee.kept(), (1, 0));
            y.push(next(&mut b).await.unwrap());
        }
        assert!(next(&mut a).await.is_none());
        assert!(next(&mut b).await.is_none());
        assert_eq!(values(&x), expected(32));
        assert_eq!(values(&y), expected(32));
        assert_eq!(metrics.spill_file_count.value(), 0);
        assert_eq!(context.memory_pool().reserved(), 0);
    }

    #[tokio::test]
    async fn only_older_batches_that_a_reader_owes_are_written_to_disk() {
        let probe = Arc::new(Probe::default());
        let context = context(Some(1024 * 1024), true);
        let (tee, metrics) = tee(source(32, None, None, &probe), 2, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        let (mut x, mut y) = (vec![], vec![]);
        for _ in 0..8 {
            x.push(next(&mut a).await.unwrap());
        }
        // The second reader catches up: the batches before the newest one are on disk.
        for k in (1..=8).rev() {
            assert_eq!(tee.kept(), (k, k - 1));
            y.push(next(&mut b).await.unwrap());
        }
        for _ in 8..32 {
            x.push(next(&mut a).await.unwrap());
            y.push(next(&mut b).await.unwrap());
        }
        assert!(next(&mut a).await.is_none());
        assert!(next(&mut b).await.is_none());
        assert_eq!(values(&x), expected(32));
        assert_eq!(values(&y), expected(32));
        assert_eq!(metrics.spilled_rows.value(), 7 * ROWS);
        assert_eq!(metrics.spill_file_count.value(), 1);
        assert_eq!(context.memory_pool().reserved(), 0);
    }

    #[tokio::test]
    async fn readers_read_files_while_they_are_written() {
        let probe = Arc::new(Probe::default());
        let context = context(Some(1), true);
        let (tee, metrics) = tee(source(16, None, None, &probe), 3, &context);
        let mut a = tee.reader(0).unwrap();
        let mut b = tee.reader(1).unwrap();
        let mut c = tee.reader(2).unwrap();
        let (mut x, mut y, mut z) = (vec![], vec![], vec![]);
        for i in 0..16 {
            // Each reader polls the stream in turn, and the others read from disk.
            x.push(next(&mut a).await.unwrap());
            if i % 2 == 0 {
                y.push(next(&mut b).await.unwrap());
            }
            if i % 4 == 3 {
                while z.len() < x.len() {
                    z.push(next(&mut c).await.unwrap());
                }
            }
        }
        while let Some(batch) = next(&mut b).await {
            y.push(batch);
        }
        while let Some(batch) = next(&mut c).await {
            z.push(batch);
        }
        assert_eq!(values(&x), expected(16));
        assert_eq!(values(&y), expected(16));
        assert_eq!(values(&z), expected(16));
        assert!(metrics.spill_file_count.value() > 1);
        assert_eq!(probe.pulled.load(Ordering::SeqCst), 16);
    }

    #[tokio::test]
    async fn kept_batches_stay_in_memory_without_temporary_files() {
        const LIMIT: usize = 1024 * 1024;
        let probe = Arc::new(Probe::default());
        let context = context(Some(LIMIT), false);
        // The batches are four times the size of the pool.
        let batches = 4 * LIMIT / (ROWS * 8);
        let (tee, metrics) = tee(source(batches, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        let x = a.try_collect::<Vec<_>>().await.unwrap();
        // The batches are kept outside the pool, which does not fail the stream or another
        // operator.
        assert_eq!(tee.kept(), (batches, 0));
        assert!(context.memory_pool().reserved() <= LIMIT);
        let other = MemoryConsumer::new("other").register(context.memory_pool());
        other.try_grow(LIMIT / 2).unwrap();
        other.free();
        let b = tee.reader(1).unwrap();
        let y = b.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&x), expected(batches));
        assert_eq!(values(&y), expected(batches));
        assert_eq!(metrics.spill_file_count.value(), 0);
        assert_eq!(context.memory_pool().reserved(), 0);
    }

    #[tokio::test]
    async fn kept_batches_stay_in_memory_under_an_unbounded_pool() {
        let probe = Arc::new(Probe::default());
        let context = context(None, true);
        let (tee, metrics) = tee(source(256, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        a.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(tee.kept(), (256, 0));
        let b = tee.reader(1).unwrap();
        let y = b.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&y), expected(256));
        assert_eq!(metrics.spill_file_count.value(), 0);
    }

    #[test]
    fn slices_of_an_array_add_up_to_its_size() {
        use datafusion::arrow::array::{
            DictionaryArray, ListArray, StringArray, StringViewArray, StructArray,
        };
        use datafusion::arrow::datatypes::{Int32Type, Int64Type};

        let rows = 1 << 16;
        let total = |array: ArrayRef| -> (usize, usize) {
            let whole = slice_size(&array.to_data());
            // An aggregate returns its output as slices of one large array.
            let slices = (0..rows)
                .step_by(512)
                .map(|offset| slice_size(&array.slice(offset, 512).to_data()))
                .sum::<usize>();
            (whole, slices)
        };
        let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
        let strings = (0..rows).map(|x| format!("{x:032}")).collect::<Vec<_>>();
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..rows).map(|x| Some(vec![Some(x as i64); 4])),
        ));
        let structs: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int64, false)),
            Arc::clone(&integers),
        )]));
        for array in [
            Arc::clone(&integers),
            Arc::new(StringArray::from_iter_values(&strings)) as ArrayRef,
            Arc::new(StringViewArray::from_iter_values(&strings)),
            lists,
            structs,
        ] {
            let (whole, slices) = total(array);
            assert!(whole >= rows * 8, "{whole}");
            assert!(slices <= whole + (rows / 512) * 8, "{slices} > {whole}");
            assert!(slices >= whole / 2, "{slices} < {whole}");
        }
        // A dictionary is counted once per slice, which is small when the values are few.
        let keys = (0..rows).map(|x| (x % 4) as i32).collect::<Vec<_>>();
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                keys.into(),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            )
            .unwrap(),
        );
        let (whole, slices) = total(dictionary);
        assert!(slices <= whole + (rows / 512) * 64);
    }

    #[tokio::test]
    async fn batches_read_from_disk_are_compacted() {
        use datafusion::arrow::array::{StringArray, StringViewArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8View, false),
        ]));
        let strings = (0..8000).map(|x| format!("{x:064}")).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..8000)),
                Arc::new(StringArray::from_iter_values(&strings)),
                Arc::new(StringViewArray::from_iter_values(&strings)),
            ],
        )
        .unwrap();
        let source_schema = Arc::clone(&schema);
        let source: TeeSource = Box::new(move || {
            let stream = futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(source_schema, stream)) as _)
        });
        // Under a memory limit, the first batch is written to disk once the second one is
        // kept.
        let context = context(Some(1), true);
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let tee = Arc::new(Tee::new(
            "test".to_string(),
            schema,
            source,
            2,
            false,
            &context,
            metrics,
        ));
        let a = tee
            .reader(0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        // The reader that polls the source gets the batch itself.
        assert_eq!(
            a.iter().map(|x| x.num_rows()).collect::<Vec<_>>(),
            [8000, 8000]
        );
        let b = tee
            .reader(1)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            b.iter().map(|x| x.num_rows()).collect::<Vec<_>>(),
            [8000, 8000]
        );
        // Each array holds the memory of its own values, and not that of the file block
        // that it is read from.
        for batch in &b[..1] {
            let rows = batch.num_rows();
            assert!(batch.column(0).get_array_memory_size() < rows * 8 + 1024);
            assert!(batch.column(1).get_array_memory_size() < rows * 72 + 1024);
            assert!(batch.column(2).get_array_memory_size() < rows * 80 + 1024);
        }
    }

    #[tokio::test]
    async fn a_failure_to_write_to_disk_is_an_error() {
        let probe = Arc::new(Probe::default());
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(ROWS * 8, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(1),
            );
        let context = TaskContext::default().with_runtime(runtime.build_arc().unwrap());
        let (tee, _) = tee(source(8, None, None, &probe), 2, &context);
        let a = tee.reader(0).unwrap();
        assert!(a.try_collect::<Vec<_>>().await.is_err());
        let b = tee.reader(1).unwrap();
        assert!(b.try_collect::<Vec<_>>().await.is_err());
    }
}
