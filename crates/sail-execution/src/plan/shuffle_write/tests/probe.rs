// SPDX-License-Identifier: Apache-2.0

//! Measures the heap memory that a shuffle write holds while it runs.
//!
//! What the writer buffers cannot be seen from outside, so these tests count the bytes
//! that the test thread has allocated and not freed. `#[tokio::test]` runs the whole
//! write on the test thread, and the other tests of the binary run on their own threads,
//! so the count is exact however many tests run at once. Memory that a write allocates on
//! another thread is not counted.
//!
//! The `#[global_allocator]` of this module is the allocator of every unit test of this
//! crate, and it counts only inside a probe.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;

use datafusion::arrow::array::{ArrayData, AsArray};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{PhysicalSortExpr, RangePartitioning, SplitPoint};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};

use super::super::{ShufflePartitioning, ShuffleWriteExec};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState, TaskStreamWriter};

/// How many times the bytes of the input pulled so far a write may hold.
const INPUT_FACTOR: usize = 4;

/// What a write may hold on top of that, whatever its input, channels, and batch size are.
const TASK_BYTES: usize = 1024 * 1024;

/// What an output batch may hold on top of its rows, for itself and for each of its
/// arrays. Every batch has buffers and headers of its own however few rows it has, so
/// this is the cost of writing many small batches rather than of buffering rows.
const OUTPUT_BATCH_BYTES: usize = 512;
const OUTPUT_ARRAY_BYTES: usize = 1024;

struct Counters {
    tracking: Cell<bool>,
    live: Cell<isize>,
    peak: Cell<isize>,
}

thread_local! {
    static COUNTERS: Counters = const {
        Counters {
            tracking: Cell::new(false),
            live: Cell::new(0),
            peak: Cell::new(0),
        }
    };
}

fn count(delta: isize) {
    // The counters have no destructor, so they can be used while a thread shuts down.
    let _ = COUNTERS.try_with(|counters| {
        if counters.tracking.get() {
            let live = counters.live.get() + delta;
            counters.live.set(live);
            if live > counters.peak.get() {
                counters.peak.set(live);
            }
        }
    });
}

/// The system allocator, counting the bytes of the threads that have a [`HeapProbe`].
struct ThreadHeap;

// SAFETY: Every call is passed on to the system allocator as it is.
unsafe impl GlobalAlloc for ThreadHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds the contract of `GlobalAlloc::alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            count(layout.size() as isize);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds the contract of `GlobalAlloc::alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            count(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller upholds the contract of `GlobalAlloc::dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller upholds the contract of `GlobalAlloc::realloc`.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            count(size as isize - layout.size() as isize);
        }
        pointer
    }
}

#[global_allocator]
static HEAP: ThreadHeap = ThreadHeap;

/// Counts the heap memory of the current thread from zero until it is dropped.
struct HeapProbe(());

impl HeapProbe {
    fn start() -> Self {
        COUNTERS.with(|counters| {
            counters.live.set(0);
            counters.peak.set(0);
            counters.tracking.set(true);
        });
        Self(())
    }
}

impl Drop for HeapProbe {
    fn drop(&mut self) {
        COUNTERS.with(|counters| counters.tracking.set(false));
    }
}

/// Returns the bytes that the current thread has allocated and not freed since its probe started.
fn heap_held() -> usize {
    COUNTERS.with(|counters| counters.live.get().max(0) as usize)
}

/// Returns the most bytes held since the probe started or since the last call.
fn heap_take_peak() -> usize {
    COUNTERS.with(|counters| {
        let peak = counters.peak.get().max(0) as usize;
        counters.peak.set(counters.live.get());
        peak
    })
}

/// Runs the bookkeeping of a test without counting its memory.
///
/// Whatever `f` allocates must not be freed while the probe counts.
fn untracked<T>(f: impl FnOnce() -> T) -> T {
    let tracking = COUNTERS.with(|counters| counters.tracking.replace(false));
    let value = f();
    COUNTERS.with(|counters| counters.tracking.set(tracking));
    value
}

/// The heap held at one point of a write, and the input pulled and output written by then.
#[derive(Debug, Clone, Copy)]
pub(super) struct Sample {
    /// Whether this was taken as the sink received a write, rather than as the write
    /// asked for its next input batch.
    pub(super) at_write: bool,
    pub(super) input_batches: usize,
    pub(super) input_rows: usize,
    /// The memory of the input batches pulled so far.
    pub(super) input_bytes: usize,
    pub(super) written_rows: usize,
    pub(super) held: usize,
    /// The most held since the previous sample.
    pub(super) peak: usize,
}

/// What a shuffle write did, as its input and its sink saw it.
#[derive(Debug, Default)]
pub(super) struct Probe {
    /// The batches written to each channel, if they were retained.
    pub(super) channels: Vec<Vec<RecordBatch>>,
    pub(super) writes: usize,
    pub(super) committed: bool,
    pub(super) aborted: bool,
    /// How often the write asked for input after a write reported every channel closed.
    pub(super) pulled_after_close: usize,
    closed: bool,
    pub(super) input_batches: usize,
    pub(super) input_rows: usize,
    pub(super) input_bytes: usize,
    pub(super) written_rows: usize,
    pub(super) written_batches: usize,
    /// One sample before each input batch is pulled, one at the end of the input, one at
    /// each write, and one when the write is over.
    pub(super) samples: Vec<Sample>,
    /// The heap held once the write is over, which is the retained output.
    pub(super) held_at_end: usize,
}

impl Probe {
    fn sample(&mut self, at_write: bool, held: usize, peak: usize) {
        self.samples.push(Sample {
            at_write,
            input_batches: self.input_batches,
            input_rows: self.input_rows,
            input_bytes: self.input_bytes,
            written_rows: self.written_rows,
            held,
            peak,
        });
    }

    /// Returns the most heap held as the write asked for an input batch.
    pub(super) fn max_held(&self) -> usize {
        self.samples
            .iter()
            .filter(|sample| !sample.at_write)
            .map(|sample| sample.held)
            .max()
            .unwrap_or(0)
    }

    /// Describes where a write that discarded its output held more heap than the input
    /// pulled by then accounts for, giving the worst case of each kind.
    ///
    /// As it pulls input, the write holds only what it buffers, which is limited by the
    /// input alone. While it partitions or writes a batch, each channel can also have one
    /// small output batch in flight, and the rows pulled so far can be in full batches.
    /// `arrays` is the number of arrays in a batch of the schema, counting nested ones.
    /// `router_bytes` is the range router storage allocated before pulling input.
    pub(super) fn heap_violations(
        &self,
        channels: usize,
        arrays: usize,
        batch_size: usize,
        router_bytes: usize,
    ) -> Vec<String> {
        let held_limit =
            |sample: &Sample| INPUT_FACTOR * sample.input_bytes + TASK_BYTES + router_bytes;
        let peak_limit = |sample: &Sample| {
            let batches = channels + sample.input_rows / batch_size;
            output_limit(sample.input_bytes, batches, arrays) + router_bytes
        };
        let describe = |what: &str, held: usize, limit: usize, sample: &Sample| {
            format!(
                "{held} bytes held {what} {} of {} input batches ({} bytes of input so far), \
                 limit {limit}",
                sample.input_batches, self.input_batches, sample.input_bytes
            )
        };
        let held = self
            .samples
            .iter()
            .filter(|sample| !sample.at_write && sample.held > held_limit(sample))
            .max_by_key(|sample| sample.held - held_limit(sample))
            .map(|sample| describe("after", sample.held, held_limit(sample), sample));
        let peak = self
            .samples
            .iter()
            .filter(|sample| sample.peak > peak_limit(sample))
            .max_by_key(|sample| sample.peak - peak_limit(sample))
            .map(|sample| describe("at most within", sample.peak, peak_limit(sample), sample));
        held.into_iter().chain(peak).collect()
    }
}

/// Returns the heap that output batches of the input may hold, in all.
pub(super) fn output_limit(input_bytes: usize, batches: usize, arrays: usize) -> usize {
    INPUT_FACTOR * input_bytes
        + TASK_BYTES
        + batches * (OUTPUT_BATCH_BYTES + arrays * OUTPUT_ARRAY_BYTES)
}

/// Returns the number of arrays in a batch, counting nested ones.
pub(super) fn arrays(batch: &RecordBatch) -> usize {
    fn count(data: &ArrayData) -> usize {
        1 + data.child_data().iter().map(count).sum::<usize>()
    }
    batch
        .columns()
        .iter()
        .map(|column| count(&column.to_data()))
        .sum()
}

fn with<T>(probe: &Mutex<Probe>, f: impl FnOnce(&mut Probe) -> T) -> T {
    f(&mut probe.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Streams its batches as they are, sampling the heap before each one is pulled.
#[derive(Debug)]
struct ProbedSource {
    schema: SchemaRef,
    batches: Arc<[RecordBatch]>,
    probe: Arc<Mutex<Probe>>,
}

impl PartitionStream for ProbedSource {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = Arc::clone(&self.batches);
        let probe = Arc::clone(&self.probe);
        let mut next = 0;
        let stream = futures::stream::poll_fn(move |_| -> Poll<Option<Result<RecordBatch>>> {
            let held = heap_held();
            let peak = heap_take_peak();
            // The write frees the batch, so it is cloned while the probe counts.
            let batch = batches.get(next).cloned();
            next += 1;
            untracked(|| {
                with(&probe, |probe| {
                    if probe.closed {
                        probe.pulled_after_close += 1;
                    }
                    probe.sample(false, held, peak);
                    if let Some(batch) = &batch {
                        probe.input_batches += 1;
                        probe.input_rows += batch.num_rows();
                        probe.input_bytes += batch.get_array_memory_size();
                    }
                })
            });
            Poll::Ready(batch.map(Ok))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

/// Records what is written, reporting `state` after each write.
#[derive(Debug)]
struct ProbeWriter {
    probe: Arc<Mutex<Probe>>,
    state: TaskStreamWriteState,
    retain: bool,
}

struct ProbeSink {
    probe: Arc<Mutex<Probe>>,
    state: TaskStreamWriteState,
    retain: bool,
}

#[tonic::async_trait]
impl TaskStreamWriter for ProbeWriter {
    async fn open(&self, _partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        Ok(Box::new(ProbeSink {
            probe: Arc::clone(&self.probe),
            state: self.state,
            retain: self.retain,
        }))
    }
}

#[tonic::async_trait]
impl TaskStreamSink for ProbeSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        let held = heap_held();
        let peak = heap_take_peak();
        let state = self.state;
        let retain = self.retain;
        untracked(|| {
            with(&self.probe, |probe| {
                probe.writes += 1;
                probe.sample(true, held, peak);
                if retain && probe.channels.len() < batches.len() {
                    probe.channels.resize(batches.len(), vec![]);
                }
                for (channel, batch) in batches.iter().enumerate() {
                    let Some(batch) = batch else {
                        continue;
                    };
                    probe.written_rows += batch.num_rows();
                    probe.written_batches += 1;
                    if retain {
                        probe.channels[channel].push(batch.clone());
                    }
                }
                if state == TaskStreamWriteState::Closed {
                    probe.closed = true;
                }
            })
        });
        // The write allocated the batches, so they are freed while the probe counts.
        drop(batches);
        Ok(state)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        untracked(|| with(&self.probe, |probe| probe.committed = true));
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        untracked(|| with(&self.probe, |probe| probe.aborted = true));
        Ok(())
    }
}

/// Shuffles the input with the batch size, sampling the heap that the write holds.
///
/// The output is kept only if `retain` is set, so that otherwise all the heap counted
/// is held by the write itself. The input must have at least one batch.
pub(super) async fn probed_shuffle(
    input: &[RecordBatch],
    partitioning: ShufflePartitioning,
    unbounded: bool,
    batch_size: usize,
    state: TaskStreamWriteState,
    retain: bool,
) -> Result<Probe> {
    let probe = Arc::new(Mutex::new(Probe::default()));
    with(&probe, |probe| probe.samples.reserve(2 * input.len() + 64));
    let schema = input[0].schema();
    let source = ProbedSource {
        schema: Arc::clone(&schema),
        batches: input.into(),
        probe: Arc::clone(&probe),
    };
    let writer = Arc::new(ProbeWriter {
        probe: Arc::clone(&probe),
        state,
        retain,
    });
    let config = super::session_config().with_batch_size(batch_size);
    let context = Arc::new(TaskContext::default().with_session_config(config));
    let result = {
        let _heap = HeapProbe::start();
        let plan = Arc::new(StreamingTableExec::try_new(
            schema,
            vec![Arc::new(source)],
            None,
            Vec::new(),
            unbounded,
            None,
        )?);
        let exec = ShuffleWriteExec::new(plan, writer, partitioning);
        let result = collect(exec.execute(0, context)?).await;
        drop(exec);
        let held = heap_held();
        let peak = heap_take_peak();
        with(&probe, |probe| {
            probe.held_at_end = held;
            probe.sample(true, held, peak);
        });
        result
    };
    result?;
    Ok(with(&probe, std::mem::take))
}

/// Returns the partitioning of a scheme by the Int64 column `k`, the first of the input.
///
/// Range partitioning splits the distinct keys of the input evenly. Channels that the
/// distinct keys cannot fill stay empty, and there are fewer than `channels` only if the
/// split points would overflow.
pub(super) fn key_partitioning(
    scheme: &str,
    input: &[RecordBatch],
    channels: usize,
) -> Result<ShufflePartitioning> {
    let key = col("k", &input[0].schema())?;
    Ok(match scheme {
        "hash" => ShufflePartitioning::Hash(vec![key], channels),
        "range" => {
            let mut keys: Vec<i64> = input
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect();
            keys.sort_unstable();
            keys.dedup();
            let mut points: Vec<i64> = (1..channels)
                .filter_map(|i| keys.get(i * keys.len() / channels).copied())
                .collect();
            points.dedup();
            while points.len() + 1 < channels {
                let next = match points.last() {
                    Some(last) => last.checked_add(1),
                    None => Some(0),
                };
                let Some(next) = next else {
                    break;
                };
                points.push(next);
            }
            let points = points
                .into_iter()
                .map(|point| SplitPoint::new(vec![ScalarValue::Int64(Some(point))]))
                .collect();
            ShufflePartitioning::Range(RangePartitioning::try_new(
                [PhysicalSortExpr::new_default(key)].into(),
                points,
            )?)
        }
        "round_robin_batch" => ShufflePartitioning::RoundRobinBatch(channels),
        _ => ShufflePartitioning::RoundRobinRow(channels),
    })
}

#[test]
fn heap_probe_counts_the_memory_of_its_thread() {
    const MIB: usize = 1024 * 1024;
    let probe = HeapProbe::start();
    let block = std::hint::black_box(vec![1u8; MIB]);
    assert!(heap_held() >= MIB);
    // Memory of other threads is not counted.
    let other = std::thread::spawn(|| std::hint::black_box(vec![1u8; 4 * MIB]).len());
    assert!(other.join().is_ok());
    assert!(heap_held() < 2 * MIB);
    drop(block);
    assert!(heap_held() < MIB / 16);
    assert!(heap_take_peak() >= MIB);
    assert!(heap_take_peak() < MIB / 16);
    drop(probe);
}
