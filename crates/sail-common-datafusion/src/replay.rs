//! Execution-scoped, named, sealed replay data. No process-local handle is a wire identity.

#[cfg(test)]
mod backing_tests;
#[cfg(test)]
mod export_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayData, BinaryViewArray, StringViewArray, make_array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::spill_file::SpillFile;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::config::SpillCompression;
use datafusion_common::{DataFusionError, Result, exec_err, internal_datafusion_err, internal_err};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ReplayId {
    pub scope: Uuid,
    pub ordinal: u32,
}

impl Display for ReplayId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.scope, self.ordinal)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEpoch {
    pub session_id: String,
    pub job_id: u64,
}

// No batch index or in-memory batch cache: one IPC stream per partition.
const MAX_BATCH_BYTES: usize = 256 * 1024;
const MIN_BATCH_BYTES: usize = 1024;
// DiskManager's unbuffered stream still has a 128 KiB byte buffer.
const IO_BYTES: usize = 128 * 1024;

fn resource_error(message: &str) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("replay: {message}"))
}

fn checked_size(parts: &[usize]) -> Result<usize> {
    parts.iter().try_fold(0usize, |a, b| {
        a.checked_add(*b)
            .ok_or_else(|| resource_error("memory size overflow"))
    })
}

fn schema_bytes(schema: &SchemaRef) -> Result<usize> {
    let metadata = schema.metadata().iter().try_fold(0usize, |size, (k, v)| {
        checked_size(&[size, k.capacity(), v.capacity(), 128])
    })?;
    checked_size(&[schema.fields().size(), metadata, 1024])
}

fn workspace_bytes(schema: &SchemaRef, batch_bytes: usize) -> Result<usize> {
    // Uncompressed IPC: compacted view data, IPC body/metadata, dictionary state,
    // and decoder output. Retain this reservation until the writer/reader drops.
    let data = batch_bytes
        .checked_mul(4)
        .ok_or_else(|| resource_error("batch too large"))?;
    let schema = schema_bytes(schema)?
        .checked_mul(4)
        .ok_or_else(|| resource_error("schema too large"))?;
    checked_size(&[data, schema, IO_BYTES])
}

fn sliced_bytes(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(0usize, |size, array| {
        checked_size(&[
            size,
            array.to_data().get_slice_memory_size()?,
            array_overhead(array.as_ref())?,
        ])
    })
}

fn array_overhead(array: &dyn Array) -> Result<usize> {
    // Arrow's layout omits external byte-view buffers, including nested views.
    // DF compacts these above 10 KiB; smaller backing buffers may survive whole.
    let mut bytes = 256;
    if let Some(view) = array.as_any().downcast_ref::<StringViewArray>() {
        let retained = view
            .data_buffers()
            .iter()
            .try_fold(0usize, |size, b| checked_size(&[size, b.capacity()]))?;
        let used = view
            .iter()
            .flatten()
            .filter(|x| x.len() > 12)
            .try_fold(0usize, |size, v| checked_size(&[size, v.len()]))?;
        bytes = checked_size(&[bytes, used.max(retained.min(10 * 1024))])?;
    } else if let Some(view) = array.as_any().downcast_ref::<BinaryViewArray>() {
        let retained = view
            .data_buffers()
            .iter()
            .try_fold(0usize, |size, b| checked_size(&[size, b.capacity()]))?;
        let used = view
            .iter()
            .flatten()
            .filter(|x| x.len() > 12)
            .try_fold(0usize, |size, v| checked_size(&[size, v.len()]))?;
        bytes = checked_size(&[bytes, used.max(retained.min(10 * 1024))])?;
    }
    for child in array.to_data().child_data() {
        bytes = checked_size(&[bytes, array_overhead(make_array(child.clone()).as_ref())?])?;
    }
    Ok(bytes)
}

fn track_dictionary_backing(
    data: &ArrayData,
    maxima: &mut Vec<usize>,
    slot: &mut usize,
) -> Result<()> {
    if matches!(data.data_type(), DataType::Dictionary(_, _)) {
        if maxima.len() == *slot {
            maxima.push(0);
        }
        maxima[*slot] = maxima[*slot].max(checked_size(&[
            data.get_array_memory_size(),
            std::mem::size_of::<usize>(),
        ])?);
        *slot += 1;
    }
    for child in data.child_data() {
        track_dictionary_backing(child, maxima, slot)?;
    }
    Ok(())
}

// DataFusion exposes the factory but keeps InProgressSpillFile's module private.
// Type erasure here keeps append/flush/finish on the upstream implementation.
type AppendOrFinish =
    Box<dyn FnMut(Option<&RecordBatch>) -> Result<Option<Arc<dyn SpillFile>>> + Send>;

pub struct ReplayPartitionBuilder {
    manager: SpillManager,
    writer: Option<AppendOrFinish>,
    reservation: MemoryReservation,
    dictionary_reservation: MemoryReservation,
    dictionary_bytes: Vec<usize>,
    max_batch_bytes: usize,
    rows: usize,
}

impl ReplayPartitionBuilder {
    pub fn try_new(schema: SchemaRef, runtime: Arc<RuntimeEnv>, partition: usize) -> Result<Self> {
        let reservation = MemoryConsumer::new("ReplaySpool")
            .with_can_spill(true)
            .register(&runtime.memory_pool);
        let mut max_batch_bytes = MAX_BATCH_BYTES;
        loop {
            match reservation.try_resize(workspace_bytes(&schema, max_batch_bytes)?) {
                Ok(()) => break,
                Err(e) if max_batch_bytes <= MIN_BATCH_BYTES => return Err(e),
                Err(_) => max_batch_bytes /= 2,
            }
        }
        let manager = SpillManager::new(
            runtime,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), partition),
            schema,
        )
        .with_compression_type(SpillCompression::Uncompressed);
        let mut file = manager.create_in_progress_file("replay partition")?;
        let writer = Box::new(move |batch: Option<&RecordBatch>| {
            if let Some(batch) = batch {
                file.append_batch(batch)?;
                Ok(None)
            } else {
                file.flush()?;
                file.finish()
            }
        });
        let dictionary_reservation = reservation.new_empty();
        Ok(Self {
            manager,
            writer: Some(writer),
            reservation,
            dictionary_reservation,
            dictionary_bytes: vec![],
            max_batch_bytes,
            rows: 0,
        })
    }

    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        let result = self.append_inner(batch);
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn append_inner(&mut self, batch: RecordBatch) -> Result<()> {
        if self.writer.is_none() {
            return exec_err!("replay partition has been aborted");
        }
        if batch.schema() != *self.manager.schema() {
            return internal_err!("replay batch schema mismatch");
        }
        // Slices retain the original backing allocations. Never pretend splitting
        // the IPC payload makes an arbitrarily large upstream batch free.
        let input = self.reservation.new_empty();
        input.try_grow(batch.get_array_memory_size())?;
        // Arrow's DictionaryTracker retains ArrayData, including sliced key
        // backing buffers. Reserve each dictionary slot's high-water mark until
        // seal, even when equal dictionaries keep an earlier batch alive.
        let mut slot = 0;
        for column in batch.columns() {
            track_dictionary_backing(&column.to_data(), &mut self.dictionary_bytes, &mut slot)?;
        }
        self.dictionary_reservation
            .try_resize(checked_size(&self.dictionary_bytes)?)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let mut rows = (batch.num_rows() - offset).min(8192);
            let chunk = loop {
                let chunk = batch.slice(offset, rows);
                let required = sliced_bytes(&chunk)?;
                if required <= self.max_batch_bytes {
                    break chunk;
                }
                if rows == 1 {
                    // The normal chunk target is not a row-size limit. Keep
                    // the high-water budget for the IPC writer and every reader.
                    self.reservation
                        .try_resize(workspace_bytes(self.manager.schema(), required)?)?;
                    self.max_batch_bytes = required;
                    break chunk;
                }
                rows = rows.div_ceil(2);
            };
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| internal_datafusion_err!("missing replay writer"))?;
            writer(Some(&chunk))?;
            offset += rows;
        }
        self.rows = self
            .rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| resource_error("row count overflow"))?;
        Ok(())
    }

    pub fn seal(mut self) -> Result<ReplayPartition> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| internal_datafusion_err!("cannot seal aborted replay"))?;
        let file = writer(None)?;
        // Drop the IPC writer (including dictionary/scratch buffers) before freeing
        // its reservation. A sealed partition contains no cached record batches.
        drop(writer);
        let dictionary_io = self
            .dictionary_bytes
            .len()
            .checked_mul(IO_BYTES)
            .ok_or_else(|| resource_error("too many dictionary slots"))?;
        let dictionary_data = checked_size(&self.dictionary_bytes)?
            .checked_mul(4)
            .ok_or_else(|| resource_error("dictionary reader budget overflow"))?;
        // StreamDecoder retains dictionary values between batches. Each slot
        // can pin a different input chunk, including for a one-byte dictionary.
        let decoder_memory_bytes = checked_size(&[
            workspace_bytes(self.manager.schema(), self.max_batch_bytes)?,
            dictionary_io,
            dictionary_data,
        ])?;
        self.dictionary_reservation.free();
        self.reservation.free();
        Ok(ReplayPartition {
            manager: self.manager,
            file,
            decoder_memory_bytes,
            rows: self.rows,
        })
    }

    pub fn abort(&mut self) {
        self.writer.take();
        self.dictionary_reservation.free();
        self.reservation.free();
    }
}

pub struct ReplayPartition {
    manager: SpillManager,
    file: Option<Arc<dyn SpillFile>>,
    decoder_memory_bytes: usize,
    rows: usize,
}

impl Debug for ReplayPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayPartition")
            .field("rows", &self.rows)
            .field("spilled", &self.file.is_some())
            .finish()
    }
}

/// A read-only, leased export of the sealed IPC stream. Metadata is constant-size;
/// no batch index or second serialization is needed by remote storage backends.
pub struct ReplayExport {
    pub length: u64,
    pub rows: usize,
    pub decoder_memory_bytes: usize,
    pub chunks: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>,
}

impl ReplayPartition {
    pub fn export(&self, runtime: &RuntimeEnv) -> Result<ReplayExport> {
        let reservation = MemoryConsumer::new("ReplayExport").register(&runtime.memory_pool);
        reservation.try_grow(2 * IO_BYTES)?;
        let length = match &self.file {
            Some(file) => file
                .size()
                .ok_or_else(|| internal_datafusion_err!("replay spill length unavailable"))?,
            None => 0,
        };
        let chunks = match &self.file {
            Some(file) => {
                let stream = file.read_stream()?;
                let file = file.clone();
                Box::pin(futures::stream::try_unfold(
                    (stream, file, reservation, length),
                    |(mut stream, file, reservation, remaining)| async move {
                        match stream.next().await {
                            Some(chunk) => {
                                let chunk = chunk?;
                                // SpillFile implementations must not allocate an unbounded
                                // transfer buffer behind the export reservation.
                                if chunk.len() > IO_BYTES {
                                    return exec_err!("oversized replay export chunk");
                                }
                                let remaining =
                                    remaining.checked_sub(chunk.len() as u64).ok_or_else(|| {
                                        internal_datafusion_err!(
                                            "replay export exceeds sealed length"
                                        )
                                    })?;
                                Ok(Some((
                                    chunk.to_vec(),
                                    (stream, file, reservation, remaining),
                                )))
                            }
                            None if remaining != 0 => {
                                exec_err!("replay export ended before sealed length")
                            }
                            None => Ok(None),
                        }
                    },
                )) as Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>
            }
            None => Box::pin(futures::stream::empty()),
        };
        Ok(ReplayExport {
            length,
            rows: self.rows,
            decoder_memory_bytes: self.decoder_memory_bytes,
            chunks,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        self.manager.schema()
    }

    pub fn row_count(&self) -> usize {
        self.rows
    }

    pub fn open_reader(&self, runtime: &RuntimeEnv) -> Result<SendableRecordBatchStream> {
        let Some(file) = &self.file else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema().clone(),
                futures::stream::empty(),
            )));
        };
        let reservation = MemoryConsumer::new("ReplayReader").register(&runtime.memory_pool);
        reservation.try_grow(self.decoder_memory_bytes)?;
        let stream = self
            .manager
            .read_spill_as_stream_unbuffered(file.clone(), None)?;
        // The decoder and its file lease die before the reservation is released.
        Ok(Box::pin(ReservedReader {
            stream,
            _reservation: reservation,
            remaining_rows: self.rows,
            done: false,
        }))
    }
}

struct ReservedReader {
    stream: SendableRecordBatchStream,
    _reservation: MemoryReservation,
    remaining_rows: usize,
    done: bool,
}

impl Stream for ReservedReader {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let Some(remaining) = self.remaining_rows.checked_sub(batch.num_rows()) else {
                    self.done = true;
                    return Poll::Ready(Some(exec_err!("replay contains more rows than sealed")));
                };
                self.remaining_rows = remaining;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) if self.remaining_rows != 0 => {
                self.done = true;
                Poll::Ready(Some(exec_err!(
                    "replay ended before all sealed rows were read"
                )))
            }
            result @ Poll::Ready(_) => {
                self.done = true;
                result
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl RecordBatchStream for ReservedReader {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

#[derive(Debug)]
enum ReplayState {
    Registered,
    Building,
    Sealed(Vec<Arc<ReplayPartition>>),
}

#[derive(Debug)]
struct ReplayEntry {
    schema: SchemaRef,
    partitioning: Partitioning,
    state: ReplayState,
    _reservation: MemoryReservation,
}

#[derive(Debug, Default)]
struct RegistryState {
    retired: bool,
    entries: HashMap<ReplayId, ReplayEntry>,
}

/// One registry per (session, job). Entries never return to Registered after
/// building, publication, failure, or cancellation. Readers cannot run producers.
#[derive(Debug)]
pub struct ReplayRegistry {
    epoch: ReplayEpoch,
    state: Mutex<RegistryState>,
    cancellation: CancellationToken,
}

impl ReplayRegistry {
    pub fn new(epoch: ReplayEpoch, cancellation: CancellationToken) -> Self {
        Self {
            epoch,
            state: Mutex::new(RegistryState::default()),
            cancellation,
        }
    }

    pub fn epoch(&self) -> &ReplayEpoch {
        &self.epoch
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn register(
        &self,
        id: ReplayId,
        schema: SchemaRef,
        partitioning: Partitioning,
        runtime: &RuntimeEnv,
    ) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || self.cancellation.is_cancelled() {
            return exec_err!("replay epoch is retired");
        }
        if let Some(entry) = state.entries.get(&id) {
            // DF intentionally defines UnknownPartitioning != itself; here we
            // validate the declared layout, not optimizer distribution equivalence.
            let same_partitioning = match (&entry.partitioning, &partitioning) {
                (Partitioning::UnknownPartitioning(a), Partitioning::UnknownPartitioning(b)) => {
                    a == b
                }
                (a, b) => a == b,
            };
            if entry.schema != schema || !same_partitioning {
                return internal_err!("conflicting schema or partitioning for replay {id}");
            }
            return Ok(false);
        }
        let partitions = partitioning.partition_count();
        if partitions == 0 {
            return internal_err!("replay requires at least one partition");
        }
        let reservation = MemoryConsumer::new("ReplayRegistry").register(&runtime.memory_pool);
        // Account the frozen partition vector, entries, and both build/publish
        // pointer vectors. There is no per-record or per-batch index.
        let index_bytes = partitions
            .checked_mul(2048 + std::mem::size_of::<ReplayPartition>())
            .ok_or_else(|| resource_error("too many replay partitions"))?;
        reservation.try_grow(checked_size(&[schema_bytes(&schema)?, index_bytes, 1024])?)?;
        state.entries.insert(
            id,
            ReplayEntry {
                schema,
                partitioning,
                state: ReplayState::Registered,
                _reservation: reservation,
            },
        );
        Ok(true)
    }

    pub fn begin(&self, id: ReplayId) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || self.cancellation.is_cancelled() {
            return exec_err!("replay epoch is retired");
        }
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| internal_datafusion_err!("unregistered replay {id}"))?;
        if !matches!(entry.state, ReplayState::Registered) {
            return internal_err!("replay producer cannot restart: {id}");
        }
        entry.state = ReplayState::Building;
        Ok(())
    }

    pub fn publish(&self, id: ReplayId, partitions: Vec<Arc<ReplayPartition>>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || self.cancellation.is_cancelled() {
            return exec_err!("replay epoch is retired");
        }
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| internal_datafusion_err!("unregistered replay {id}"))?;
        if !matches!(entry.state, ReplayState::Building) {
            return internal_err!("replay is not building: {id}");
        }
        if partitions.len() != entry.partitioning.partition_count()
            || partitions.iter().any(|p| p.schema() != &entry.schema)
        {
            return internal_err!("replay publication schema or partition count mismatch: {id}");
        }
        entry.state = ReplayState::Sealed(partitions);
        Ok(())
    }

    pub fn open_reader(
        self: &Arc<Self>,
        id: ReplayId,
        partition: usize,
        runtime: &RuntimeEnv,
    ) -> Result<SendableRecordBatchStream> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || self.cancellation.is_cancelled() {
            return exec_err!("replay epoch is retired");
        }
        let entry = state
            .entries
            .get(&id)
            .ok_or_else(|| internal_datafusion_err!("unregistered replay {id}"))?;
        let ReplayState::Sealed(partitions) = &entry.state else {
            return exec_err!("replay is not sealed: {id}");
        };
        let data = partitions
            .get(partition)
            .ok_or_else(|| internal_datafusion_err!("invalid replay partition {partition}"))?;
        let reader = data.open_reader(runtime)?;
        Ok(self.wrap_stream(reader, false))
    }

    pub fn retire(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.retired = true;
        state.entries = HashMap::new();
        self.cancellation.cancel();
    }

    /// The root owns retirement; individual readers only own file leases.
    pub fn wrap_stream(
        self: &Arc<Self>,
        stream: SendableRecordBatchStream,
        owns_epoch: bool,
    ) -> SendableRecordBatchStream {
        Box::pin(ReplayStream {
            schema: stream.schema(),
            stream: Some(stream),
            registry: self.clone(),
            cancelled: Box::pin(self.cancellation.clone().cancelled_owned()),
            owns_epoch,
        })
    }
}

struct ReplayStream {
    schema: SchemaRef,
    stream: Option<SendableRecordBatchStream>,
    registry: Arc<ReplayRegistry>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    owns_epoch: bool,
}

impl Stream for ReplayStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stream.is_none() {
            return Poll::Ready(None);
        }
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.stream.take();
            self.registry.retire();
            return Poll::Ready(Some(exec_err!("replay epoch cancelled or retired")));
        }
        let result = match self.stream.as_mut() {
            Some(stream) => stream.poll_next_unpin(cx),
            None => return Poll::Ready(None),
        };
        match &result {
            Poll::Ready(Some(Err(_))) => {
                self.stream.take();
                self.registry.retire();
            }
            Poll::Ready(None) => {
                self.stream.take();
                if self.owns_epoch {
                    self.registry.retire();
                }
            }
            _ => {}
        }
        result
    }
}
impl RecordBatchStream for ReplayStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for ReplayStream {
    fn drop(&mut self) {
        // Stop readers before retiring the registry's file ownership.
        self.stream.take();
        if self.owns_epoch {
            self.registry.retire();
        }
    }
}
