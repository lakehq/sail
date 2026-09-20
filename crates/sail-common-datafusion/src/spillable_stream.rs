//! Pipelined multicast with a shared, spillable backlog and independent readers.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::watch;

// Also bound retained memory when the session memory pool is unbounded.
const MEMORY_LIMIT: usize = 8 * 1024 * 1024;

enum Data {
    Memory(RecordBatch, usize),
    Spill(Arc<dyn SpillFile>),
}

struct Entry {
    start: usize,
    end: usize,
    data: Data,
}

enum Status {
    Writing,
    Complete,
    Failed(Arc<DataFusionError>),
}

struct State {
    entries: VecDeque<Entry>,
    positions: Vec<Option<usize>>,
    next: usize,
    memory_batches: usize,
    reservation: MemoryReservation,
    status: Status,
}

impl State {
    fn reclaim(&mut self) {
        let first = self
            .positions
            .iter()
            .flatten()
            .copied()
            .min()
            .unwrap_or(self.next);
        while self.entries.front().is_some_and(|entry| entry.end <= first) {
            if let Some(Entry {
                data: Data::Memory(_, size),
                ..
            }) = self.entries.pop_front()
            {
                self.reservation.shrink(size);
                self.memory_batches -= 1;
            }
        }
    }
}

struct Shared {
    state: Mutex<State>,
    changed: watch::Sender<()>,
    manager: Arc<SpillManager>,
    schema: SchemaRef,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Subscribers must be reserved before production starts. Keeping the returned
/// stream unpolled reserves its cursor; dropping it cancels that reservation.
pub struct SpillableStream {
    shared: Arc<Shared>,
}

impl SpillableStream {
    pub fn new(
        context: &TaskContext,
        schema: SchemaRef,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        buffer: usize,
    ) -> (Self, SpillableStreamWriter) {
        let (changed, _) = watch::channel(());
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: VecDeque::new(),
                positions: vec![],
                next: 0,
                memory_batches: 0,
                reservation: MemoryConsumer::new("PipelinedStream")
                    .with_can_spill(true)
                    .register(context.memory_pool()),
                status: Status::Writing,
            }),
            changed,
            manager: Arc::new(SpillManager::new(
                context.runtime_env().clone(),
                SpillMetrics::new(metrics, partition),
                schema.clone(),
            )),
            schema,
        });
        (
            Self {
                shared: shared.clone(),
            },
            SpillableStreamWriter {
                shared,
                buffer: buffer.max(1),
            },
        )
    }

    pub fn subscribe(&self) -> Result<SendableRecordBatchStream> {
        let mut state = self.shared.lock();
        if state.next != 0 || !matches!(state.status, Status::Writing) {
            return internal_err!("pipelined readers must be reserved before production");
        }
        let id = state.positions.len();
        state.positions.push(Some(0));
        drop(state);
        let reader = Reader {
            shared: self.shared.clone(),
            changed: self.shared.changed.subscribe(),
            id,
            spill: None,
        };
        let stream = futures::stream::try_unfold(reader, |mut reader| async move {
            Ok(reader.next().await?.map(|batch| (batch, reader)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.shared.schema.clone(),
            stream,
        )))
    }
}

pub struct SpillableStreamWriter {
    shared: Arc<Shared>,
    buffer: usize,
}

impl SpillableStreamWriter {
    /// Returns false once every reserved reader has been dropped.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<bool> {
        // Let readers consume or cancel before growing the backlog. In
        // particular, a LIMIT must get a chance to close its cursor.
        tokio::task::yield_now().await;
        let size = batch
            .get_array_memory_size()
            .max(std::mem::size_of::<RecordBatch>());
        let needs_spill = {
            let state = self.shared.lock();
            if state.positions.iter().all(Option::is_none) {
                return Ok(false);
            }
            state.memory_batches >= self.buffer
                || state.reservation.size().saturating_add(size) > MEMORY_LIMIT
        };
        if needs_spill {
            self.spill().await?;
        }
        let retained = {
            let mut state = self.shared.lock();
            if state.positions.iter().all(Option::is_none) {
                return Ok(false);
            }
            let retained = size <= MEMORY_LIMIT && state.reservation.try_grow(size).is_ok();
            if retained {
                let start = state.next;
                state.next += 1;
                state.memory_batches += 1;
                state.entries.push_back(Entry {
                    start,
                    end: start + 1,
                    data: Data::Memory(batch.clone(), size),
                });
            }
            retained
        };
        if !retained {
            self.spill().await?;
            let manager = self.shared.manager.clone();
            let file = tokio::task::spawn_blocking(move || {
                manager.spill_record_batch_and_finish(&[batch], "pipelined stream")
            })
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))??;
            let mut state = self.shared.lock();
            if let Some(file) = file {
                let start = state.next;
                state.next += 1;
                state.entries.push_back(Entry {
                    start,
                    end: start + 1,
                    data: Data::Spill(file),
                });
                state.reclaim();
            }
        }
        self.shared.changed.send_replace(());
        Ok(true)
    }

    async fn spill(&mut self) -> Result<()> {
        let (start, end, batches) = {
            let state = self.shared.lock();
            let batches = state
                .entries
                .iter()
                .filter_map(|entry| match &entry.data {
                    Data::Memory(batch, _) => Some((entry.start, batch.clone())),
                    Data::Spill(_) => None,
                })
                .collect::<Vec<_>>();
            let Some((start, _)) = batches.first() else {
                return Ok(());
            };
            (
                *start,
                state.next,
                batches
                    .into_iter()
                    .map(|(_, batch)| batch)
                    .collect::<Vec<_>>(),
            )
        };
        let manager = self.shared.manager.clone();
        // Readers may continue consuming the in-memory tail while it is written.
        // Never hold the state lock across disk I/O or an await.
        let file = tokio::task::spawn_blocking(move || {
            manager.spill_record_batch_and_finish(&batches, "pipelined stream")
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))??;
        let mut state = self.shared.lock();
        let mut remaining = false;
        while state
            .entries
            .back()
            .is_some_and(|entry| entry.start >= start)
        {
            if let Some(Entry {
                data: Data::Memory(_, size),
                ..
            }) = state.entries.pop_back()
            {
                state.reservation.shrink(size);
                state.memory_batches -= 1;
                remaining = true;
            }
        }
        if remaining && let Some(file) = file {
            state.entries.push_back(Entry {
                start,
                end,
                data: Data::Spill(file),
            });
        }
        Ok(())
    }

    pub fn finish(self) {
        self.shared.lock().status = Status::Complete;
        self.shared.changed.send_replace(());
    }

    pub fn fail(self, error: DataFusionError) {
        self.shared.lock().status = Status::Failed(Arc::new(error));
        self.shared.changed.send_replace(());
    }
}

impl Drop for SpillableStreamWriter {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        if matches!(state.status, Status::Writing) {
            state.status = Status::Failed(Arc::new(DataFusionError::Execution(
                "pipelined stream aborted".into(),
            )));
        }
        drop(state);
        self.shared.changed.send_replace(());
    }
}

struct Reader {
    shared: Arc<Shared>,
    changed: watch::Receiver<()>,
    id: usize,
    spill: Option<SendableRecordBatchStream>,
}

impl Reader {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Mark notifications seen before inspecting state to avoid lost wakes.
            self.changed.borrow_and_update();
            if let Some(spill) = &mut self.spill {
                if let Some(batch) = spill.try_next().await? {
                    let mut state = self.shared.lock();
                    if let Some(position) = &mut state.positions[self.id] {
                        *position += 1;
                    }
                    state.reclaim();
                    return Ok(Some(batch));
                }
                self.spill = None;
            }
            {
                let mut state = self.shared.lock();
                if let Status::Failed(error) = &state.status {
                    return Err(DataFusionError::Shared(error.clone()));
                }
                let Some(position) = state.positions[self.id] else {
                    return Ok(None);
                };
                let index = state.entries.partition_point(|entry| entry.end <= position);
                if let Some(entry) = state.entries.get(index) {
                    match &entry.data {
                        Data::Memory(batch, _) => {
                            let batch = batch.clone();
                            state.positions[self.id] = Some(position + 1);
                            state.reclaim();
                            return Ok(Some(batch));
                        }
                        Data::Spill(file) => {
                            let stream = self
                                .shared
                                .manager
                                .read_spill_as_stream(file.clone(), None)?;
                            self.spill = Some(Box::pin(RecordBatchStreamAdapter::new(
                                self.shared.schema.clone(),
                                stream.skip(position - entry.start),
                            )));
                            continue;
                        }
                    }
                }
                if matches!(state.status, Status::Complete) {
                    return Ok(None);
                }
            }
            self.changed
                .changed()
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.positions[self.id] = None;
        state.reclaim();
    }
}
