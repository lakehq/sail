use std::sync::{Arc, RwLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use sail_server::actor::ActorContext;
use tokio::sync::{mpsc, watch};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::RecordBatchStreamWriter;
use crate::worker::WorkerActor;

pub(super) trait LocalStream: Send {
    fn publish(
        &mut self,
        ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<Box<dyn RecordBatchStreamWriter>>;
    fn subscribe(
        &mut self,
        ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<SendableRecordBatchStream>;
}

pub(super) struct EphemeralStream {
    tx: Option<mpsc::Sender<RecordBatch>>,
    rx: Option<mpsc::Receiver<RecordBatch>>,
    schema: SchemaRef,
}

impl EphemeralStream {
    pub fn new(buffer: usize, schema: SchemaRef) -> Self {
        let (tx, rx) = mpsc::channel(buffer);
        Self {
            tx: Some(tx),
            rx: Some(rx),
            schema,
        }
    }
}

impl LocalStream for EphemeralStream {
    fn publish(
        &mut self,
        _ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<Box<dyn RecordBatchStreamWriter>> {
        let tx = self.tx.take().ok_or_else(|| {
            ExecutionError::InternalError("ephemeral stream can only be written once".to_string())
        })?;
        Ok(Box::new(tx))
    }

    fn subscribe(
        &mut self,
        _ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let rx = self.rx.take().ok_or_else(|| {
            ExecutionError::InternalError("ephemeral stream can only be read once".to_string())
        })?;
        let stream =
            RecordBatchStreamAdapter::new(self.schema.clone(), ReceiverStream::new(rx).map(Ok));
        Ok(Box::pin(stream))
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryStreamState {
    count: usize,
    completed: bool,
}

#[derive(Debug)]
struct MemoryStreamWriter {
    tx: watch::Sender<MemoryStreamState>,
    batches: Arc<RwLock<Vec<RecordBatch>>>,
}

#[tonic::async_trait]
impl RecordBatchStreamWriter for MemoryStreamWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let mut batches = self
            .batches
            .write()
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        batches.push(batch);
        self.tx
            .send(MemoryStreamState {
                count: batches.len(),
                completed: false,
            })
            .map_err(|e| DataFusionError::Internal(e.to_string()))
    }

    fn close(self: Box<Self>) -> Result<()> {
        let batches = self
            .batches
            .read()
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        self.tx
            .send(MemoryStreamState {
                count: batches.len(),
                completed: true,
            })
            .map_err(|e| DataFusionError::Internal(e.to_string()))
    }
}

/// A memory stream that can be read multiple times.
/// It maintains an unbounded list of record batches in memory.
pub(super) struct MemoryStream {
    batches: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
    tx: Option<watch::Sender<MemoryStreamState>>,
    rx: watch::Receiver<MemoryStreamState>,
}

impl MemoryStream {
    pub fn new(schema: SchemaRef) -> Self {
        let (tx, rx) = watch::channel(MemoryStreamState::default());
        Self {
            batches: Arc::new(RwLock::new(vec![])),
            schema,
            tx: Some(tx),
            rx,
        }
    }
}

impl LocalStream for MemoryStream {
    fn publish(
        &mut self,
        _ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<Box<dyn RecordBatchStreamWriter>> {
        let tx = self.tx.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(MemoryStreamWriter {
            tx,
            batches: self.batches.clone(),
        }))
    }

    fn subscribe(
        &mut self,
        ctx: &mut ActorContext<WorkerActor>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        // The channel only needs a capacity of 1 since the stream reader
        // controls the consumption rate.
        let (tx, rx) = mpsc::channel(1);
        let mut watcher = self.rx.clone();
        let batches = self.batches.clone();
        ctx.spawn(async move {
            let mut n = 0;
            loop {
                if watcher.changed().await.is_err() {
                    return;
                }
                let MemoryStreamState { count, completed } = watcher.borrow_and_update().clone();
                // Acquire the read lock inside an inner scope.
                // The lock is released after collecting all the remaining batches.
                let remaining = {
                    let Ok(batches) = batches.read() else {
                        return;
                    };
                    let out = batches[n..count].to_vec();
                    n = count;
                    out
                };
                for batch in remaining {
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
                if completed {
                    return;
                }
            }
        });
        let stream =
            RecordBatchStreamAdapter::new(self.schema.clone(), ReceiverStream::new(rx).map(Ok));
        Ok(Box::pin(stream))
    }
}
