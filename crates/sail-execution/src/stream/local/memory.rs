use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use futures::future::join_all;
use log::debug;
use sail_common_datafusion::error::CommonErrorCause;
use sail_physical_plan::shared_cte::{ReplayBuffer, shared_cte_memory_limit};
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    sender: Option<Box<dyn TaskStreamChannelSink>>,
    receivers: Vec<mpsc::Receiver<TaskStreamResult<RecordBatch>>>,
}

impl MemoryStream {
    /// Creates a memory stream. With a `replay` context, the stream is buffered in full
    /// (in memory under an unbounded memory pool, and on disk beyond a small size under a
    /// bounded one), and then read by each replica.
    pub fn new(
        buffer: usize,
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        replay: Option<(Arc<TaskContext>, SchemaRef)>,
    ) -> Self {
        let replicas = replicas.max(senders.len());
        let diff = replicas - senders.len();
        let mut senders = senders.into_iter().map(Some).collect::<Vec<_>>();
        senders.reserve(diff);
        let mut receivers = Vec::with_capacity(diff);
        for _ in 0..diff {
            let (tx, rx) = mpsc::channel(buffer);
            senders.push(Some(tx));
            receivers.push(rx);
        }
        let sender: Box<dyn TaskStreamChannelSink> = match replay {
            Some((context, schema)) => Box::new(MemoryStreamReplaySender {
                senders: senders.into_iter().flatten().collect(),
                context,
                schema,
                buffer: None,
            }),
            None => {
                let overflow = vec![VecDeque::new(); senders.len()];
                Box::new(MemoryStreamReplicaSender { senders, overflow })
            }
        };
        Self {
            sender: Some(sender),
            receivers,
        }
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

/// The input of a replay buffer and the task that fills the buffer.
type ReplayInput = (
    mpsc::Sender<Result<RecordBatch>>,
    SpawnedTask<Result<ReplayBuffer>>,
);

/// Buffers the stream in full before any replica reads it, so that the replicas are
/// read at different paces. The stream is kept in memory under an unbounded memory pool,
/// and written to disk beyond a small size under a bounded one.
struct MemoryStreamReplaySender {
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
    buffer: Option<ReplayInput>,
}

impl MemoryStreamReplaySender {
    fn buffer(&mut self) -> &mut ReplayInput {
        self.buffer.get_or_insert_with(|| {
            let (tx, rx) = mpsc::channel(1);
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                ReceiverStream::new(rx),
            ));
            let context = Arc::clone(&self.context);
            let task = SpawnedTask::spawn(async move {
                let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                ReplayBuffer::try_new(
                    stream,
                    &context,
                    "MemoryStream".to_string(),
                    metrics,
                    Arc::new(AtomicUsize::new(shared_cte_memory_limit(&context))),
                )
                .await
            });
            (tx, task)
        })
    }

    async fn finish(
        mut self,
    ) -> (
        Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        Result<ReplayBuffer>,
    ) {
        self.buffer();
        let buffer = match self.buffer.take() {
            Some((tx, task)) => {
                drop(tx);
                match task.join().await {
                    Ok(buffer) => buffer,
                    Err(error) => Err(DataFusionError::ExecutionJoin(Box::new(error))),
                }
            }
            None => Err(DataFusionError::Internal(
                "memory stream buffer is missing".to_string(),
            )),
        };
        (self.senders, buffer)
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamReplaySender {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        if self.senders.iter().all(|tx| tx.is_closed()) {
            return Ok(TaskStreamWriteState::Closed);
        }
        let (tx, _) = self.buffer();
        if tx.send(Ok(batch)).await.is_err() {
            // The buffer has failed, and the error is the output of its task.
            let Some((_, task)) = self.buffer.take() else {
                return Ok(TaskStreamWriteState::Closed);
            };
            task.join()
                .await
                .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))??;
            return Ok(TaskStreamWriteState::Closed);
        }
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.serve(None).await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn fail(self: Box<Self>, error: &DataFusionError) -> Result<bool> {
        // The error is the last item of every replica, so that it fails only the
        // consumers that read the output to its end, as they would fail if they
        // produced the output themselves.
        let cause = CommonErrorCause::new::<PyErrExtractor>(error);
        self.serve(Some(TaskStreamError::from(cause))).await?;
        Ok(true)
    }
}

impl MemoryStreamReplaySender {
    /// Serves the buffered stream to each replica, followed by the error if any.
    async fn serve(self: Box<Self>, error: Option<TaskStreamError>) -> Result<()> {
        let (senders, buffer) = self.finish().await;
        let buffer = match buffer {
            Ok(buffer) => Arc::new(buffer),
            Err(error) => {
                // Each replica ends with the error rather than with the end of the stream,
                // so that no consumer reads a part of the output as the whole output.
                let cause = TaskStreamError::from(CommonErrorCause::new::<PyErrExtractor>(&error));
                for tx in senders {
                    // Nothing has been sent to the replica yet, so the channel has room.
                    let _ = tx.try_send(Err(cause.clone()));
                }
                return Err(error);
            }
        };
        // Each replica is read at its own pace, or not at all, after the task ends. The
        // buffer is released when every replica is read or dropped.
        for tx in senders {
            let mut stream = buffer.stream();
            let error = error.clone();
            tokio::spawn(async move {
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|error| TaskStreamError::Unknown(error.to_string()));
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
                if let Some(error) = error {
                    let _ = tx.send(Err(error)).await;
                }
            });
        }
        Ok(())
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<mpsc::Sender<TaskStreamResult<RecordBatch>>>>,
    /// An overflow buffer for each sender to avoid blocking sending for slow senders.
    /// This also avoids deadlock situations where the task stream buffer size is small.
    // TODO: More investigation is needed to understand deadlocks during writes when
    //   task stream buffers are bounded, and whether unbounded overflow can be avoided.
    //   Concurrent finalization fixes commit deadlocks but does not remove this need.
    overflow: Vec<VecDeque<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let mut active = false;
        for (i, sender) in self.senders.iter_mut().enumerate() {
            if sender.is_none() {
                continue;
            }

            let overflow = &mut self.overflow[i];
            let mut dropped = false;

            if let Some(tx) = sender.as_ref() {
                // Try to flush overflow first
                while let Some(item) = overflow.pop_front() {
                    match tx.try_send(item) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_front(x);
                            break;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                            break;
                        }
                    }
                }
            }

            // A dropped receiver can happen under normal operation when the receiver no longer
            // needs more data (e.g., after a LIMIT operator has received enough rows).

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
                continue;
            }

            if let Some(tx) = sender.as_ref() {
                if overflow.is_empty() {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_back(x);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                        }
                    }
                } else {
                    overflow.push_back(Ok(batch.clone()));
                }
            }

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
            } else {
                active = true;
            }
        }
        Ok(if active {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // Replicas can be consumed at different times or left unread entirely.
        // Flush them independently, dropping each sender as soon as it finishes
        // so that its consumer sees EOF without waiting for the other replicas.
        join_all(self.senders.into_iter().zip(self.overflow).map(
            |(sender, overflow)| async move {
                if let Some(tx) = sender {
                    for item in overflow {
                        if tx.send(item).await.is_err() {
                            break;
                        }
                    }
                }
            },
        ))
        .await;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::disk_manager::DiskManagerBuilder;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    use super::*;

    #[tokio::test]
    async fn replicas_end_with_the_error_when_the_buffer_fails() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        // The batch is written to disk under the bounded pool, and the temporary files
        // exceed the size that they are allowed to have when the buffer is finished.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(1024 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(1024),
            )
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut stream = MemoryStream::new(2, 2, vec![], Some((context, Arc::clone(&schema))));
        let mut sink = stream
            .publish()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let replicas = (0..2)
            .map(|_| stream.subscribe())
            .collect::<ExecutionResult<Vec<_>>>()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..1024 * 1024))],
        )?;
        assert_eq!(sink.write(batch).await?, TaskStreamWriteState::Active);
        assert!(sink.commit().await.is_err());
        for mut replica in replicas {
            let item = replica.next().await;
            assert!(matches!(item, Some(Err(_))), "{item:?}");
        }
        Ok(())
    }
}
