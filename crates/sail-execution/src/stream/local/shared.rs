use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_physical_plan::shared_cte::tee::{Tee, TeeReader};
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::{Notify, mpsc};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::ExecutionResult;
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// Whether a replica of a shared stream asks for a batch that no replica has read yet.
#[derive(Default)]
struct Demand {
    requested: AtomicBool,
    notify: Notify,
}

/// A stream that is read by each of its replicas at its own pace. The task that writes
/// the stream produces a batch only when a replica asks for a batch that no replica has
/// read yet, and stops when every replica has finished or is dropped.
pub(crate) struct SharedStream {
    tee: Arc<Tee>,
    /// The number of replicas that are read.
    subscribed: usize,
    /// The number of batches that each replica reads ahead.
    buffer: usize,
}

impl SharedStream {
    /// Creates the stream and its sink, for the given number of replicas, and sends each
    /// replica that is already subscribed to its sender.
    pub fn try_new(
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        buffer: usize,
        context: &TaskContext,
        schema: SchemaRef,
    ) -> ExecutionResult<(Self, Box<dyn TaskStreamChannelSink>)> {
        let replicas = replicas.max(senders.len());
        let demand = Arc::new(Demand::default());
        let (tx, rx) = mpsc::channel(1);
        let source = {
            let demand = Arc::clone(&demand);
            let schema = Arc::clone(&schema);
            Box::new(move || Ok(demand_stream(demand, rx, schema))) as _
        };
        let tee = Arc::new(Tee::new(
            "SharedStream".to_string(),
            schema,
            source,
            replicas,
            false,
            context,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        ));
        let mut stream = Self {
            tee,
            subscribed: 0,
            buffer,
        };
        for tx in senders {
            stream.forward(tx)?;
        }
        let sink = SharedStreamSender { demand, tx };
        Ok((stream, Box::new(sink)))
    }

    fn reader(&mut self) -> ExecutionResult<TaskStreamSource> {
        let reader: TeeReader = self.tee.reader(self.subscribed)?;
        self.subscribed += 1;
        Ok(Box::pin(reader.map(|item| item.map_err(task_stream_error))))
    }

    fn forward(&mut self, tx: mpsc::Sender<TaskStreamResult<RecordBatch>>) -> ExecutionResult<()> {
        let mut reader = self.reader()?;
        tokio::spawn(async move {
            // A batch is read only when the replica has room for it.
            while let Ok(permit) = tx.reserve().await {
                tokio::select! {
                    item = reader.next() => match item {
                        Some(item) => permit.send(item),
                        None => break,
                    },
                    _ = tx.closed() => break,
                }
            }
        });
        Ok(())
    }

    pub fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let (tx, rx) = mpsc::channel(self.buffer);
        self.forward(tx)?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

/// Returns the stream of batches that the sink receives, which asks the sink for a batch
/// when it is polled and has no batch.
fn demand_stream(
    demand: Arc<Demand>,
    mut rx: mpsc::Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let stream = futures::stream::poll_fn(move |cx| match rx.poll_recv(cx) {
        Poll::Ready(item) => Poll::Ready(item),
        Poll::Pending => {
            demand.requested.store(true, Ordering::SeqCst);
            demand.notify.notify_one();
            Poll::Pending
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Returns the error of a replica, which is the error of the task that writes the stream
/// if the task fails.
fn task_stream_error(error: DataFusionError) -> TaskStreamError {
    let error = match error {
        DataFusionError::Shared(error) => error,
        error => Arc::new(error),
    };
    if let DataFusionError::External(external) = error.as_ref()
        && let Some(error) = external.downcast_ref::<TaskStreamError>()
    {
        return error.clone();
    }
    TaskStreamError::from(CommonErrorCause::new::<PyErrExtractor>(error.as_ref()))
}

struct SharedStreamSender {
    demand: Arc<Demand>,
    tx: mpsc::Sender<Result<RecordBatch>>,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for SharedStreamSender {
    async fn ready(&mut self) -> Result<TaskStreamWriteState> {
        loop {
            if self.tx.is_closed() {
                return Ok(TaskStreamWriteState::Closed);
            }
            // The request is answered by the next batch that is written.
            if self.demand.requested.load(Ordering::SeqCst) {
                return Ok(TaskStreamWriteState::Active);
            }
            tokio::select! {
                _ = self.demand.notify.notified() => {}
                _ = self.tx.closed() => {}
            }
        }
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        // The batch answers the requests made before it is sent.
        self.demand.requested.store(false, Ordering::SeqCst);
        Ok(match self.tx.send(Ok(batch)).await {
            Ok(()) => TaskStreamWriteState::Active,
            Err(_) => TaskStreamWriteState::Closed,
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // The replicas reach the end of the stream when the sender is dropped.
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        // A replica that reads to the end of what is written fails rather than reading a
        // part of the stream as the whole stream.
        let _ = self.tx.try_send(Err(DataFusionError::Execution(
            "the task that writes a shared stream has stopped".to_string(),
        )));
        Ok(())
    }

    async fn fail(self: Box<Self>, error: &DataFusionError) -> Result<bool> {
        // The error is the last item of every replica, so that it fails only the
        // consumers that read the stream to its end, as they would fail if they
        // produced the stream themselves.
        let cause = TaskStreamError::from(CommonErrorCause::new::<PyErrExtractor>(error));
        let _ = self
            .tx
            .send(Err(DataFusionError::External(Box::new(cause))))
            .await;
        Ok(true)
    }
}

/// The sink of a stream whose readers have all finished, which writes nothing.
pub(crate) struct ClosedSink;

#[tonic::async_trait]
impl TaskStreamChannelSink for ClosedSink {
    async fn ready(&mut self) -> Result<TaskStreamWriteState> {
        Ok(TaskStreamWriteState::Closed)
    }

    async fn write(&mut self, _batch: RecordBatch) -> Result<TaskStreamWriteState> {
        Ok(TaskStreamWriteState::Closed)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::FutureExt;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn batch(i: i64) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![i]))]).unwrap()
    }

    fn value(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    async fn is_ready(sink: &mut Box<dyn TaskStreamChannelSink>) -> Option<TaskStreamWriteState> {
        tokio::time::timeout(Duration::from_millis(50), sink.ready())
            .await
            .ok()
            .map(|x| x.unwrap())
    }

    #[tokio::test]
    async fn the_sink_writes_only_when_a_replica_asks_for_a_batch() {
        let context = TaskContext::default();
        let (mut stream, mut sink) =
            SharedStream::try_new(2, vec![], 1, &context, schema()).unwrap();
        // Nothing is asked before a replica reads.
        assert_eq!(is_ready(&mut sink).await, None);
        let mut a = stream.subscribe().unwrap();
        let mut b = stream.subscribe().unwrap();
        assert!(a.next().now_or_never().is_none());
        assert_eq!(
            is_ready(&mut sink).await,
            Some(TaskStreamWriteState::Active)
        );
        sink.write(batch(1)).await.unwrap();
        // The batch is kept for the other replica, which does not ask for a new one,
        // and neither replica asks for more than its buffer holds.
        assert_eq!(is_ready(&mut sink).await, None);
        assert_eq!(value(&a.next().await.unwrap().unwrap()), 1);
        assert_eq!(value(&b.next().await.unwrap().unwrap()), 1);
        drop(a);
        drop(b);
        // The sink stops once every replica is dropped.
        assert_eq!(
            is_ready(&mut sink).await,
            Some(TaskStreamWriteState::Closed)
        );
    }

    #[tokio::test]
    async fn replicas_read_to_the_end_or_to_the_error() {
        let context = TaskContext::default();
        let (mut stream, mut sink) =
            SharedStream::try_new(2, vec![], 1, &context, schema()).unwrap();
        let mut a = stream.subscribe().unwrap();
        let reader = tokio::spawn(async move {
            let mut values = vec![];
            while let Some(item) = a.next().await {
                values.push(item.map(|x| value(&x)));
            }
            values
        });
        for i in 0..3 {
            assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
            sink.write(batch(i)).await.unwrap();
        }
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
        let error = DataFusionError::Execution("body failed".to_string());
        assert!(sink.fail(&error).await.unwrap());
        let values = reader.await.unwrap();
        assert_eq!(values.len(), 4);
        assert!(
            values[..3]
                .iter()
                .zip(0..)
                .all(|(x, i)| x.as_ref().ok() == Some(&i))
        );
        assert!(
            values[3]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("body failed")
        );
        // The replica that subscribes later reads the same rows and the same error.
        let b = stream.subscribe().unwrap();
        let rest = b.collect::<Vec<_>>().await;
        assert_eq!(rest.len(), 4);
        assert!(rest[3].is_err());
    }

    #[tokio::test]
    async fn subscribed_replicas_are_sent_the_stream() {
        let context = TaskContext::default();
        let (tx, mut rx) = mpsc::channel(1);
        let (_stream, mut sink) =
            SharedStream::try_new(1, vec![tx], 1, &context, schema()).unwrap();
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
        sink.write(batch(7)).await.unwrap();
        assert_eq!(value(&rx.recv().await.unwrap().unwrap()), 7);
        sink.commit().await.unwrap();
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn an_aborted_stream_is_an_error_for_its_replicas() {
        let context = TaskContext::default();
        let (mut stream, mut sink) =
            SharedStream::try_new(1, vec![], 1, &context, schema()).unwrap();
        let mut a = stream.subscribe().unwrap();
        let reader = tokio::spawn(async move { a.next().await });
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
        sink.abort().await.unwrap();
        assert!(reader.await.unwrap().unwrap().is_err());
    }
}
