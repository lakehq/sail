use std::collections::VecDeque;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::StreamExt;
use futures::future::join_all;
use log::debug;
use tokio::sync::{mpsc, oneshot};
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
    sender: Option<MemoryStreamReplicaSender>,
    receivers: Vec<TaskStreamSource>,
}

impl MemoryStream {
    pub fn new(buffer: usize, replicas: usize, senders: Vec<MemoryStreamSender>) -> Self {
        let replicas = replicas.max(senders.len());
        let diff = replicas - senders.len();
        let mut senders = senders.into_iter().map(Some).collect::<Vec<_>>();
        senders.reserve(diff);
        let mut receivers = Vec::with_capacity(diff);
        for _ in 0..diff {
            let (tx, rx) = memory_stream_channel(buffer);
            senders.push(Some(tx));
            receivers.push(rx);
        }
        let overflow = vec![VecDeque::new(); senders.len()];
        Self {
            sender: Some(MemoryStreamReplicaSender { senders, overflow }),
            receivers,
        }
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(sender))
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(rx)
    }
}

/// The completion channel is independent of batch backpressure. Dropping an
/// unfinished producer must not turn a truncated stream into successful EOF.
pub(crate) struct MemoryStreamSender {
    batches: mpsc::Sender<RecordBatch>,
    completion: oneshot::Sender<TaskStreamResult<()>>,
}

impl MemoryStreamSender {
    pub(super) fn finish(self, result: TaskStreamResult<()>) {
        let _ = self.completion.send(result);
    }
}

pub(super) fn memory_stream_channel(buffer: usize) -> (MemoryStreamSender, TaskStreamSource) {
    let (batches, rx) = mpsc::channel(buffer);
    let (completion, terminal) = oneshot::channel();
    let end = futures::stream::once(async move {
        match terminal.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(TaskStreamError::ProducerDropped)),
        }
    })
    .filter_map(futures::future::ready);
    let stream = ReceiverStream::new(rx).map(Ok).chain(end);
    (
        MemoryStreamSender {
            batches,
            completion,
        },
        Box::pin(stream),
    )
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<MemoryStreamSender>>,
    /// An overflow buffer for each sender to avoid blocking sending for slow senders.
    /// This also avoids deadlock situations where the task stream buffer size is small.
    // TODO: More investigation is needed to understand deadlocks during writes when
    //   task stream buffers are bounded, and whether unbounded overflow can be avoided.
    //   Concurrent finalization fixes commit deadlocks but does not remove this need.
    overflow: Vec<VecDeque<RecordBatch>>,
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
                    match tx.batches.try_send(item) {
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
                    match tx.batches.try_send(batch.clone()) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_back(x);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                        }
                    }
                } else {
                    overflow.push_back(batch.clone());
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
                        if tx.batches.send(item).await.is_err() {
                            break;
                        }
                    }
                    tx.finish(Ok(()));
                }
            },
        ))
        .await;
        Ok(())
    }

    async fn abort(self: Box<Self>, error: TaskStreamError) -> Result<()> {
        let error = error.into_producer_failure();
        for sender in self.senders.into_iter().flatten() {
            sender.finish(Err(error.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::common::DataFusionError;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::test::exec::{ErrorExec, MockExec};
    use datafusion::physical_plan::union::UnionExec;
    use futures::{FutureExt, TryStreamExt};
    use sail_common_datafusion::error::CommonErrorCause;
    use sail_python_udf::error::PyErrExtractor;
    use tokio::time::timeout;

    use super::*;
    use crate::plan::{ShufflePartitioning, ShuffleWriteExec};
    use crate::stream::writer::{MultiChannelTaskStreamSink, TaskStreamSink, TaskStreamWriter};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)])
            .expect("valid batch")
    }

    #[tokio::test]
    async fn producer_abort_is_not_successful_eof() -> TestResult {
        for batches in [0, 1, 3] {
            for pending in [false, true] {
                let (sender, mut source) = memory_stream_channel(1);
                let mut stream = MemoryStream::new(1, 2, vec![sender]);
                let mut sink = stream.publish()?;
                for _ in 0..batches {
                    sink.write(batch()).await?;
                }
                // No scheduler notification or available buffer capacity is required.
                timeout(
                    Duration::from_secs(5),
                    sink.abort(CommonErrorCause::Io("missing file".into()).into()),
                )
                .await??;
                if !pending {
                    source = stream.subscribe()?;
                }
                let items: Vec<_> = source.collect().await;
                assert_eq!(items.len(), usize::from(batches > 0) + 1);
                assert!(items[..items.len() - 1].iter().all(Result::is_ok));
                let error = items
                    .into_iter()
                    .last()
                    .expect("terminal error")
                    .expect_err("not EOF");
                assert!(
                    matches!(CommonErrorCause::from(error), CommonErrorCause::Io(s) if s == "missing file")
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn drop_and_cancel_do_not_complete_a_stream() -> TestResult {
        for batches in [0, 3] {
            let mut stream = MemoryStream::new(1, 1, vec![]);
            let mut sink = stream.publish()?;
            for _ in 0..batches {
                sink.write(batch()).await?;
            }
            drop(sink);
            assert!(stream.subscribe()?.try_collect::<Vec<_>>().await.is_err());
        }
        let mut stream = MemoryStream::new(1, 1, vec![]);
        let mut sink = stream.publish()?;
        sink.write(batch()).await?;
        sink.write(batch()).await?;
        let mut commit = sink.commit();
        assert!((&mut commit).now_or_never().is_none());
        drop(commit);
        assert!(stream.subscribe()?.try_collect::<Vec<_>>().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn successful_completion_flushes_replicas_independently() -> TestResult {
        for batches in [0, 1, 3] {
            let mut stream = MemoryStream::new(1, 2, vec![]);
            let mut sink = stream.publish()?;
            let mut source = stream.subscribe()?;
            assert!(source.next().now_or_never().is_none());
            for _ in 0..batches {
                sink.write(batch()).await?;
            }
            let commit = tokio::spawn(sink.commit());
            let items = timeout(Duration::from_secs(5), source.try_collect::<Vec<_>>()).await??;
            assert_eq!(items, vec![batch(); batches]);
            // The other replica has not been consumed, and cannot delay this one's EOF.
            if batches > 1 {
                assert!(!commit.is_finished());
            }
            drop(stream);
            timeout(Duration::from_secs(5), commit).await???;
        }
        Ok(())
    }

    #[tokio::test]
    async fn consumer_early_stop_does_not_fail_other_replicas() -> TestResult {
        let mut stream = MemoryStream::new(1, 2, vec![]);
        let mut sink = stream.publish()?;
        let source = stream.subscribe()?;
        drop(stream.subscribe()?);
        assert_eq!(sink.write(batch()).await?, TaskStreamWriteState::Active);
        sink.commit().await?;
        assert_eq!(source.try_collect::<Vec<_>>().await?, vec![batch()]);

        let mut stream = MemoryStream::new(1, 1, vec![]);
        let mut sink = stream.publish()?;
        drop(stream.subscribe()?);
        assert_eq!(sink.write(batch()).await?, TaskStreamWriteState::Closed);
        sink.commit().await?;
        Ok(())
    }

    struct TestWriter(Mutex<Option<Box<dyn TaskStreamSink>>>);

    impl std::fmt::Debug for TestWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("TestWriter")
        }
    }

    #[tonic::async_trait]
    impl TaskStreamWriter for TestWriter {
        async fn open(&self, _partition: usize) -> Result<Box<dyn TaskStreamSink>> {
            Ok(self
                .0
                .lock()
                .expect("writer lock")
                .take()
                .expect("one writer"))
        }
    }

    #[tokio::test]
    async fn failed_shuffle_cannot_pass_an_eof_commit_barrier() -> TestResult {
        for batches in [0, 1] {
            let mut data: Vec<_> = (0..batches).map(|_| Ok(batch())).collect();
            data.push(Err(DataFusionError::Execution(
                "injected input failure".into(),
            )));
            let input: Arc<dyn ExecutionPlan> = Arc::new(
                MockExec::new(data, batch().schema())
                    .with_use_task(false)
                    .with_unknown_statistics(),
            );
            // Equivalent upstream multi-partition coalesce must also propagate the cause.
            let reference = CoalescePartitionsExec::new(UnionExec::try_new(vec![
                input.clone(),
                Arc::new(EmptyExec::new(batch().schema())),
            ])?);
            let reference_error = reference
                .execute(0, Arc::new(TaskContext::default()))?
                .try_collect::<Vec<_>>()
                .await
                .expect_err("reference must fail");
            assert!(
                matches!(CommonErrorCause::new::<PyErrExtractor>(&reference_error),
                CommonErrorCause::Execution(s) if s == "injected input failure")
            );

            let mut stream = MemoryStream::new(1, 1, vec![]);
            let writer = Arc::new(TestWriter(Mutex::new(Some(Box::new(
                MultiChannelTaskStreamSink {
                    sinks: vec![Some(stream.publish()?)],
                },
            )))));
            let plan =
                ShuffleWriteExec::new(input, writer, ShufflePartitioning::RoundRobinBatch(1));
            let producer_error = plan
                .execute(0, Arc::new(TaskContext::default()))?
                .try_collect::<Vec<_>>()
                .await
                .expect_err("producer must fail");
            assert!(
                matches!(CommonErrorCause::new::<PyErrExtractor>(&producer_error),
                CommonErrorCause::Execution(s) if s == "injected input failure")
            );
            // Poll the consumer only after producer failure, with no control plane running.
            // Like a mutation coordinator, commit is reachable only after successful EOF.
            let mut committed = false;
            let result = async {
                let mut source = stream.subscribe()?;
                while source.try_next().await?.is_some() {}
                committed = true;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            assert!(result.is_err());
            assert!(!committed);
        }
        Ok(())
    }

    #[tokio::test]
    async fn input_stream_creation_error_reaches_the_consumer() -> TestResult {
        let mut stream = MemoryStream::new(1, 1, vec![]);
        let writer = Arc::new(TestWriter(Mutex::new(Some(Box::new(
            MultiChannelTaskStreamSink {
                sinks: vec![Some(stream.publish()?)],
            },
        )))));
        let plan = ShuffleWriteExec::new(
            Arc::new(ErrorExec::new()),
            writer,
            ShufflePartitioning::RoundRobinBatch(1),
        );
        let error = plan
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await
            .expect_err("input creation must fail");
        let consumer_error = stream
            .subscribe()?
            .try_next()
            .await
            .expect_err("failure, not EOF");
        let expected = CommonErrorCause::new::<PyErrExtractor>(&error);
        assert!(matches!(expected, CommonErrorCause::Internal(_)));
        assert_eq!(
            serde_json::to_value(expected)?,
            serde_json::to_value(CommonErrorCause::from(consumer_error))?
        );
        Ok(())
    }

    #[tokio::test]
    async fn flight_preserves_memory_stream_completion_and_causes() -> TestResult {
        use std::num::NonZeroUsize;

        use arrow_flight::flight_service_server::FlightServiceServer;
        use sail_common_datafusion::error::PythonErrorCause;
        use tokio::net::TcpListener;
        use tokio_util::task::AbortOnDropHandle;
        use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;

        use crate::id::TaskStreamKey;
        use crate::rpc::ClientOptions;
        use crate::shuffle::ShuffleCompression;
        use crate::stream::service::{
            TaskStreamFetcher, TaskStreamFlightClient, TaskStreamFlightServer, TaskStreamOwner,
        };

        struct Fetcher(Mutex<VecDeque<TaskStreamSource>>);
        #[tonic::async_trait]
        impl TaskStreamFetcher<TaskStreamKey> for Arc<Fetcher> {
            async fn fetch(
                &self,
                _key: TaskStreamKey,
                sender: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
            ) -> ExecutionResult<()> {
                let source = self
                    .0
                    .lock()
                    .expect("fetcher lock")
                    .pop_front()
                    .expect("one source per request");
                let _ = sender.send(Ok(source));
                Ok(())
            }
        }

        timeout(Duration::from_secs(15), async {
            let fetcher = Arc::new(Fetcher(Mutex::new(VecDeque::new())));
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let service =
                TaskStreamFlightServer::new(Box::new(fetcher.clone()), ShuffleCompression::None);
            let _server = AbortOnDropHandle::new(tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(FlightServiceServer::new(service))
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));
            let client = TaskStreamFlightClient::new(
                ClientOptions {
                    enable_tls: false,
                    host: address.ip().to_string(),
                    port: address.port(),
                    flight_connection_count: NonZeroUsize::MIN,
                    flight_initial_stream_window_size: None,
                    flight_initial_connection_window_size: None,
                },
                TaskStreamOwner::Worker,
            );
            let key = TaskStreamKey {
                job_id: 1.into(),
                stage: 0,
                partition: 0,
                attempt: 0,
                channel: 0,
            };
            let causes = [
                None,
                Some(CommonErrorCause::Io("missing file".into())),
                Some(CommonErrorCause::FormatCsv("malformed CSV".into())),
                Some(CommonErrorCause::Python(PythonErrorCause {
                    summary: "row mismatch".into(),
                    traceback: Some(vec!["traceback line".into()]),
                })),
                Some(CommonErrorCause::NonRetryableExecution(
                    "deterministic failure".into(),
                )),
            ];
            for cause in causes {
                for after_batch in [false, true] {
                    let mut memory = MemoryStream::new(1, 1, vec![]);
                    let mut sink = memory.publish()?;
                    fetcher
                        .0
                        .lock()
                        .expect("fetcher lock")
                        .push_back(memory.subscribe()?);
                    let mut source = client
                        .fetch_task_stream(key.clone(), batch().schema())
                        .await?;
                    if after_batch {
                        sink.write(batch()).await?;
                        assert_eq!(source.try_next().await?, Some(batch()));
                    }
                    if let Some(cause) = &cause {
                        sink.abort(cause.clone().into()).await?;
                        let error = source
                            .try_next()
                            .await
                            .expect_err("remote failure, not EOF");
                        assert!(matches!(error, TaskStreamError::ProducerFailed(_)));
                        assert_eq!(
                            serde_json::to_value(CommonErrorCause::from(error))?,
                            serde_json::to_value(cause)?
                        );
                    } else {
                        sink.commit().await?;
                        assert!(source.try_next().await?.is_none());
                    }
                }
            }
            // Cancellation or panic drops the writer without an explicit terminal.
            // The same connection remains usable after the failed stream.
            for after_batch in [false, true] {
                let mut memory = MemoryStream::new(1, 1, vec![]);
                let mut sink = memory.publish()?;
                fetcher
                    .0
                    .lock()
                    .expect("fetcher lock")
                    .push_back(memory.subscribe()?);
                let mut source = client
                    .fetch_task_stream(key.clone(), batch().schema())
                    .await?;
                if after_batch {
                    sink.write(batch()).await?;
                    assert_eq!(source.try_next().await?, Some(batch()));
                }
                drop(sink);
                let error = source
                    .try_next()
                    .await
                    .expect_err("dropped producer is not EOF");
                assert!(matches!(error, TaskStreamError::ProducerDropped));
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await?
    }
}
