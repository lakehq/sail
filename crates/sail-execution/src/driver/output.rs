use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, mem};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::SelectAll;
use futures::{Stream, StreamExt};
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::{DriverActor, DriverMessage};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;

pub struct JobOutputHandle {
    sender: mpsc::Sender<JobOutputItem>,
}

impl fmt::Debug for JobOutputHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobOutputHandle").finish()
    }
}

impl JobOutputHandle {
    pub async fn send(self, item: JobOutputItem) {
        // We ignore the error here because it indicates that the job output
        // consumer has been dropped.
        let _ = self.sender.send(item).await;
    }
}

pub struct JobOutputManager {
    sender: mpsc::Sender<JobOutputItem>,
}

impl JobOutputManager {
    pub fn handle(&self) -> JobOutputHandle {
        JobOutputHandle {
            sender: self.sender.clone(),
        }
    }
}

pub enum JobOutputItem {
    Stream {
        key: TaskStreamKey,
        stream: TaskStreamSource,
    },
    Error {
        cause: CommonErrorCause,
    },
}

/// Why the job output stopped being consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutputOutcome {
    Completed,
    Failed,
    Canceled,
}

impl JobOutputItem {
    const CHANNEL_SIZE: usize = 32;
}

struct JobOutputStream {
    state: JobOutputState,
    /// Keep output history after completed streams have been removed from `SelectAll`.
    emitted: HashSet<OutputPartition>,
    /// Failed attempts may be replaced before they emit any output.
    incomplete: HashMap<OutputPartition, TaskStreamError>,
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputItem>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
            emitted: HashSet::new(),
            incomplete: HashMap::new(),
        }
    }
}

pub fn build_job_output(
    ctx: &mut ActorContext<DriverActor>,
    job_id: JobId,
    schema: SchemaRef,
) -> (JobOutputManager, SendableRecordBatchStream) {
    let (sender, receiver) = mpsc::channel(JobOutputItem::CHANNEL_SIZE);
    let (tx, rx) = mpsc::channel(1);
    let handle = ctx.handle().clone();
    ctx.spawn(async move {
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx).await;
        // Output errors and consumer cancellation can precede terminal task updates.
        // Tell the scheduler why output ended so cleanup records the correct job status.
        // Keep `tx` alive until cleanup has been sent, before signaling EOF to the client.
        let _ = handle
            .send(DriverMessage::CleanUpJob { job_id, outcome })
            .await;
    });
    (
        JobOutputManager { sender },
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            ReceiverStream::new(rx),
        )),
    )
}

async fn forward_job_output(
    mut stream: JobOutputStream,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> JobOutputOutcome {
    loop {
        let next = tokio::select! {
            biased;
            x = stream.next() => x,
            _ = sender.closed() => return JobOutputOutcome::Canceled,
        };
        let Some(batch) = next else {
            return JobOutputOutcome::Completed;
        };
        // Preserve an observed failure even if the consumer disconnects before
        // receiving the error. No more output can be delivered after a failure.
        let failed = batch.is_err();
        let sent = sender.send(batch).await.is_ok();
        if failed {
            return JobOutputOutcome::Failed;
        }
        if !sent {
            return JobOutputOutcome::Canceled;
        }
    }
}

enum JobOutputState {
    Active {
        receiver: mpsc::Receiver<JobOutputItem>,
        inner: Pin<Box<SelectAll<TaskStreamWrapper>>>,
    },
    Draining {
        inner: Pin<Box<SelectAll<TaskStreamWrapper>>>,
    },
    Modifying,
    Completed,
    Failed,
}

// If the task fails, the consumer of the job output will receive an error
// from either the stream ("data plane") or the fail action ("control plane").
// We cannot guarantee which error will be received. Fortunately, the error
// will appear to be the same to the consumer, since they are standardized
// via `CommonErrorCause`.

impl Stream for JobOutputStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = mem::replace(&mut self.state, JobOutputState::Modifying);
        match state {
            JobOutputState::Active {
                mut receiver,
                mut inner,
            } => match receiver.poll_recv(cx) {
                Poll::Pending => {
                    self.state = JobOutputState::Active { receiver, inner };
                }
                Poll::Ready(Some(JobOutputItem::Error { cause })) => {
                    self.state = JobOutputState::Failed;
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                        TaskStreamError::from(cause),
                    )))));
                }
                Poll::Ready(Some(JobOutputItem::Stream { key, stream })) => {
                    if self.emitted.contains(&OutputPartition::from(&key)) {
                        self.state = JobOutputState::Failed;
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                            TaskStreamError::Unknown(format!(
                                "cannot add stream for {}: a different attempt has already produced job output",
                                TaskStreamKeyDisplay(&key)
                            )),
                        )))));
                    }
                    self.incomplete.remove(&OutputPartition::from(&key));
                    inner.iter_mut().for_each(|s| s.mute_if_needed(&key));
                    inner.push(TaskStreamWrapper::new(key, stream));
                    self.state = JobOutputState::Active { receiver, inner };
                    // Receiving an item does not register the receiver's waker. Even if
                    // the new stream is immediately empty, poll again for queued items
                    // or channel closure before waiting for further notifications.
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(None) => {
                    self.state = JobOutputState::Draining { inner };
                }
            },
            _ => {
                self.state = state;
            }
        }
        let poll = match &mut self.state {
            JobOutputState::Active { inner, receiver: _ } => {
                match inner.as_mut().poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => {
                        // We return pending even if all the existing streams are done,
                        // because new streams may still be added.
                        Poll::Pending
                    }
                    Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
                }
            }
            JobOutputState::Modifying => Poll::Pending,
            JobOutputState::Draining { inner } => match inner.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.state = JobOutputState::Completed;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            },
            JobOutputState::Completed | JobOutputState::Failed => Poll::Ready(None),
        };
        // Only producer termination guarantees a control-plane update/replacement.
        // Reader/connect/decode failures must terminate output so cleanup can release
        // a healthy producer blocked on an unread stream. A canceled attempt must
        // still not race the original producer cause.
        let poll = match poll {
            Poll::Ready(Some((partition, Err(error))))
                if error.is_retryable_producer_failure()
                    && (!self.emitted.contains(&partition)
                        || matches!(error, TaskStreamError::ProducerDropped)) =>
            {
                self.incomplete.insert(partition, error);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(None)
                if matches!(self.state, JobOutputState::Completed)
                    && !self.incomplete.is_empty() =>
            {
                self.state = JobOutputState::Failed;
                let error = mem::take(&mut self.incomplete)
                    .into_values()
                    .max_by_key(|error| !matches!(error, TaskStreamError::ProducerDropped))
                    .unwrap_or(TaskStreamError::ProducerDropped);
                return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
            }
            poll => poll,
        };
        poll.map(|item| {
            item.map(|(partition, result)| {
                if result.as_ref().is_ok_and(|batch| batch.num_rows() > 0) {
                    self.emitted.insert(partition);
                }
                result.map_err(|e| DataFusionError::External(Box::new(e)))
            })
        })
    }
}

/// Identifies an output channel across task attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OutputPartition {
    job_id: JobId,
    stage: usize,
    partition: usize,
    channel: usize,
}

impl From<&TaskStreamKey> for OutputPartition {
    fn from(key: &TaskStreamKey) -> Self {
        Self {
            job_id: key.job_id,
            stage: key.stage,
            partition: key.partition,
            channel: key.channel,
        }
    }
}

struct TaskStreamWrapper {
    key: TaskStreamKey,
    inner: Option<TaskStreamSource>,
}

impl TaskStreamWrapper {
    fn new(key: TaskStreamKey, inner: TaskStreamSource) -> Self {
        Self {
            key,
            inner: Some(inner),
        }
    }

    fn mute_if_needed(&mut self, key: &TaskStreamKey) {
        if self.key.job_id == key.job_id
            && self.key.stage == key.stage
            && self.key.partition == key.partition
            && self.key.channel == key.channel
        {
            self.inner = None;
        }
    }
}

impl Stream for TaskStreamWrapper {
    type Item = (OutputPartition, TaskStreamResult<RecordBatch>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = &mut self.inner else {
            return Poll::Ready(None);
        };
        let poll = inner.as_mut().poll_next(cx);
        poll.map(|item| item.map(|result| (OutputPartition::from(&self.key), result)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use futures::task::{ArcWake, waker};

    use super::*;
    use crate::driver::job_scheduler::JobState;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn empty_stream_does_not_hide_channel_closure() {
        let (sender, receiver) = mpsc::channel(2);
        assert!(sender.try_send(empty_stream()).is_ok());
        drop(sender);
        let mut output = JobOutputStream::new(receiver);
        let counter = Arc::new(WakeCounter::default());
        let waker = waker(counter.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(counter.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn empty_stream_does_not_hide_queued_failure() {
        let (sender, receiver) = mpsc::channel(2);
        assert!(sender.try_send(empty_stream()).is_ok());
        assert!(
            sender
                .try_send(JobOutputItem::Error {
                    cause: CommonErrorCause::Execution("test failure".to_string()),
                })
                .is_ok()
        );
        let mut output = JobOutputStream::new(receiver);
        let counter = Arc::new(WakeCounter::default());
        let waker = waker(counter.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(counter.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
    }

    #[test]
    #[expect(clippy::panic)]
    fn retryable_attempt_waits_for_failure_or_replacement_but_never_succeeds() {
        use sail_python_udf::error::PyErrExtractor;

        for (outcome, error) in (0..3).flat_map(|outcome| {
            [
                (outcome, TaskStreamError::ProducerDropped),
                (
                    outcome,
                    TaskStreamError::from(CommonErrorCause::Execution("transient failure".into()))
                        .into_producer_failure(),
                ),
            ]
        }) {
            let (sender, receiver) = mpsc::channel(2);
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream: Box::pin(futures::stream::iter([Err(error)])),
                    })
                    .is_ok()
            );
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            match outcome {
                0 => {
                    assert!(
                        sender
                            .try_send(JobOutputItem::Error {
                                cause: CommonErrorCause::InvalidArgument(
                                    "original producer error".into()
                                ),
                            })
                            .is_ok()
                    );
                    let Poll::Ready(Some(Err(error))) = Pin::new(&mut output).poll_next(&mut cx)
                    else {
                        panic!("expected original error");
                    };
                    assert!(matches!(CommonErrorCause::new::<PyErrExtractor>(&error),
                        CommonErrorCause::InvalidArgument(s) if s == "original producer error"));
                    assert!(matches!(
                        Pin::new(&mut output).poll_next(&mut cx),
                        Poll::Ready(None)
                    ));
                }
                1 => {
                    assert!(
                        sender
                            .try_send(JobOutputItem::Stream {
                                key: TaskStreamKey {
                                    attempt: 1,
                                    ..stream_key()
                                },
                                stream: Box::pin(futures::stream::empty()),
                            })
                            .is_ok()
                    );
                    drop(sender);
                    assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
                    assert!(matches!(
                        Pin::new(&mut output).poll_next(&mut cx),
                        Poll::Ready(None)
                    ));
                }
                _ => {
                    drop(sender);
                    assert!(matches!(
                        Pin::new(&mut output).poll_next(&mut cx),
                        Poll::Ready(Some(Err(_)))
                    ));
                    assert!(matches!(
                        Pin::new(&mut output).poll_next(&mut cx),
                        Poll::Ready(None)
                    ));
                }
            }
        }
    }

    #[tokio::test]
    async fn reader_failure_terminates_with_live_sender_and_blocked_producer()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::time::Duration;

        use futures::FutureExt;
        use sail_common::config::AppConfig;
        use sail_common::runtime::RuntimeHandle;
        use tokio::time::timeout;

        use crate::driver::DriverOptions;
        use crate::stream::local::{LocalStreamManager, LocalStreamManagerOptions};

        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        let mut config = AppConfig::load()?;
        config.cluster.task_stream_buffer = 1;
        let runtime = tokio::runtime::Handle::current();
        let options = DriverOptions::try_new(
            &config,
            RuntimeHandle::new(runtime.clone(), runtime),
            "reader-failure".into(),
            1.into(),
            0,
        )?;
        let mut streams = LocalStreamManager::new(LocalStreamManagerOptions::from(&options));
        let mut sink = streams.create_stream(stream_key(), 1, batch.schema())?;
        sink.write(batch.clone()).await?;
        sink.write(batch).await?;
        // The queue has no subscriber, and its second batch cannot be committed.
        let mut commit = sink.commit();
        assert!((&mut commit).now_or_never().is_none());

        let (sender, receiver) = mpsc::channel(1);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Err(TaskStreamError::from(
                        tonic::Status::unavailable("fetch failed before subscription"),
                    ))])),
                })
                .is_ok()
        );
        let (tx, mut rx) = mpsc::channel(1);
        let outcome = timeout(
            Duration::from_secs(1),
            forward_job_output(JobOutputStream::new(receiver), &tx),
        )
        .await?;
        assert_eq!(outcome, JobOutputOutcome::Failed);
        assert!(matches!(rx.recv().await, Some(Err(_))));
        // Neither sender closure nor producer completion caused the output to finish.
        assert!(sender.is_closed());
        assert!((&mut commit).now_or_never().is_none());
        streams.remove_streams(stream_key().job_id, None);
        timeout(Duration::from_secs(1), commit).await??;
        Ok(())
    }

    #[tokio::test]
    async fn producer_failure_after_output_cannot_retry()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use sail_python_udf::error::PyErrExtractor;

        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        let (sender, receiver) = mpsc::channel(1);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([
                        Ok(batch.clone()),
                        Err(
                            TaskStreamError::from(CommonErrorCause::Io("producer failed".into()))
                                .into_producer_failure()
                        ),
                    ])),
                })
                .is_ok()
        );
        let mut output = JobOutputStream::new(receiver);
        assert_eq!(output.next().await.transpose()?, Some(batch));
        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(
            forward_job_output(output, &tx).await,
            JobOutputOutcome::Failed
        );
        let Some(Err(error)) = rx.recv().await else {
            return Err("missing producer error".into());
        };
        assert!(matches!(CommonErrorCause::new::<PyErrExtractor>(&error),
            CommonErrorCause::Io(s) if s == "producer failed"));
        assert!(sender.is_closed());
        Ok(())
    }

    #[test]
    fn completed_attempt_output_rejects_retry() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        let (sender, receiver) = mpsc::channel(1);
        let mut output = JobOutputStream::new(receiver);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                })
                .is_ok()
        );
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Ok(actual))) if actual == batch
        ));

        // Poll through EOF so the first attempt's wrapper is removed before retrying.
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(matches!(
            &output.state,
            JobOutputState::Active { inner, .. } if inner.is_empty()
        ));

        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: TaskStreamKey {
                        attempt: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::iter([Ok(batch)])),
                })
                .is_ok()
        );
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Err(error))) if error.to_string().contains(
                "a different attempt has already produced job output"
            )
        ));
        Ok(())
    }

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn semantic_failure_preserves_cause_after_another_partition_emits() -> Result<()> {
        use sail_python_udf::error::PyErrExtractor;

        let expected = "SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row";
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for control_plane in [false, true] {
            let (sender, receiver) = mpsc::channel(2);
            let mut stream = JobOutputStream::new(receiver);
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                    })
                    .is_ok(),
                "output channel has capacity"
            );
            assert_eq!(stream.next().await.transpose()?, Some(batch.clone()));

            let cause = CommonErrorCause::NonRetryableExecution(expected.into());
            let failure = if control_plane {
                JobOutputItem::Error { cause }
            } else {
                JobOutputItem::Stream {
                    key: TaskStreamKey {
                        partition: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::iter([Err(TaskStreamError::from(cause))])),
                }
            };
            assert!(
                sender.try_send(failure).is_ok(),
                "output channel has capacity"
            );
            let (tx, mut rx) = mpsc::channel(2);
            assert_eq!(
                forward_job_output(stream, &tx).await,
                JobOutputOutcome::Failed
            );
            drop(tx);
            let error = rx
                .recv()
                .await
                .expect("terminal error")
                .expect_err("must fail");
            assert!(matches!(
                CommonErrorCause::new::<PyErrExtractor>(&error),
                CommonErrorCause::NonRetryableExecution(x) if x == expected
            ));
            assert!(
                rx.recv().await.is_none(),
                "no batches may follow the failure"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn output_conflict_fails_running_and_draining_jobs() {
        for disconnected in [false, true] {
            for mut state in active_job_states() {
                let (sender, receiver) = mpsc::channel(1);
                let key = TaskStreamKey {
                    attempt: 1,
                    ..stream_key()
                };
                let mut stream = JobOutputStream::new(receiver);
                // An earlier attempt has already emitted output for this partition.
                stream.emitted.insert(OutputPartition::from(&key));
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key,
                            stream: Box::pin(futures::stream::empty()),
                        })
                        .is_ok()
                );
                let (tx, mut rx) = mpsc::channel(1);
                if disconnected {
                    rx.close();
                }

                let outcome = forward_job_output(stream, &tx).await;
                state.finish_output(outcome);

                assert_eq!(outcome, JobOutputOutcome::Failed);
                assert_eq!(state.status(), "FAILED");
                if !disconnected {
                    assert!(matches!(
                        rx.recv().await,
                        Some(Err(error)) if error.to_string().contains(
                            "a different attempt has already produced job output"
                        )
                    ));
                }
            }
        }
    }

    #[tokio::test]
    async fn task_stream_error_fails_running_and_draining_jobs() {
        for mut state in active_job_states() {
            let (sender, receiver) = mpsc::channel(1);
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream: Box::pin(futures::stream::iter([Err(TaskStreamError::Unknown(
                            "test stream failure".to_string(),
                        ))])),
                    })
                    .is_ok()
            );
            let (tx, mut rx) = mpsc::channel(1);

            // Reader failure must not depend on channel closure or task failure.
            let outcome = forward_job_output(JobOutputStream::new(receiver), &tx).await;
            assert!(sender.is_closed());
            state.finish_output(outcome);

            assert_eq!(state.status(), "FAILED");
            assert!(matches!(
                rx.recv().await,
                Some(Err(error)) if error.to_string().contains("test stream failure")
            ));
        }
    }

    #[tokio::test]
    async fn consumer_disconnection_cancels_running_and_draining_jobs() {
        for mut state in active_job_states() {
            let (_sender, receiver) = mpsc::channel(1);
            let (tx, rx) = mpsc::channel(1);
            drop(rx);

            let outcome = forward_job_output(JobOutputStream::new(receiver), &tx).await;
            state.finish_output(outcome);

            assert_eq!(outcome, JobOutputOutcome::Canceled);
            assert_eq!(state.status(), "CANCELED");
        }
    }

    #[tokio::test]
    async fn drained_output_completes_job() {
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        let (tx, _rx) = mpsc::channel(1);
        let mut state = JobState::Draining;

        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx).await;
        state.finish_output(outcome);

        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert_eq!(state.status(), "SUCCEEDED");
    }

    #[test]
    fn output_cleanup_preserves_terminal_job_states() {
        for mut state in [JobState::Failed, JobState::Canceled, JobState::Succeeded] {
            let status = state.status();
            for outcome in [
                JobOutputOutcome::Completed,
                JobOutputOutcome::Failed,
                JobOutputOutcome::Canceled,
            ] {
                state.finish_output(outcome);
                assert_eq!(state.status(), status);
            }
        }
    }

    fn active_job_states() -> [JobState; 2] {
        let (sender, _receiver) = mpsc::channel(1);
        [
            JobState::Running {
                output: JobOutputManager { sender },
            },
            JobState::Draining,
        ]
    }

    fn empty_stream() -> JobOutputItem {
        JobOutputItem::Stream {
            key: stream_key(),
            stream: Box::pin(futures::stream::empty()),
        }
    }

    fn stream_key() -> TaskStreamKey {
        TaskStreamKey {
            job_id: JobId::from(1),
            stage: 0,
            partition: 0,
            attempt: 0,
            channel: 0,
        }
    }
}
