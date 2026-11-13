use std::collections::{HashMap, HashSet};
use std::future::Future;
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
use crate::id::{JobId, TaskKey, TaskStreamKey};
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
    /// The streams that have emitted rows for the first time for their output partition,
    /// which are not yet reported.
    newly_emitted: Vec<TaskStreamKey>,
    /// The errors of the streams that have failed before emitting any rows, which are
    /// returned unless another attempt adds a stream for the output partition.
    failed: HashMap<OutputPartition, TaskStreamError>,
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputItem>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
            emitted: HashSet::new(),
            newly_emitted: vec![],
            failed: HashMap::new(),
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
        let emitted = |key: TaskStreamKey| {
            let handle = handle.clone();
            async move {
                let _ = handle
                    .send(DriverMessage::JobOutputEmitted {
                        key: TaskKey::from(key),
                    })
                    .await;
            }
        };
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, emitted).await;
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

async fn forward_job_output<F, Fut>(
    mut stream: JobOutputStream,
    sender: &mpsc::Sender<Result<RecordBatch>>,
    emitted: F,
) -> JobOutputOutcome
where
    F: Fn(TaskStreamKey) -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        let next = tokio::select! {
            biased;
            x = stream.next() => x,
            _ = sender.closed() => return JobOutputOutcome::Canceled,
        };
        let Some(batch) = next else {
            return JobOutputOutcome::Completed;
        };
        // The scheduler learns that the task region cannot be retried
        // before the rows are delivered.
        for key in mem::take(&mut stream.newly_emitted) {
            emitted(key).await;
        }
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
    /// A stream has failed after emitting rows. No more rows are emitted,
    /// and the error of the job is returned once it is received.
    Stopping {
        receiver: mpsc::Receiver<JobOutputItem>,
        error: TaskStreamError,
    },
    Modifying,
    Completed,
    Failed,
}

// If the task fails, the consumer of the job output will receive the error
// from the fail action ("control plane"). The error of a stream ("data plane")
// is returned only if the job does not fail otherwise, since a stream also fails
// when its task is canceled, and the task region may be retried. The errors
// are standardized via `CommonErrorCause`.

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
                    let partition = OutputPartition::from(&key);
                    // The rows of an output partition come from one attempt. If another
                    // attempt has emitted rows, the job fails since its task region
                    // cannot be retried.
                    if !self.emitted.contains(&partition) {
                        self.failed.remove(&partition);
                        inner.iter_mut().for_each(|s| s.mute_if_needed(&key));
                        inner.push(TaskStreamWrapper::new(key, stream));
                    }
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
            JobOutputState::Stopping {
                mut receiver,
                error,
            } => match receiver.poll_recv(cx) {
                Poll::Pending => {
                    self.state = JobOutputState::Stopping { receiver, error };
                    return Poll::Pending;
                }
                Poll::Ready(Some(JobOutputItem::Error { cause })) => {
                    self.state = JobOutputState::Failed;
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                        TaskStreamError::from(cause),
                    )))));
                }
                Poll::Ready(Some(JobOutputItem::Stream { .. })) => {
                    self.state = JobOutputState::Stopping { receiver, error };
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    self.state = JobOutputState::Failed;
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
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
            JobOutputState::Modifying | JobOutputState::Stopping { .. } => Poll::Pending,
            JobOutputState::Draining { inner } => match inner.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    // A stream that has failed is not replaced by another attempt.
                    if let Some(partition) = self.failed.keys().next().copied()
                        && let Some(error) = self.failed.remove(&partition)
                    {
                        self.state = JobOutputState::Failed;
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                    }
                    self.state = JobOutputState::Completed;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            },
            JobOutputState::Completed | JobOutputState::Failed => Poll::Ready(None),
        };
        match poll {
            Poll::Ready(Some((key, Ok(batch)))) => {
                if batch.num_rows() > 0 && self.emitted.insert(OutputPartition::from(&key)) {
                    self.newly_emitted.push(key);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some((key, Err(error)))) => {
                let partition = OutputPartition::from(&key);
                if self.emitted.contains(&partition) {
                    // The rows that are emitted cannot be taken back.
                    match mem::replace(&mut self.state, JobOutputState::Modifying) {
                        JobOutputState::Active { receiver, inner: _ } => {
                            self.state = JobOutputState::Stopping { receiver, error };
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        _ => {
                            self.state = JobOutputState::Failed;
                            Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))))
                        }
                    }
                } else {
                    // The stream may fail since its task region is retried, so the error
                    // is kept until another attempt adds a stream for the output partition.
                    if let JobOutputState::Active { inner, .. }
                    | JobOutputState::Draining { inner } = &mut self.state
                    {
                        inner.iter_mut().for_each(|s| s.mute_if_needed(&key));
                    }
                    self.failed.insert(partition, error);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
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
    type Item = (TaskStreamKey, TaskStreamResult<RecordBatch>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = &mut self.inner else {
            return Poll::Ready(None);
        };
        let poll = inner.as_mut().poll_next(cx);
        poll.map(|item| item.map(|result| (self.key.clone(), result)))
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
    fn completed_attempt_output_ignores_retry() -> Result<()> {
        let batch = batch(42)?;
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
        assert_eq!(output.newly_emitted, vec![stream_key()]);

        // Poll through EOF so the first attempt's wrapper is removed before retrying.
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(matches!(
            &output.state,
            JobOutputState::Active { inner, .. } if inner.is_empty()
        ));

        // The scheduler fails the job, since the task region cannot be retried.
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
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(matches!(
            &output.state,
            JobOutputState::Active { inner, .. } if inner.is_empty()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn task_stream_error_after_output_returns_job_error() -> Result<()> {
        for job_error in [true, false] {
            for mut state in active_job_states() {
                let (sender, receiver) = mpsc::channel(2);
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key: stream_key(),
                            stream: Box::pin(futures::stream::iter([
                                Ok(batch(1)?),
                                Err(TaskStreamError::Unknown("test stream failure".to_string())),
                                Ok(batch(2)?),
                            ])),
                        })
                        .is_ok()
                );
                let (tx, mut rx) = mpsc::channel(4);
                let emitted = std::sync::Mutex::new(vec![]);
                let forward = forward_job_output(JobOutputStream::new(receiver), &tx, |key| {
                    if let Ok(mut emitted) = emitted.lock() {
                        emitted.push(key);
                    }
                    async {}
                });
                let fail = async {
                    // The rows that are emitted are delivered before the job error.
                    assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual.num_rows() == 1));
                    if job_error {
                        let _ = sender
                            .send(JobOutputItem::Error {
                                cause: CommonErrorCause::Execution("test task failure".to_string()),
                            })
                            .await;
                    } else {
                        drop(sender);
                    }
                };
                let (outcome, ()) = tokio::join!(forward, fail);
                state.finish_output(outcome);

                assert_eq!(outcome, JobOutputOutcome::Failed);
                assert_eq!(state.status(), "FAILED");
                let expected = if job_error {
                    "test task failure"
                } else {
                    "test stream failure"
                };
                assert!(matches!(
                    rx.recv().await,
                    Some(Err(error)) if error.to_string().contains(expected)
                ));
                assert!(rx.try_recv().is_err());
                assert!(matches!(
                    emitted.lock().as_deref().map(Vec::as_slice),
                    Ok([key]) if key == &stream_key()
                ));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_stream_error_before_output_is_replaced_by_retry() -> Result<()> {
        let (sender, receiver) = mpsc::channel(2);
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
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: TaskStreamKey {
                        attempt: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::iter([Ok(batch(1)?)])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(2);

        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |_| async {}).await;

        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual.num_rows() == 1));
        assert!(rx.try_recv().is_err());
        Ok(())
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
            // The error of a stream that is not replaced by another attempt is returned.
            drop(sender);
            let (tx, mut rx) = mpsc::channel(1);

            let outcome =
                forward_job_output(JobOutputStream::new(receiver), &tx, |_| async {}).await;
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

            let outcome =
                forward_job_output(JobOutputStream::new(receiver), &tx, |_| async {}).await;
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

        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |_| async {}).await;
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

    fn batch(value: i32) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![value])) as ArrayRef,
        )])?)
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
