use std::collections::HashSet;
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
use tokio::sync::{mpsc, oneshot};
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

/// The output asks the scheduler before exposing an attempt's first rows or handling
/// a stream failure. Sending this request alone does not authorize delivery.
#[derive(Debug)]
pub enum JobOutputUpdate {
    Emit,
    Fail(CommonErrorCause),
}

#[derive(Debug)]
pub enum JobOutputDecision {
    Accept,
    Discard,
    Fail(CommonErrorCause),
}

struct JobOutputStream {
    state: JobOutputState,
    /// Output partitions whose first rows have been authorized for delivery.
    /// Keep this history after completed streams have been removed from `SelectAll`.
    emitted: HashSet<OutputPartition>,
}

enum JobOutputEvent {
    Batch {
        key: TaskStreamKey,
        batch: RecordBatch,
    },
    StreamError {
        key: TaskStreamKey,
        error: TaskStreamError,
    },
    JobError {
        cause: CommonErrorCause,
    },
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputItem>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
            emitted: HashSet::new(),
        }
    }

    fn discard_attempt(&mut self, key: &TaskStreamKey) {
        if let JobOutputState::Active { inner, .. } | JobOutputState::Draining { inner } =
            &mut self.state
        {
            for stream in inner.iter_mut() {
                if stream.key == *key {
                    stream.inner = None;
                }
            }
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
        let update = |key: TaskStreamKey, update| {
            let handle = handle.clone();
            async move {
                let (result, receiver) = oneshot::channel();
                if handle
                    .send(DriverMessage::UpdateJobOutput {
                        key: TaskKey::from(key),
                        update,
                        result,
                    })
                    .await
                    .is_err()
                {
                    return JobOutputDecision::Fail(CommonErrorCause::Internal(
                        "driver stopped before deciding job output".to_string(),
                    ));
                }
                receiver.await.unwrap_or_else(|_| {
                    JobOutputDecision::Fail(CommonErrorCause::Internal(
                        "driver dropped the job output decision".to_string(),
                    ))
                })
            }
        };
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, update).await;
        // Output errors and consumer cancellation can precede terminal task updates.
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
    update: F,
) -> JobOutputOutcome
where
    F: Fn(TaskStreamKey, JobOutputUpdate) -> Fut,
    Fut: Future<Output = JobOutputDecision>,
{
    loop {
        let next = tokio::select! {
            biased;
            x = stream.next() => x,
            _ = sender.closed() => return JobOutputOutcome::Canceled,
        };
        let Some(event) = next else {
            return JobOutputOutcome::Completed;
        };
        let batch = match event {
            JobOutputEvent::Batch { key, batch } => {
                let partition = OutputPartition::from(&key);
                if batch.num_rows() > 0 && !stream.emitted.contains(&partition) {
                    // The acknowledgment validates this attempt and records its output
                    // before a task retry can be scheduled or these rows can be delivered.
                    let decision = tokio::select! {
                        decision = update(key.clone(), JobOutputUpdate::Emit) => decision,
                        _ = sender.closed() => return JobOutputOutcome::Canceled,
                    };
                    match decision {
                        JobOutputDecision::Accept => {
                            stream.emitted.insert(partition);
                            Ok(batch)
                        }
                        JobOutputDecision::Discard => {
                            stream.discard_attempt(&key);
                            continue;
                        }
                        JobOutputDecision::Fail(cause) => Err(cause),
                    }
                } else {
                    Ok(batch)
                }
            }
            JobOutputEvent::StreamError { key, error } => {
                stream.discard_attempt(&key);
                let cause = CommonErrorCause::from(error);
                // Only the scheduler can confirm a retry. A current transport failure
                // otherwise fails promptly, even if its producer has not finished.
                let decision = tokio::select! {
                    decision = update(key, JobOutputUpdate::Fail(cause.clone())) => decision,
                    _ = sender.closed() => return JobOutputOutcome::Canceled,
                };
                match decision {
                    JobOutputDecision::Discard => continue,
                    JobOutputDecision::Fail(cause) => Err(cause),
                    // Accept is only valid for rows; never turn a read failure into EOF.
                    JobOutputDecision::Accept => Err(cause),
                }
            }
            JobOutputEvent::JobError { cause } => Err(cause),
        };
        let batch = batch
            .map_err(|cause| DataFusionError::External(Box::new(TaskStreamError::from(cause))));
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

impl Stream for JobOutputStream {
    type Item = JobOutputEvent;

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
                    return Poll::Ready(Some(JobOutputEvent::JobError { cause }));
                }
                Poll::Ready(Some(JobOutputItem::Stream { key, stream })) => {
                    let partition = OutputPartition::from(&key);
                    let newer = inner.iter().any(|stream| {
                        OutputPartition::from(&stream.key) == partition
                            && stream.key.attempt > key.attempt
                    });
                    // Fetches complete asynchronously. A delayed old registration
                    // must not replace the current attempt's still unread stream.
                    if !self.emitted.contains(&partition) && !newer {
                        inner.iter_mut().for_each(|s| s.mute_if_needed(&key));
                        inner.push(TaskStreamWrapper::new(key, stream));
                    }
                    self.state = JobOutputState::Active { receiver, inner };
                    // Receiving an item does not register the receiver's waker. Even if
                    // the new stream is empty, poll again for queued items or closure.
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(None) => {
                    self.state = JobOutputState::Draining { inner };
                }
            },
            _ => self.state = state,
        }
        let poll = match &mut self.state {
            JobOutputState::Active { inner, .. } => match inner.as_mut().poll_next(cx) {
                // New streams or a job failure may still arrive.
                Poll::Ready(None) => Poll::Pending,
                other => other,
            },
            JobOutputState::Draining { inner } => match inner.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    self.state = JobOutputState::Completed;
                    Poll::Ready(None)
                }
                other => other,
            },
            JobOutputState::Modifying => Poll::Pending,
            JobOutputState::Completed | JobOutputState::Failed => Poll::Ready(None),
        };
        poll.map(|item| {
            item.map(|(key, result)| match result {
                Ok(batch) => JobOutputEvent::Batch { key, batch },
                Err(error) => JobOutputEvent::StreamError { key, error },
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
pub(crate) fn test_job_output() -> (JobOutputManager, mpsc::Receiver<JobOutputItem>) {
    let (sender, receiver) = mpsc::channel(JobOutputItem::CHANNEL_SIZE);
    (JobOutputManager { sender }, receiver)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use futures::task::{ArcWake, waker};
    use tokio::sync::Notify;

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
        assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(JobOutputEvent::JobError { cause: CommonErrorCause::Execution(message) }))
                if message == "test failure"));
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
        assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(JobOutputEvent::Batch { batch: actual, .. })) if actual == batch));
        // Forwarding records this only after the scheduler authorizes delivery.
        output.emitted.insert(OutputPartition::from(&stream_key()));
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(matches!(&output.state, JobOutputState::Active { inner, .. } if inner.is_empty()));
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
        assert!(matches!(&output.state, JobOutputState::Active { inner, .. } if inner.is_empty()));
        Ok(())
    }

    #[tokio::test]
    async fn discarded_attempt_does_not_hide_replacement_output() -> Result<()> {
        let (sender, receiver) = mpsc::channel(2);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch(41)?), Ok(batch(42)?)])),
                })
                .is_ok()
        );
        let expected = batch(99)?;
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: TaskStreamKey {
                        attempt: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::iter([Ok(expected.clone())])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(4);
        let rejected = AtomicUsize::new(0);
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |key, update| {
            assert!(matches!(update, JobOutputUpdate::Emit));
            let decision = if key.attempt == 0 {
                rejected.fetch_add(1, Ordering::Relaxed);
                JobOutputDecision::Discard
            } else {
                JobOutputDecision::Accept
            };
            async move { decision }
        })
        .await;
        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert_eq!(rejected.load(Ordering::Relaxed), 1);
        assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual == expected));
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn delayed_old_registration_cannot_replace_or_leak_into_new_output() -> Result<()> {
        let (sender, receiver) = mpsc::channel(2);
        let expected = batch(99)?;
        let current = expected.clone();
        // The current stream is installed first, but does not produce its batch on
        // its first poll. The older fetch completes while that stream is waiting.
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: TaskStreamKey {
                        attempt: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::once(async move {
                        tokio::task::yield_now().await;
                        Ok(current)
                    })),
                })
                .is_ok()
        );
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch(41)?), Ok(batch(42)?)])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(4);
        let outcome = forward_job_output(
            JobOutputStream::new(receiver),
            &tx,
            |key, update| async move {
                assert!(matches!(update, JobOutputUpdate::Emit));
                if key.attempt == 1 {
                    JobOutputDecision::Accept
                } else {
                    JobOutputDecision::Discard
                }
            },
        )
        .await;
        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual == expected));
        assert!(
            rx.try_recv().is_err(),
            "rows from the old attempt must not follow the current rows"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rows_wait_for_the_scheduler_decision() -> Result<()> {
        let (sender, receiver) = mpsc::channel(1);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch(1)?)])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(1);
        let requested = Notify::new();
        let permission = Notify::new();
        let forward = forward_job_output(JobOutputStream::new(receiver), &tx, |_, update| {
            assert!(matches!(update, JobOutputUpdate::Emit));
            async {
                requested.notify_one();
                permission.notified().await;
                JobOutputDecision::Accept
            }
        });
        let authorize = async {
            requested.notified().await;
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            permission.notify_one();
        };
        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(forward, authorize)
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual.num_rows() == 1));
        Ok(())
    }

    #[tokio::test]
    async fn consumer_cancellation_interrupts_a_pending_scheduler_decision() -> Result<()> {
        let (sender, receiver) = mpsc::channel(1);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch(1)?)])),
                })
                .is_ok()
        );
        let (tx, rx) = mpsc::channel(1);
        let requested = Notify::new();
        let forward = forward_job_output(JobOutputStream::new(receiver), &tx, |_, update| {
            assert!(matches!(update, JobOutputUpdate::Emit));
            async {
                requested.notify_one();
                futures::future::pending::<JobOutputDecision>().await
            }
        });
        let cancel = async {
            requested.notified().await;
            drop(rx);
        };
        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(forward, cancel)
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(outcome, JobOutputOutcome::Canceled);
        drop(sender);
        Ok(())
    }

    #[tokio::test]
    async fn reader_open_failure_does_not_wait_for_producer_completion() -> Result<()> {
        let (sender, receiver) = mpsc::channel(1);
        // A failed do_get produces an error-only stream while its task may remain
        // running, blocked on an output channel that no reader ever subscribed to.
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Err(TaskStreamError::Unknown(
                        "do_get connection failed".to_string(),
                    ))])),
                })
                .is_ok()
        );
        let (tx, mut rx) = mpsc::channel(1);
        let forward = forward_job_output(
            JobOutputStream::new(receiver),
            &tx,
            |key, update| async move {
                assert_eq!(key, stream_key());
                match update {
                    JobOutputUpdate::Fail(cause) => JobOutputDecision::Fail(cause),
                    JobOutputUpdate::Emit => JobOutputDecision::Fail(CommonErrorCause::Internal(
                        "unexpected rows from a reader that never attached".into(),
                    )),
                }
            },
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), forward)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(outcome, JobOutputOutcome::Failed);
        // Keep the control sender alive until after the assertion: producer
        // completion or control-channel closure cannot be what unblocked output.
        assert!(
            matches!(rx.recv().await, Some(Err(error)) if error.to_string().contains("do_get connection failed"))
        );
        for mut state in active_job_states() {
            state.finish_output(outcome);
            assert_eq!(state.status(), "FAILED");
        }
        drop(sender);
        Ok(())
    }

    #[tokio::test]
    async fn task_stream_error_after_output_returns_the_scheduler_error() -> Result<()> {
        for task_error in [true, false] {
            for mut state in active_job_states() {
                let (sender, receiver) = mpsc::channel(1);
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
                let emitted = AtomicUsize::new(0);
                let outcome =
                    forward_job_output(JobOutputStream::new(receiver), &tx, |key, update| {
                        assert_eq!(key, stream_key());
                        let decision = match update {
                            JobOutputUpdate::Emit => {
                                emitted.fetch_add(1, Ordering::Relaxed);
                                JobOutputDecision::Accept
                            }
                            JobOutputUpdate::Fail(cause) => {
                                JobOutputDecision::Fail(if task_error {
                                    CommonErrorCause::Execution("test task failure".to_string())
                                } else {
                                    cause
                                })
                            }
                        };
                        async move { decision }
                    })
                    .await;
                state.finish_output(outcome);
                assert_eq!(outcome, JobOutputOutcome::Failed);
                assert_eq!(state.status(), "FAILED");
                assert_eq!(emitted.load(Ordering::Relaxed), 1);
                assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual.num_rows() == 1));
                let expected = if task_error {
                    "test task failure"
                } else {
                    "test stream failure"
                };
                assert!(
                    matches!(rx.recv().await, Some(Err(error)) if error.to_string().contains(expected))
                );
                assert!(rx.try_recv().is_err());
                drop(sender);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_retry_replaces_a_failed_stream_before_output() -> Result<()> {
        let (sender, receiver) = mpsc::channel(2);
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([
                        Err(TaskStreamError::Unknown("old stream failure".to_string())),
                        Ok(batch(41)?),
                    ])),
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
                    stream: Box::pin(futures::stream::iter([Ok(batch(99)?)])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(2);
        let failures = AtomicUsize::new(0);
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |key, update| {
            let decision = match update {
                JobOutputUpdate::Fail(_) => {
                    assert_eq!(key.attempt, 0);
                    failures.fetch_add(1, Ordering::Relaxed);
                    JobOutputDecision::Discard
                }
                JobOutputUpdate::Emit => {
                    assert_eq!(key.attempt, 1);
                    JobOutputDecision::Accept
                }
            };
            async move { decision }
        })
        .await;
        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert_eq!(failures.load(Ordering::Relaxed), 1);
        let expected = batch(99)?;
        assert!(matches!(rx.recv().await, Some(Ok(actual)) if actual == expected));
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn empty_batches_do_not_commit_an_attempt() -> Result<()> {
        let (sender, receiver) = mpsc::channel(1);
        let empty = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
        )])?;
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(empty), Ok(batch(1)?)])),
                })
                .is_ok()
        );
        drop(sender);
        let (tx, mut rx) = mpsc::channel(2);
        let requests = AtomicUsize::new(0);
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |_, update| {
            assert!(matches!(update, JobOutputUpdate::Emit));
            requests.fetch_add(1, Ordering::Relaxed);
            async { JobOutputDecision::Accept }
        })
        .await;
        assert_eq!(outcome, JobOutputOutcome::Completed);
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        let mut rows = 0;
        while let Ok(batch) = rx.try_recv() {
            rows += batch?.num_rows();
        }
        assert_eq!(rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn consumer_disconnection_cancels_running_and_draining_jobs() {
        for mut state in active_job_states() {
            let (_sender, receiver) = mpsc::channel(1);
            let (tx, rx) = mpsc::channel(1);
            drop(rx);
            let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |_, _| async {
                JobOutputDecision::Fail(CommonErrorCause::Internal(
                    "unexpected output request with no stream installed".into(),
                ))
            })
            .await;
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
        let outcome = forward_job_output(JobOutputStream::new(receiver), &tx, |_, _| async {
            JobOutputDecision::Fail(CommonErrorCause::Internal(
                "unexpected output request after draining empty output".into(),
            ))
        })
        .await;
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
        let (output, _receiver) = test_job_output();
        [JobState::Running { output }, JobState::Draining]
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
