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
        retry_cause: Option<CommonErrorCause>,
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
    /// Keep the accepted attempt's cause even after its stream reaches EOF.
    accepted: HashMap<OutputPartition, OutputAttempt>,
}

struct OutputAttempt {
    attempt: usize,
    retry_cause: Option<CommonErrorCause>,
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputItem>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
            emitted: HashSet::new(),
            accepted: HashMap::new(),
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
                Poll::Ready(Some(JobOutputItem::Stream {
                    key,
                    stream,
                    retry_cause,
                })) => {
                    let partition = OutputPartition::from(&key);
                    if self.emitted.contains(&partition) {
                        self.state = JobOutputState::Failed;
                        // Registration can arrive out of order. The newer attempt owns
                        // the retry cause; its missing cause must not revive an older one.
                        let retry_cause = match self.accepted.get(&partition) {
                            Some(current) if current.attempt > key.attempt => {
                                current.retry_cause.clone()
                            }
                            Some(current) if current.attempt == key.attempt => {
                                retry_cause.or_else(|| current.retry_cause.clone())
                            }
                            _ => retry_cause,
                        };
                        let error = retry_cause.map(TaskStreamError::from).unwrap_or_else(|| {
                            TaskStreamError::Unknown(format!(
                                "cannot add stream for {}: a different attempt has already produced job output",
                                TaskStreamKeyDisplay(&key)
                            ))
                        });
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                    }
                    if self
                        .accepted
                        .get(&partition)
                        .is_none_or(|current| current.attempt <= key.attempt)
                    {
                        self.accepted.insert(
                            partition,
                            OutputAttempt {
                                attempt: key.attempt,
                                retry_cause,
                            },
                        );
                    }
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
    fn emitted_attempt_output_rejects_retry_with_original_cause() -> Result<()> {
        use sail_python_udf::error::PyErrExtractor;

        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for retry_cause in [
            None,
            Some(CommonErrorCause::ArrowCast("invalid integer".into())),
        ] {
            for completed in [false, true] {
                let (sender, receiver) = mpsc::channel(1);
                let mut output = JobOutputStream::new(receiver);
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let batches = futures::stream::iter([Ok(batch.clone())]);
                let stream: TaskStreamSource = if completed {
                    Box::pin(batches)
                } else {
                    Box::pin(batches.chain(futures::stream::pending()))
                };
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key: stream_key(),
                            stream,
                            retry_cause: None,
                        })
                        .is_ok()
                );
                assert!(matches!(
                    Pin::new(&mut output).poll_next(&mut cx),
                    Poll::Ready(Some(Ok(actual))) if actual == batch
                ));
                // Cover both an active sibling and one removed from SelectAll after EOF.
                assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
                assert!(matches!(
                    &output.state,
                    JobOutputState::Active { inner, .. } if inner.is_empty() == completed
                ));
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key: TaskStreamKey {
                                attempt: 1,
                                ..stream_key()
                            },
                            stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                            retry_cause: retry_cause.clone(),
                        })
                        .is_ok()
                );
                let Poll::Ready(Some(Err(error))) = Pin::new(&mut output).poll_next(&mut cx) else {
                    return Err(DataFusionError::Internal(
                        "retry must not replay an emitted partition".into(),
                    ));
                };
                let cause = CommonErrorCause::new::<PyErrExtractor>(&error);
                if retry_cause.is_some() {
                    assert!(
                        matches!(cause, CommonErrorCause::ArrowCast(message) if message == "invalid integer")
                    );
                } else {
                    assert!(
                        matches!(cause, CommonErrorCause::Unknown(message) if message ==
                        "cannot add stream for job 1 stage 0 partition 0 attempt 1 channel 0: a different attempt has already produced job output")
                    );
                }
                assert!(matches!(output.state, JobOutputState::Failed));
                assert!(matches!(
                    Pin::new(&mut output).poll_next(&mut cx),
                    Poll::Ready(None)
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn delayed_original_stream_preserves_already_registered_retry_cause() -> Result<()> {
        use sail_python_udf::error::PyErrExtractor;

        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        let (sender, receiver) = mpsc::channel(1);
        let mut output = JobOutputStream::new(receiver);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        // Async registration delivers the retry before the original stream.
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: TaskStreamKey {
                        attempt: 1,
                        ..stream_key()
                    },
                    stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                    retry_cause: Some(CommonErrorCause::ArrowCast("invalid integer".into())),
                })
                .is_ok()
        );
        assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Ok(actual))) if actual == batch));
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(batch)])),
                    retry_cause: None,
                })
                .is_ok()
        );
        let Poll::Ready(Some(Err(error))) = Pin::new(&mut output).poll_next(&mut cx) else {
            return Err(DataFusionError::Internal(
                "late original must not replay output".into(),
            ));
        };
        let cause = CommonErrorCause::new::<PyErrExtractor>(&error);
        assert!(
            matches!(&cause, CommonErrorCause::ArrowCast(message) if message == "invalid integer"),
            "unexpected cause: {cause:?}"
        );
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_ready());
        Ok(())
    }

    #[test]
    fn output_conflict_uses_the_newer_attempt_cause() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for (accepted, incoming, accepted_cause, incoming_cause, expected) in [
            (1, 2, Some("old"), Some("new"), Some("new")),
            (2, 1, Some("new"), Some("old"), Some("new")),
            (1, 2, Some("old"), None, None),
            (2, 1, None, Some("old"), None),
            (1, 0, None, None, None),
            (1, 1, Some("same"), None, Some("same")),
            (1, 1, None, Some("same"), Some("same")),
        ] {
            for completed in [false, true] {
                let (sender, receiver) = mpsc::channel(1);
                let mut output = JobOutputStream::new(receiver);
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let batches = futures::stream::iter([Ok(batch.clone())]);
                let stream: TaskStreamSource = if completed {
                    Box::pin(batches)
                } else {
                    Box::pin(batches.chain(futures::stream::pending()))
                };
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key: TaskStreamKey {
                                attempt: accepted,
                                ..stream_key()
                            },
                            stream,
                            retry_cause: accepted_cause
                                .map(|message| CommonErrorCause::ArrowCast(message.into())),
                        })
                        .is_ok()
                );
                assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
                    Poll::Ready(Some(Ok(actual))) if actual == batch));
                assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
                assert!(matches!(&output.state,
                    JobOutputState::Active { inner, .. } if inner.is_empty() == completed));
                let key = TaskStreamKey {
                    attempt: incoming,
                    ..stream_key()
                };
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key: key.clone(),
                            stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                            retry_cause: incoming_cause
                                .map(|message| CommonErrorCause::ArrowCast(message.into())),
                        })
                        .is_ok()
                );
                assert_conflict_cause(output, &key, expected)?;
            }
        }
        Ok(())
    }

    #[test]
    fn delayed_original_output_preserves_the_newest_pending_attempt_cause() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for newest_cause in [Some("new"), None] {
            let (sender, receiver) = mpsc::channel(1);
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: TaskStreamKey {
                            attempt: 2,
                            ..stream_key()
                        },
                        stream: Box::pin(futures::stream::pending()),
                        retry_cause: newest_cause
                            .map(|message| CommonErrorCause::ArrowCast(message.into())),
                    })
                    .is_ok()
            );
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: None,
                    })
                    .is_ok()
            );
            assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
                Poll::Ready(Some(Ok(actual))) if actual == batch));
            let key = TaskStreamKey {
                attempt: 1,
                ..stream_key()
            };
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: key.clone(),
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: Some(CommonErrorCause::ArrowCast("old".into())),
                    })
                    .is_ok()
            );
            assert_conflict_cause(output, &key, newest_cause)?;
        }
        Ok(())
    }

    #[test]
    fn accepting_next_attempt_replaces_its_predecessors_cause() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for next_cause in [None, Some("new")] {
            let (sender, receiver) = mpsc::channel(1);
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: TaskStreamKey {
                            attempt: 1,
                            ..stream_key()
                        },
                        stream: Box::pin(futures::stream::empty()),
                        retry_cause: Some(CommonErrorCause::ArrowCast("old".into())),
                    })
                    .is_ok()
            );
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: TaskStreamKey {
                            attempt: 2,
                            ..stream_key()
                        },
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: next_cause
                            .map(|message| CommonErrorCause::ArrowCast(message.into())),
                    })
                    .is_ok()
            );
            assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
                Poll::Ready(Some(Ok(actual))) if actual == batch));
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            let key = TaskStreamKey {
                attempt: 1,
                ..stream_key()
            };
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: key.clone(),
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: Some(CommonErrorCause::ArrowCast("old".into())),
                    })
                    .is_ok()
            );
            assert_conflict_cause(output, &key, next_cause)?;
        }
        Ok(())
    }

    #[test]
    fn accepted_retry_causes_are_scoped_to_the_output_partition() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for unrelated in [
            TaskStreamKey {
                job_id: JobId::from(2),
                ..stream_key()
            },
            TaskStreamKey {
                stage: 1,
                ..stream_key()
            },
            TaskStreamKey {
                partition: 1,
                ..stream_key()
            },
            TaskStreamKey {
                channel: 1,
                ..stream_key()
            },
        ] {
            let (sender, receiver) = mpsc::channel(1);
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            for (key, retry_cause) in [
                (
                    TaskStreamKey {
                        attempt: 2,
                        ..stream_key()
                    },
                    None,
                ),
                (
                    TaskStreamKey {
                        attempt: 99,
                        ..unrelated
                    },
                    Some(CommonErrorCause::ArrowCast("unrelated".into())),
                ),
            ] {
                assert!(
                    sender
                        .try_send(JobOutputItem::Stream {
                            key,
                            stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                            retry_cause,
                        })
                        .is_ok()
                );
                assert!(matches!(Pin::new(&mut output).poll_next(&mut cx),
                    Poll::Ready(Some(Ok(actual))) if actual == batch));
                assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            }
            let key = TaskStreamKey {
                attempt: 1,
                ..stream_key()
            };
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: key.clone(),
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: None,
                    })
                    .is_ok()
            );
            assert_conflict_cause(output, &key, None)?;
        }
        Ok(())
    }

    fn assert_conflict_cause(
        mut output: JobOutputStream,
        key: &TaskStreamKey,
        expected: Option<&str>,
    ) -> Result<()> {
        use sail_python_udf::error::PyErrExtractor;

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready(Some(Err(error))) = Pin::new(&mut output).poll_next(&mut cx) else {
            return Err(DataFusionError::Internal(
                "conflicting stream must not replay output".into(),
            ));
        };
        let cause = CommonErrorCause::new::<PyErrExtractor>(&error);
        match expected {
            Some(expected) => assert!(
                matches!(&cause, CommonErrorCause::ArrowCast(message) if message == expected),
                "unexpected cause: {cause:?}"
            ),
            None => assert!(
                matches!(&cause, CommonErrorCause::Unknown(message) if message == &format!(
                    "cannot add stream for {}: a different attempt has already produced job output",
                    TaskStreamKeyDisplay(key)
                )),
                "unexpected cause: {cause:?}"
            ),
        }
        assert!(matches!(output.state, JobOutputState::Failed));
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(None)
        ));
        Ok(())
    }

    #[test]
    fn retry_cause_does_not_fail_unemitted_output() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        )])?;
        for completed in [false, true] {
            let (sender, receiver) = mpsc::channel(1);
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let stream: TaskStreamSource = if completed {
                Box::pin(futures::stream::empty())
            } else {
                Box::pin(futures::stream::iter([Ok(batch.slice(0, 0))]))
            };
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream,
                        retry_cause: None,
                    })
                    .is_ok()
            );
            let first = Pin::new(&mut output).poll_next(&mut cx);
            if completed {
                assert!(first.is_pending());
            } else {
                assert!(matches!(first, Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 0));
            }
            assert!(output.emitted.is_empty());
            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: TaskStreamKey {
                            attempt: 1,
                            ..stream_key()
                        },
                        stream: Box::pin(futures::stream::iter([Ok(batch.clone())])),
                        retry_cause: Some(CommonErrorCause::Io("transient failure".into())),
                    })
                    .is_ok()
            );
            assert!(matches!(
                Pin::new(&mut output).poll_next(&mut cx),
                Poll::Ready(Some(Ok(actual))) if actual == batch
            ));
            drop(sender);
            assert!(matches!(
                Pin::new(&mut output).poll_next(&mut cx),
                Poll::Ready(None)
            ));
            assert!(matches!(output.state, JobOutputState::Completed));
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
                            retry_cause: None,
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
                        retry_cause: None,
                    })
                    .is_ok()
            );
            let (tx, mut rx) = mpsc::channel(1);

            let outcome = forward_job_output(JobOutputStream::new(receiver), &tx).await;
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
            retry_cause: None,
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
