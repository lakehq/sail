use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, ready};
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
        region: RegionOutput,
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

/// Orders "rows of this region reached the client" against "this region was (re)started".
///
/// The scheduler and the job output share one cell for each task region.
/// The attempt of a region is the attempt index shared by its tasks.
#[derive(Debug, Clone, Default)]
pub struct RegionOutput(Arc<Mutex<RegionOutputState>>);

#[derive(Debug, Default)]
struct RegionOutputState {
    attempt: usize,
    /// Whether rows of the current attempt have reached the client.
    delivered: bool,
}

impl RegionOutput {
    fn state(&self) -> MutexGuard<'_, RegionOutputState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Called by the scheduler when it is about to create the task attempts with index
    /// `attempt`. Returns false if rows of an earlier attempt were delivered, in which case the
    /// region must not be started again.
    pub fn start(&self, attempt: usize) -> bool {
        let mut state = self.state();
        if state.attempt == attempt {
            return true;
        }
        if state.delivered {
            return false;
        }
        state.attempt = attempt;
        true
    }

    /// Called by the job output before it forwards the first non-empty batch of a stream.
    /// Returns false if the stream belongs to a replaced attempt.
    fn deliver(&self, attempt: usize) -> bool {
        let mut state = self.state();
        if state.attempt != attempt {
            return false;
        }
        state.delivered = true;
        true
    }

    fn attempt(&self) -> usize {
        self.state().attempt
    }

    /// Whether this is the cell of the same region as `other`.
    fn is(&self, other: &RegionOutput) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

struct JobOutputStream {
    state: JobOutputState,
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputItem>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
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

/// Drops the streams of the region that do not belong to the given attempt,
/// whether or not they are ready.
fn drop_replaced_streams(
    inner: &mut Pin<Box<SelectAll<TaskStreamWrapper>>>,
    region: &RegionOutput,
    attempt: usize,
) {
    let keep = |s: &TaskStreamWrapper| !s.region.is(region) || s.attempt == attempt;
    if inner.iter().all(keep) {
        return;
    }
    let streams = mem::take(&mut **inner);
    **inner = streams.into_iter().filter(keep).collect();
}

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
                    region,
                })) => {
                    let current = region.attempt();
                    match key.attempt.cmp(&current) {
                        Ordering::Less => {}
                        Ordering::Equal => {
                            // A stream of a replaced attempt may never end, so it is
                            // dropped here rather than drained.
                            drop_replaced_streams(&mut inner, &region, current);
                            inner.push(TaskStreamWrapper::new(key.attempt, stream, region));
                        }
                        Ordering::Greater => {
                            self.state = JobOutputState::Failed;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                                TaskStreamError::Unknown(format!(
                                    "cannot add stream for {}: the task region is at attempt {current}",
                                    TaskStreamKeyDisplay(&key)
                                )),
                            )))));
                        }
                    }
                    self.state = JobOutputState::Active { receiver, inner };
                    // Receiving an item does not register the receiver's waker. Even if
                    // the new stream is immediately empty, poll again for queued items
                    // or channel closure before waiting for further notifications.
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(None) => {
                    // No stream of a replaced attempt is held here. A region succeeds only
                    // after the streams of its current attempt have been registered, and
                    // each registration drops the replaced streams of the region.
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
            item.map(|result| result.map_err(|e| DataFusionError::External(Box::new(e))))
        })
    }
}

/// A job output stream of one task attempt.
struct TaskStreamWrapper {
    /// The attempt index of the task that produces the stream.
    attempt: usize,
    region: RegionOutput,
    inner: TaskStreamSource,
    /// Whether the region has recorded that rows of this stream reached the client.
    delivered: bool,
}

impl TaskStreamWrapper {
    fn new(attempt: usize, inner: TaskStreamSource, region: RegionOutput) -> Self {
        Self {
            attempt,
            region,
            inner,
            delivered: false,
        }
    }
}

impl Stream for TaskStreamWrapper {
    type Item = TaskStreamResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(self.inner.as_mut().poll_next(cx));
        match &item {
            Some(Ok(batch)) if !self.delivered && batch.num_rows() > 0 => {
                if !self.region.deliver(self.attempt) {
                    return Poll::Ready(None);
                }
                self.delivered = true;
            }
            // Only the current attempt can fail the job output.
            Some(Err(_)) if self.region.attempt() != self.attempt => {
                return Poll::Ready(None);
            }
            _ => {}
        }
        Poll::Ready(item)
    }
}

#[cfg(test)]
mod tests {
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
    fn delivered_rows_stop_region_restart() -> Result<()> {
        let region = RegionOutput::default();
        let (sender, receiver) = mpsc::channel(1);
        let mut output = JobOutputStream::new(receiver);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let empty = RecordBatch::new_empty(batch(1)?.schema());

        assert!(
            sender
                .try_send(JobOutputItem::Stream {
                    key: stream_key(),
                    stream: Box::pin(futures::stream::iter([Ok(empty), Ok(batch(1)?)])),
                    region: region.clone(),
                })
                .is_ok()
        );
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 0
        ));
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 1
        ));
        assert!(region.start(0));
        assert!(!region.start(1));
        assert_eq!(region.attempt(), 0);
        Ok(())
    }

    #[test]
    fn restart_drops_streams_of_replaced_attempt() -> Result<()> {
        let region = RegionOutput::default();
        let other = RegionOutput::default();
        let (sender, receiver) = mpsc::channel(4);
        let mut output = JobOutputStream::new(receiver);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let item = |partition, attempt, stream: TaskStreamSource, region: &RegionOutput| {
            JobOutputItem::Stream {
                key: TaskStreamKey {
                    partition,
                    attempt,
                    ..stream_key()
                },
                stream,
                region: region.clone(),
            }
        };
        let rows = |poll: Poll<Option<Result<RecordBatch>>>| match poll {
            Poll::Ready(Some(Ok(batch))) => Some(batch.num_rows()),
            _ => None,
        };

        // The stream of the first attempt never ends.
        let stream = Box::pin(futures::stream::pending());
        assert!(sender.try_send(item(0, 0, stream, &region)).is_ok());
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
        let stream = Box::pin(futures::stream::iter([Ok(batch(2)?)]));
        assert!(sender.try_send(item(1, 0, stream, &other)).is_ok());
        assert_eq!(rows(Pin::new(&mut output).poll_next(&mut cx)), Some(2));

        assert!(region.start(1));
        let stream = Box::pin(futures::stream::iter([Ok(batch(3)?)]));
        assert!(sender.try_send(item(0, 1, stream, &region)).is_ok());
        assert_eq!(rows(Pin::new(&mut output).poll_next(&mut cx)), Some(3));

        // A stream of the replaced attempt can also be registered late.
        let stream = Box::pin(futures::stream::iter([Ok(batch(5)?)]));
        assert!(sender.try_send(item(0, 0, stream, &region)).is_ok());
        assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());

        // The output completes although the stream of the first attempt has not ended.
        drop(sender);
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(None)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn restart_keeps_pending_streams_of_other_regions() -> Result<()> {
        let region = RegionOutput::default();
        let other = RegionOutput::default();
        let (sender, receiver) = mpsc::channel(4);
        let (stream_tx, stream_rx) = mpsc::channel(1);
        let mut output = JobOutputStream::new(receiver);
        let item =
            |attempt, stream: TaskStreamSource, region: &RegionOutput| JobOutputItem::Stream {
                key: TaskStreamKey {
                    attempt,
                    ..stream_key()
                },
                stream,
                region: region.clone(),
            };

        // Both regions have a stream that is not ready when the first region is restarted.
        let stream = Box::pin(futures::stream::pending());
        assert!(sender.try_send(item(0, stream, &region)).is_ok());
        let stream = Box::pin(ReceiverStream::new(stream_rx));
        assert!(sender.try_send(item(0, stream, &other)).is_ok());
        assert!(futures::poll!(output.next()).is_pending());
        assert!(futures::poll!(output.next()).is_pending());
        assert!(region.start(1));
        let stream = Box::pin(futures::stream::iter([Ok(batch(3)?)]));
        assert!(sender.try_send(item(1, stream, &region)).is_ok());
        assert!(matches!(output.next().await, Some(Ok(batch)) if batch.num_rows() == 3));

        // The stream of the other region is still read after the restart.
        let reader = tokio::spawn(async move { output.next().await });
        tokio::task::yield_now().await;
        assert!(stream_tx.send(Ok(batch(5)?)).await.is_ok());
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), reader).await;
        assert!(matches!(next, Ok(Ok(Some(Ok(batch)))) if batch.num_rows() == 5));
        Ok(())
    }

    #[test]
    fn replaced_attempt_cannot_deliver_rows_or_errors() -> Result<()> {
        for item in [
            Ok(batch(1)?),
            Err(TaskStreamError::Unknown("test stream failure".to_string())),
        ] {
            let region = RegionOutput::default();
            let (sender, receiver) = mpsc::channel(1);
            let (stream_tx, stream_rx) = mpsc::channel(1);
            let mut output = JobOutputStream::new(receiver);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);

            assert!(
                sender
                    .try_send(JobOutputItem::Stream {
                        key: stream_key(),
                        stream: Box::pin(ReceiverStream::new(stream_rx)),
                        region: region.clone(),
                    })
                    .is_ok()
            );
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());

            // The region is restarted before the stream of the first attempt yields anything.
            assert!(region.start(1));
            assert!(stream_tx.try_send(item).is_ok());
            assert!(Pin::new(&mut output).poll_next(&mut cx).is_pending());
            assert!(matches!(
                &output.state,
                JobOutputState::Active { inner, .. } if inner.is_empty()
            ));
            assert!(region.start(2));
        }
        Ok(())
    }

    #[test]
    fn stream_of_unknown_attempt_fails_output() {
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
                    region: RegionOutput::default(),
                })
                .is_ok()
        );
        assert!(matches!(
            Pin::new(&mut output).poll_next(&mut cx),
            Poll::Ready(Some(Err(error))) if error.to_string().contains("cannot add stream")
        ));
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
                        region: RegionOutput::default(),
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
            region: RegionOutput::default(),
        }
    }

    fn batch(rows: i32) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
        )])?)
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
