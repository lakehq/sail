use std::fmt::Formatter;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, mem};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, HashSet, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::SelectAll;
use futures::{Stream, StreamExt};
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::ActorContext;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::{DriverActor, DriverEvent};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;

pub struct JobOutputSender {
    key: TaskStreamKey,
    sender: mpsc::Sender<JobOutputCommand>,
}

impl fmt::Debug for JobOutputSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobOutputSender").finish()
    }
}

impl JobOutputSender {
    pub async fn send(self, stream: TaskStreamSource) {
        let command = JobOutputCommand::AddStream {
            key: self.key,
            stream,
        };
        // We ignore the error here because it indicates that the job output
        // consumer has been dropped.
        let _ = self.sender.send(command).await;
    }
}

pub struct JobOutputNotifier {
    sender: mpsc::Sender<JobOutputCommand>,
}

impl fmt::Debug for JobOutputNotifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobOutputFailureNotifier").finish()
    }
}

impl JobOutputNotifier {
    pub async fn succeed(self) {
        let command = JobOutputCommand::Succeed;
        let _ = self.sender.send(command).await;
    }

    pub async fn fail(self, cause: CommonErrorCause) {
        let command = JobOutputCommand::Fail { cause };
        let _ = self.sender.send(command).await;
    }
}

pub struct JobOutputManager {
    streams: HashSet<TaskStreamKey>,
    sender: mpsc::Sender<JobOutputCommand>,
}

impl JobOutputManager {
    pub fn has_stream(&self, key: &TaskStreamKey) -> bool {
        self.streams.contains(key)
    }

    pub fn send_stream(&mut self, key: TaskStreamKey) -> JobOutputSender {
        self.streams.insert(key.clone());
        JobOutputSender {
            key,
            sender: self.sender.clone(),
        }
    }

    pub fn notifier(&self) -> JobOutputNotifier {
        JobOutputNotifier {
            sender: self.sender.clone(),
        }
    }
}

pub enum JobOutputCommand {
    Succeed,
    Fail {
        cause: CommonErrorCause,
    },
    AddStream {
        key: TaskStreamKey,
        stream: TaskStreamSource,
    },
}

impl JobOutputCommand {
    pub const CHANNEL_SIZE: usize = 32;
}

struct JobOutputStream {
    state: JobOutputState,
    succeeded: bool,
}

impl JobOutputStream {
    fn new(receiver: mpsc::Receiver<JobOutputCommand>) -> Self {
        Self {
            state: JobOutputState::Active {
                receiver,
                inner: Box::pin(SelectAll::new()),
            },
            succeeded: false,
        }
    }
}

pub fn build_job_output(
    ctx: &mut ActorContext<DriverActor>,
    job_id: JobId,
    schema: SchemaRef,
) -> (JobOutputManager, SendableRecordBatchStream) {
    let (sender, receiver) = mpsc::channel(JobOutputCommand::CHANNEL_SIZE);
    let (tx, rx) = mpsc::channel(1);
    let stream = JobOutputStream::new(receiver);
    let handle = ctx.handle().clone();
    ctx.spawn(async move {
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            if tx.send(batch).await.is_err() {
                break;
            }
        }
        let _ = handle.send(DriverEvent::CleanUpJob { job_id }).await;
    });
    (
        JobOutputManager {
            streams: HashSet::new(),
            sender,
        },
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            ReceiverStream::new(rx),
        )),
    )
}

enum JobOutputState {
    Active {
        receiver: mpsc::Receiver<JobOutputCommand>,
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
                Poll::Ready(Some(JobOutputCommand::Succeed)) => {
                    self.state = JobOutputState::Active { receiver, inner };
                    self.succeeded = true;
                }
                Poll::Ready(Some(JobOutputCommand::Fail { cause })) => {
                    self.state = JobOutputState::Failed;
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                        TaskStreamError::from(cause),
                    )))));
                }
                Poll::Ready(Some(JobOutputCommand::AddStream { key, stream })) => {
                    if inner.iter().any(|s| s.conflicts_with(&key)) {
                        self.state = JobOutputState::Failed;
                        return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                            TaskStreamError::Unknown(format!(
                                "cannot add stream for {}: a different attempt has already produced job output",
                                TaskStreamKeyDisplay(&key)
                            )),
                        )))));
                    }
                    inner.iter_mut().for_each(|s| s.mute_if_needed(&key));
                    inner.push(TaskStreamWrapper::new(key, stream));
                    self.state = JobOutputState::Active { receiver, inner };
                }
                Poll::Ready(None) => {
                    self.state = JobOutputState::Draining { inner };
                }
            },
            _ => {
                self.state = state;
            }
        }
        match &mut self.state {
            JobOutputState::Active { inner, receiver: _ } => {
                match inner.as_mut().poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => {
                        // We return pending even if all the existing streams are done,
                        // because new streams may still be added.
                        Poll::Pending
                    }
                    Poll::Ready(Some(result)) => Poll::Ready(Some(
                        result.map_err(|e| DataFusionError::External(Box::new(e))),
                    )),
                }
            }
            JobOutputState::Modifying => Poll::Pending,
            JobOutputState::Draining { inner } => match inner.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.state = JobOutputState::Completed;
                    if self.succeeded {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
                Poll::Ready(Some(result)) => Poll::Ready(Some(
                    result.map_err(|e| DataFusionError::External(Box::new(e))),
                )),
            },
            JobOutputState::Completed | JobOutputState::Failed => Poll::Ready(None),
        }
    }
}

struct TaskStreamWrapper {
    key: TaskStreamKey,
    inner: Option<TaskStreamSource>,
    count: usize,
}

impl TaskStreamWrapper {
    fn new(key: TaskStreamKey, inner: TaskStreamSource) -> Self {
        Self {
            key,
            inner: Some(inner),
            count: 0,
        }
    }

    fn conflicts_with(&self, key: &TaskStreamKey) -> bool {
        self.key.job_id == key.job_id
            && self.key.stage == key.stage
            && self.key.partition == key.partition
            && self.key.channel == key.channel
            && self.count > 0
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
    type Item = TaskStreamResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = &mut self.inner else {
            return Poll::Ready(None);
        };
        let poll = inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(ref batch))) = poll {
            self.count += batch.num_rows();
        }
        poll
    }
}
