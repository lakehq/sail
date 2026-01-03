use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::RecordBatchStream;
use futures::stream::SelectAll;
use futures::Stream;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;

use crate::id::{TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
const JOB_OUTPUT_ACTION_CHANNEL_SIZE: usize = 32;

pub enum JobOutputAction {
    Fail {
        cause: CommonErrorCause,
    },
    AddStream {
        key: TaskStreamKey,
        stream: TaskStreamSource,
    },
}

pub struct JobOutputHandle {
    sender: mpsc::Sender<JobOutputAction>,
}

pub struct JobOutputStream {
    schema: SchemaRef,
    state: JobOutputState,
}

enum JobOutputState {
    Active {
        receiver: mpsc::Receiver<JobOutputAction>,
        inner: Pin<Box<SelectAll<TaskStreamWrapper>>>,
    },
    Draining {
        inner: Pin<Box<SelectAll<TaskStreamWrapper>>>,
    },
    Modifying,
    Completed,
    Failed,
}

impl JobOutputHandle {
    pub fn create(schema: SchemaRef) -> (Self, JobOutputStream) {
        let (tx, rx) = mpsc::channel(JOB_OUTPUT_ACTION_CHANNEL_SIZE);
        let handle = Self { sender: tx };
        let stream = JobOutputStream {
            schema,
            state: JobOutputState::Active {
                receiver: rx,
                inner: Box::pin(SelectAll::new()),
            },
        };
        (handle, stream)
    }
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
                Poll::Ready(Some(JobOutputAction::Fail { cause })) => {
                    self.state = JobOutputState::Failed;
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                        TaskStreamError::from(cause),
                    )))));
                }
                Poll::Ready(Some(JobOutputAction::AddStream { key, stream })) => {
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
                    Poll::Ready(None)
                }
                Poll::Ready(Some(result)) => Poll::Ready(Some(
                    result.map_err(|e| DataFusionError::External(Box::new(e))),
                )),
            },
            JobOutputState::Completed | JobOutputState::Failed => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for JobOutputStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
