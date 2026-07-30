use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::Stream;
use futures::stream::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{ArrowBatch, SqlCommandResult};
use crate::spark::connect::{
    CheckpointCommandResult, DataType, StreamingQueryCommandResult,
    StreamingQueryManagerCommandResult, WriteStreamOperationStartResult,
};

#[derive(Clone, Debug)]
pub enum ExecutorBatch {
    Heartbeat,
    ArrowBatch(ArrowBatch),
    SqlCommandResult(Box<SqlCommandResult>),
    WriteStreamOperationStartResult(Box<WriteStreamOperationStartResult>),
    StreamingQueryCommandResult(Box<StreamingQueryCommandResult>),
    StreamingQueryManagerCommandResult(Box<StreamingQueryManagerCommandResult>),
    CheckpointCommandResult(Box<CheckpointCommandResult>),
    Schema(Box<DataType>),
    Complete,
}

#[derive(Clone, Debug)]
pub struct ExecutorOutput {
    pub(crate) id: String,
    pub(crate) batch: ExecutorBatch,
}

impl ExecutorOutput {
    pub fn new(batch: ExecutorBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            batch,
        }
    }

    pub fn complete() -> Self {
        Self::new(ExecutorBatch::Complete)
    }
}

pub type ExecutorOutputStream = Pin<Box<dyn Stream<Item = SparkResult<ExecutorOutput>> + Send>>;

struct ExecutorBuffer {
    capacity: usize,
    inner: VecDeque<ExecutorOutput>,
}

// TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
// TODO: limit the size based on serialized message size instead of element count
const EXECUTOR_BUFFER_CAPACITY: usize = 128;

impl ExecutorBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    fn add(&mut self, output: ExecutorOutput) {
        if self.capacity == 0 {
            return;
        }
        if self.inner.len() >= self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(output);
    }

    fn remove_until(&mut self, id: &str) {
        let index = self.inner.iter().position(|x| x.id == id);
        if let Some(index) = index {
            self.inner.drain(0..=index);
        }
    }

    fn iter(&self) -> impl Iterator<Item = &ExecutorOutput> {
        self.inner.iter()
    }
}

pub(crate) struct ExecutorMetadata {
    pub(crate) operation_id: String,
    pub(crate) tags: Vec<String>,
    pub(crate) reattachable: bool,
}

pub(crate) struct Executor {
    pub(crate) metadata: ExecutorMetadata,
    state: Mutex<ExecutorState>,
}

enum ExecutorState {
    Idle,
    Pending {
        context: ExecutorTaskContext,
        span: Span,
    },
    Running {
        task: ExecutorTask,
        span: Span,
    },
    Pausing,
    Failed(SparkError),
}

struct ExecutorTask {
    notifier: oneshot::Sender<()>,
    handle: JoinHandle<ExecutorTaskResult>,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

struct ExecutorTaskContext {
    stream: SendableRecordBatchStream,
    heartbeat_interval: Duration,
    state: ExecutorTaskState,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

pub(crate) enum ExecutorMode {
    Query,
    Command {
        completion: CommandCompletionHandler,
    },
}

impl ExecutorMode {
    pub(crate) fn command() -> Self {
        Self::command_with_completion(|_, _| Ok(None))
    }

    pub(crate) fn command_with_completion(
        completion: impl FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>>
        + Send
        + 'static,
    ) -> Self {
        Self::Command {
            completion: Box::new(completion),
        }
    }
}

pub(crate) enum ExecutorTaskState {
    Query,
    Command {
        batches: Vec<RecordBatch>,
        completion: Option<CommandCompletionHandler>,
    },
}

enum ExecutorTaskItem {
    Batch(Option<RecordBatch>),
    Heartbeat,
}

pub(crate) type CommandCompletionHandler =
    Box<dyn FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>> + Send>;

impl ExecutorTaskContext {
    async fn next(
        stream: &mut SendableRecordBatchStream,
        heartbeat_interval: Duration,
    ) -> SparkResult<ExecutorTaskItem> {
        let span = Span::enter_with_local_parent("ExecutorTaskContext::next");
        tokio::select! {
            batch = stream.next().in_span(span) => Ok(ExecutorTaskItem::Batch(batch.transpose()?)),
            _ = tokio::time::sleep(heartbeat_interval) => {
                // FIXME: non-reattachable clients cannot refresh session activity by releasing heartbeat responses
                Ok(ExecutorTaskItem::Heartbeat)
            }
        }
    }

    async fn send_output(
        buffer: &Arc<Mutex<ExecutorBuffer>>,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        output: ExecutorOutput,
    ) -> SparkResult<()> {
        {
            let mut buffer = buffer.lock()?;
            buffer.add(output.clone());
        }
        tx.send(Ok(output)).await?;
        Ok(())
    }
}

enum ExecutorTaskResult {
    Paused(ExecutorTaskContext),
    Failed(SparkError),
    Completed,
}

impl Executor {
    pub(crate) fn new(
        metadata: ExecutorMetadata,
        stream: SendableRecordBatchStream,
        heartbeat_interval: Duration,
        mode: ExecutorMode,
    ) -> Self {
        let state = match mode {
            ExecutorMode::Query => ExecutorTaskState::Query,
            ExecutorMode::Command { completion } => ExecutorTaskState::Command {
                batches: vec![],
                completion: Some(completion),
            },
        };
        let buffer = if metadata.reattachable {
            ExecutorBuffer::new(EXECUTOR_BUFFER_CAPACITY)
        } else {
            ExecutorBuffer::new(0)
        };
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext {
                    stream,
                    heartbeat_interval,
                    state,
                    buffer: Arc::new(Mutex::new(buffer)),
                },
                span: Span::enter_with_local_parent("Executor::new"),
            }),
        }
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
    ) -> SparkResult<()> {
        let outputs = {
            let buffer = context.buffer.lock()?;
            buffer.iter().cloned().collect::<Vec<_>>()
        };
        for output in outputs {
            tx.send(Ok(output)).await?;
        }
        match &mut context.state {
            ExecutorTaskState::Query => {
                let schema = to_spark_schema(context.stream.schema())?;
                ExecutorTaskContext::send_output(
                    &context.buffer,
                    tx,
                    ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema))),
                )
                .await?;

                let mut empty = true;
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            let batch = to_arrow_batch(&batch)?;
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                            )
                            .await?;
                            empty = false;
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                if empty {
                    let batch = RecordBatch::new_empty(context.stream.schema());
                    let batch = to_arrow_batch(&batch)?;
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                    )
                    .await?;
                }
                if reattachable {
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::complete(),
                    )
                    .await?;
                }
            }
            ExecutorTaskState::Command {
                batches,
                completion,
            } => {
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            batches.push(batch);
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                let schema = context.stream.schema();
                let output = completion
                    .take()
                    .map(|completion| completion(schema, mem::take(batches)))
                    .transpose()?
                    .flatten();
                if let Some(output) = output {
                    ExecutorTaskContext::send_output(&context.buffer, tx, output).await?;
                }
                if reattachable {
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::complete(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<()>,
        tx: mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            x = Executor::run_internal(&mut context, &tx, reattachable) => x,
            _ = listener => return ExecutorTaskResult::Paused(context),
        };
        match out {
            Ok(()) => ExecutorTaskResult::Completed,
            Err(SparkError::SendError(_)) => ExecutorTaskResult::Paused(context),
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                // TODO: track the original error in the task result
                ExecutorTaskResult::Failed(SparkError::internal(
                    "task failed while executing the plan",
                ))
            }
        }
    }

    pub(crate) fn start(&self) -> SparkResult<ExecutorOutputStream> {
        let mut state = self.state.lock()?;
        let (context, span) = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending { context, span } => (context, span),
            ExecutorState::Failed(e) => {
                *state = ExecutorState::Failed(SparkError::internal(
                    "task failed due to a previous error",
                ));
                return Err(e);
            }
            x @ ExecutorState::Idle => {
                *state = x;
                return Err(SparkError::internal("task context not found for operation"));
            }
            x @ ExecutorState::Running { .. } => {
                *state = x;
                return Err(SparkError::internal("task is already running"));
            }
            x @ ExecutorState::Pausing => {
                *state = x;
                return Err(SparkError::internal("task is being paused"));
            }
        };
        let (tx, rx) = mpsc::channel(1);
        let (notifier, listener) = oneshot::channel();
        let buffer = Arc::clone(&context.buffer);
        let reattachable = self.metadata.reattachable;
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            tokio::spawn(async move {
                Executor::run(context, listener, tx, reattachable)
                    .in_span(span)
                    .await
            })
        };
        *state = ExecutorState::Running {
            task: ExecutorTask {
                notifier,
                handle,
                buffer,
            },
            span,
        };
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    pub(crate) async fn pause_if_running(&self) -> SparkResult<()> {
        let (task, span) = {
            let mut state = self.state.lock()?;
            match mem::replace(state.deref_mut(), ExecutorState::Idle) {
                ExecutorState::Running { task, span } => {
                    *state = ExecutorState::Pausing;
                    (task, span)
                }
                x => {
                    *state = x;
                    return Ok(());
                }
            }
        };
        let _ = task.notifier.send(());
        let state = match task.handle.await? {
            ExecutorTaskResult::Paused(context) => ExecutorState::Pending { context, span },
            ExecutorTaskResult::Completed => ExecutorState::Idle,
            ExecutorTaskResult::Failed(e) => ExecutorState::Failed(e),
        };
        *(self.state.lock()?) = state;
        Ok(())
    }

    pub(crate) fn release(&self, response_id: String) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running { task, span: _ } => &task.buffer,
            ExecutorState::Pending { context, span: _ } => &context.buffer,
            ExecutorState::Idle | ExecutorState::Failed(_) | ExecutorState::Pausing => {
                return Ok(());
            }
        };
        buffer.lock()?.remove_until(&response_id);
        Ok(())
    }
}

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}
