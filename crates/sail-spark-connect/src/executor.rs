// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::stream::{self, BoxStream, StreamExt};
use sail_common::runtime::spawn_stream_on;
use sail_common_datafusion::schema::{
    normalize_record_batch, record_batch_has_views, spark_arrow_schema,
};
use tokio::runtime::Handle;
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
    Completed {
        buffer: Arc<Mutex<ExecutorBuffer>>,
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
    outputs: BoxStream<'static, SparkResult<Option<ExecutorOutput>>>,
    primary: Handle,
    heartbeat_interval: Duration,
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

pub(crate) type CommandCompletionHandler =
    Box<dyn FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>> + Send>;

impl ExecutorTaskContext {
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
        primary: Handle,
    ) -> Self {
        let reattachable = metadata.reattachable;
        // Keep the batch stream and command completion on the query runtime across reattachments.
        let outputs = spawn_stream_on(Handle::current(), (), move |_| {
            Self::outputs(stream, mode, reattachable)
        });
        let buffer = if metadata.reattachable {
            ExecutorBuffer::new(EXECUTOR_BUFFER_CAPACITY)
        } else {
            ExecutorBuffer::new(0)
        };
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext {
                    outputs,
                    primary,
                    heartbeat_interval,
                    buffer: Arc::new(Mutex::new(buffer)),
                },
                span: Span::enter_with_local_parent("Executor::new"),
            }),
        }
    }

    pub(crate) fn completed(metadata: ExecutorMetadata, outputs: Vec<ExecutorOutput>) -> Self {
        let mut buffer = ExecutorBuffer::new(outputs.len());
        for output in outputs {
            buffer.add(output);
        }
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Completed {
                buffer: Arc::new(Mutex::new(buffer)),
            }),
        }
    }

    fn outputs(
        mut stream: SendableRecordBatchStream,
        mode: ExecutorMode,
        reattachable: bool,
    ) -> BoxStream<'static, SparkResult<Option<ExecutorOutput>>> {
        async_stream::try_stream! {
            let schema = stream.schema();
            match mode {
                ExecutorMode::Query => {
                    let spark_schema = to_spark_schema(schema.clone())?;
                    yield Some(ExecutorOutput::new(ExecutorBatch::Schema(Box::new(spark_schema))));
                    let mut empty = true;
                    while let Some(batch) = stream.next().await {
                        let batch = to_arrow_batch(&batch?)?;
                        yield Some(ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)));
                        empty = false;
                    }
                    if empty {
                        let batch = to_arrow_batch(&RecordBatch::new_empty(schema))?;
                        yield Some(ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)));
                    }
                }
                ExecutorMode::Command { completion } => {
                    let mut batches = vec![];
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                        // Bound read-ahead while the response stream is paused.
                        yield None;
                    }
                    if let Some(output) = completion(schema, batches)? {
                        yield Some(output);
                    }
                }
            }
            if reattachable {
                yield Some(ExecutorOutput::complete());
            }
        }
        .boxed()
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
    ) -> SparkResult<()> {
        let outputs = {
            let buffer = context.buffer.lock()?;
            buffer.iter().cloned().collect::<Vec<_>>()
        };
        for output in outputs {
            tx.send(Ok(output)).await?;
        }
        let heartbeat = tokio::time::sleep(context.heartbeat_interval);
        tokio::pin!(heartbeat);
        loop {
            let output = tokio::select! {
                item = context.outputs.next() => match item.transpose()? {
                    Some(Some(output)) => output,
                    Some(None) => continue,
                    None => return Ok(()),
                },
                _ = &mut heartbeat => {
                    ExecutorOutput::new(ExecutorBatch::Heartbeat)
                }
            };
            let complete = matches!(output.batch, ExecutorBatch::Complete);
            if complete {
                context.outputs = stream::empty().boxed();
            }
            ExecutorTaskContext::send_output(&context.buffer, tx, output).await?;
            if complete {
                return Ok(());
            }
            heartbeat
                .as_mut()
                .reset(tokio::time::Instant::now() + context.heartbeat_interval);
        }
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<()>,
        tx: mpsc::Sender<SparkResult<ExecutorOutput>>,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            x = Executor::run_internal(&mut context, &tx) => x,
            pause = listener => match pause {
                Ok(()) => return ExecutorTaskResult::Paused(context),
                Err(_) => Err(SparkError::internal("query cancelled because its executor was dropped")),
            },
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

    pub(crate) fn start(&self) -> SparkResult<BoxStream<'static, SparkResult<ExecutorOutput>>> {
        let mut state = self.state.lock()?;
        let (context, span) = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending { context, span } => (context, span),
            ExecutorState::Completed { buffer } => {
                let outputs = buffer.lock()?.iter().cloned().collect::<Vec<_>>();
                *state = ExecutorState::Completed { buffer };
                return Ok(Box::pin(stream::iter(outputs.into_iter().map(Ok))));
            }
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
        let primary = context.primary.clone();
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            primary.spawn(async move { Executor::run(context, listener, tx).in_span(span).await })
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
            ExecutorTaskResult::Completed => ExecutorState::Completed {
                buffer: task.buffer,
            },
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
            ExecutorState::Completed { buffer } => buffer,
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
        // Fallback incase view types weren't handled in planning/execution.
        let batch = if record_batch_has_views(batch) {
            // TODO: Look into passing the session's `spark.sql.execution.arrow.useLargeVarTypes` value.
            // TODO: Look into making `large_to_utf8_and_binary` configurable for Spark Connect clients that may not support `LargeUtf8` or `LargeBinary`.
            &normalize_record_batch(batch, false, false)?
        } else {
            batch
        };
        let cursor = Cursor::new(&mut output.data);
        let schema = spark_arrow_schema(batch.schema().as_ref());
        let mut writer = StreamWriter::try_new(cursor, &schema)?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use tokio::runtime::Builder;

    use super::*;

    #[test]
    fn heartbeat_continues_while_compute_is_blocked() -> Result<(), Box<dyn std::error::Error>> {
        let primary = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let compute = Builder::new_multi_thread().worker_threads(1).build()?;
        let (started, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let schema = Arc::new(Schema::empty());
        let input = RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::once(async move {
                let _ = started.send(());
                blocked
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(RecordBatch::new_empty(schema))
            }),
        );
        let executor = {
            let _guard = compute.enter();
            Executor::new(
                ExecutorMetadata {
                    operation_id: "test".into(),
                    tags: vec![],
                    reattachable: true,
                },
                Box::pin(input),
                Duration::from_millis(50),
                ExecutorMode::Query,
                primary.handle().clone(),
            )
        };
        let mut responses = executor.start()?;
        primary.block_on(async {
            ready.await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(output) = responses.next().await {
                    if matches!(output?.batch, ExecutorBatch::Heartbeat) {
                        return Ok(());
                    }
                }
                Err(SparkError::internal("expected an execution heartbeat"))
            })
            .await??;
            release.send(())?;
            Ok(())
        })
    }
}
