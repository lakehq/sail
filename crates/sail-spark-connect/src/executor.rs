use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::Stream;
use futures::stream::{StreamExt, TryStreamExt};
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
    ArrowBatch(ArrowBatch),
    SqlCommandResult(Box<SqlCommandResult>),
    WriteStreamOperationStartResult(Box<WriteStreamOperationStartResult>),
    StreamingQueryCommandResult(Box<StreamingQueryCommandResult>),
    StreamingQueryManagerCommandResult(Box<StreamingQueryManagerCommandResult>),
    CheckpointCommandResult(Box<CheckpointCommandResult>),
    Schema(Box<DataType>),
    Heartbeat,
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

pub type ExecutorOutputStream = Pin<Box<dyn Stream<Item = ExecutorOutput> + Send>>;
pub type ExecutorBatchStream = Pin<Box<dyn Stream<Item = SparkResult<ExecutorBatch>> + Send>>;

struct ExecutorBuffer {
    inner: VecDeque<ExecutorOutput>,
}

impl ExecutorBuffer {
    fn new() -> Self {
        // TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
        // TODO: limit the size based on serialized message size instead of element count
        Self {
            inner: VecDeque::with_capacity(128),
        }
    }

    fn add(&mut self, output: ExecutorOutput) {
        if self.inner.len() >= self.inner.capacity() {
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
    input: ExecutorInput,
    heartbeat_interval: Duration,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

enum ExecutorInput {
    Query(SendableRecordBatchStream),
    Operation(ExecutorBatchStream),
}

impl ExecutorTaskContext {
    fn new_query(stream: SendableRecordBatchStream, heartbeat_interval: Duration) -> Self {
        Self {
            input: ExecutorInput::Query(stream),
            heartbeat_interval,
            buffer: Arc::new(Mutex::new(ExecutorBuffer::new())),
        }
    }

    fn new_operation(stream: ExecutorBatchStream, heartbeat_interval: Duration) -> Self {
        Self {
            input: ExecutorInput::Operation(stream),
            heartbeat_interval,
            buffer: Arc::new(Mutex::new(ExecutorBuffer::new())),
        }
    }

    async fn next_query_batch(&mut self) -> SparkResult<Option<RecordBatch>> {
        let ExecutorInput::Query(stream) = &mut self.input else {
            return Err(SparkError::internal(
                "query batch requested for a non-query operation",
            ));
        };
        let schema = stream.schema();
        let span = Span::enter_with_local_parent("ExecutorTaskContext::next");
        tokio::select! {
            batch = stream.next().in_span(span) => Ok(batch.transpose()?),
            _ = tokio::time::sleep(self.heartbeat_interval) => {
                Ok(Some(RecordBatch::new_empty(schema)))
            }
        }
    }

    async fn next_operation_batch(&mut self) -> SparkResult<Option<ExecutorBatch>> {
        let ExecutorInput::Operation(stream) = &mut self.input else {
            return Err(SparkError::internal(
                "operation batch requested for a query operation",
            ));
        };
        tokio::select! {
            batch = stream.next() => batch.transpose(),
            _ = tokio::time::sleep(self.heartbeat_interval) => {
                Ok(Some(ExecutorBatch::Heartbeat))
            }
        }
    }

    fn save_output(&self, output: &ExecutorOutput) -> SparkResult<()> {
        let mut buffer = self.buffer.lock()?;
        buffer.add(output.clone());
        Ok(())
    }

    fn replay_outputs(&self) -> SparkResult<Vec<ExecutorOutput>> {
        let buffer = self.buffer.lock()?;
        Ok(buffer.iter().cloned().collect())
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
    ) -> Self {
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext::new_query(stream, heartbeat_interval),
                span: Span::enter_with_local_parent("Executor::new"),
            }),
        }
    }

    pub(crate) fn new_operation(
        metadata: ExecutorMetadata,
        stream: ExecutorBatchStream,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext::new_operation(stream, heartbeat_interval),
                span: Span::enter_with_local_parent("Executor::new_operation"),
            }),
        }
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: mpsc::Sender<ExecutorOutput>,
    ) -> SparkResult<()> {
        let replay = context.replay_outputs()?;
        let completed = replay
            .last()
            .is_some_and(|output| matches!(&output.batch, ExecutorBatch::Complete));
        for out in replay {
            tx.send(out).await?;
        }
        if completed {
            return Ok(());
        }

        if matches!(&context.input, ExecutorInput::Query(_)) {
            let schema = match &context.input {
                ExecutorInput::Query(stream) => stream.schema(),
                ExecutorInput::Operation(_) => {
                    return Err(SparkError::internal("query stream is not available"));
                }
            };
            let schema = to_spark_schema(schema)?;
            let out = ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema)));
            context.save_output(&out)?;
            tx.send(out).await?;

            let mut empty = true;
            while let Some(batch) = context.next_query_batch().await? {
                let batch = to_arrow_batch(&batch)?;
                let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
                context.save_output(&out)?;
                tx.send(out).await?;
                empty = false;
            }
            if empty {
                let ExecutorInput::Query(stream) = &context.input else {
                    return Err(SparkError::internal("query stream is not available"));
                };
                let batch = RecordBatch::new_empty(stream.schema());
                let batch = to_arrow_batch(&batch)?;
                let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
                context.save_output(&out)?;
                tx.send(out).await?;
            }
        } else {
            while let Some(batch) = context.next_operation_batch().await? {
                let out = ExecutorOutput::new(batch);
                context.save_output(&out)?;
                tx.send(out).await?;
            }
        }

        let out = ExecutorOutput::new(ExecutorBatch::Complete);
        context.save_output(&out)?;
        tx.send(out).await?;
        Ok(())
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<()>,
        tx: mpsc::Sender<ExecutorOutput>,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            x = Executor::run_internal(&mut context, tx) => x,
            _ = listener => return ExecutorTaskResult::Paused(context),
        };
        match out {
            Ok(()) => ExecutorTaskResult::Completed,
            Err(SparkError::SendError(_)) => ExecutorTaskResult::Paused(context),
            Err(e) => ExecutorTaskResult::Failed(e),
        }
    }

    pub(crate) fn start(&self) -> SparkResult<ReceiverStream<ExecutorOutput>> {
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
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            tokio::spawn(async move { Executor::run(context, listener, tx).in_span(span).await })
        };
        *state = ExecutorState::Running {
            task: ExecutorTask {
                notifier,
                handle,
                buffer,
            },
            span,
        };
        Ok(ReceiverStream::new(rx))
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

    pub(crate) fn release(&self, response_id: Option<String>) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running { task, span: _ } => &task.buffer,
            ExecutorState::Pending { context, span: _ } => &context.buffer,
            ExecutorState::Idle | ExecutorState::Failed(_) | ExecutorState::Pausing => {
                return Ok(());
            }
        };
        if let Some(response_id) = response_id {
            buffer.lock()?.remove_until(&response_id);
        }
        Ok(())
    }
}

pub(crate) async fn read_stream(
    stream: SendableRecordBatchStream,
) -> SparkResult<Vec<RecordBatch>> {
    stream.err_into().try_collect::<Vec<_>>().await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spark::connect::CachedRemoteRelation;

    async fn wait_for_buffer(executor: &Executor, minimum: usize) -> SparkResult<()> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let size = {
                    let state = executor.state.lock()?;
                    match state.deref() {
                        ExecutorState::Running { task, .. } => task.buffer.lock()?.inner.len(),
                        ExecutorState::Pending { context, .. } => {
                            context.buffer.lock()?.inner.len()
                        }
                        ExecutorState::Idle | ExecutorState::Pausing | ExecutorState::Failed(_) => {
                            0
                        }
                    }
                };
                if size >= minimum {
                    return Ok(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| SparkError::internal("timed out waiting for executor output"))?
    }

    #[tokio::test]
    async fn operation_replays_result_and_completion_after_pause() -> SparkResult<()> {
        let relation_id = "checkpoint-relation".to_string();
        let operation = futures::stream::iter([Ok(ExecutorBatch::CheckpointCommandResult(
            Box::new(CheckpointCommandResult {
                relation: Some(CachedRemoteRelation {
                    relation_id: relation_id.clone(),
                }),
            }),
        ))]);
        let executor = Executor::new_operation(
            ExecutorMetadata {
                operation_id: "checkpoint-operation".to_string(),
                tags: vec![],
                reattachable: true,
            },
            Box::pin(operation),
            Duration::from_secs(60),
        );

        let first = executor.start()?;
        wait_for_buffer(&executor, 2).await?;
        executor.pause_if_running().await?;
        drop(first);

        let replay = executor.start()?;
        let outputs = replay.collect::<Vec<_>>().await;
        assert_eq!(outputs.len(), 2);
        let ExecutorBatch::CheckpointCommandResult(result) = &outputs[0].batch else {
            return Err(SparkError::internal("checkpoint result was not replayed"));
        };
        assert_eq!(
            result
                .relation
                .as_ref()
                .map(|relation| relation.relation_id.as_str()),
            Some(relation_id.as_str())
        );
        assert!(matches!(&outputs[1].batch, ExecutorBatch::Complete));
        Ok(())
    }

    #[tokio::test]
    async fn operation_emits_heartbeats_while_pending() -> SparkResult<()> {
        let operation = futures::stream::pending::<SparkResult<ExecutorBatch>>();
        let executor = Executor::new_operation(
            ExecutorMetadata {
                operation_id: "pending-checkpoint-operation".to_string(),
                tags: vec![],
                reattachable: true,
            },
            Box::pin(operation),
            Duration::from_millis(1),
        );

        let mut output = executor.start()?;
        let heartbeat = tokio::time::timeout(Duration::from_secs(1), output.next())
            .await
            .map_err(|_| SparkError::internal("timed out waiting for operation heartbeat"))?
            .ok_or_else(|| SparkError::internal("operation ended before its heartbeat"))?;
        assert!(matches!(heartbeat.batch, ExecutorBatch::Heartbeat));
        executor.pause_if_running().await?;
        Ok(())
    }
}
