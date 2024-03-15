use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::StreamExt;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::plan::to_arrow_batch;
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{ArrowBatch, Metrics, ObservedMetrics};
use crate::spark::connect::DataType;

#[derive(Clone)]
pub(crate) enum ExecutorBatch {
    ArrowBatch(ArrowBatch),
    Schema(DataType),
    Metrics(Metrics),
    ObservedMetrics(Vec<ObservedMetrics>),
    Complete,
}

#[derive(Clone)]
pub(crate) struct ExecutorOutput {
    pub(crate) id: String,
    pub(crate) batch: ExecutorBatch,
}

impl ExecutorOutput {
    pub(crate) fn new(batch: ExecutorBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            batch,
        }
    }
}

pub(crate) struct ExecutorMetadata {
    pub(crate) operation_id: String,
    pub(crate) tags: Vec<String>,
    pub(crate) reattachable: bool,
}

pub(crate) struct Executor {
    pub(crate) metadata: ExecutorMetadata,
    state: ExecutorState,
}

enum ExecutorState {
    Idle,
    Running(ExecutorTask),
    Paused(ExecutorTaskContext),
    Failed(SparkError),
    Completed,
}

struct ExecutorTask {
    notifier: oneshot::Sender<()>,
    handle: JoinHandle<ExecutorTaskResult>,
}

pub(crate) struct ExecutorTaskContext {
    stream: SendableRecordBatchStream,
    buffer: VecDeque<ExecutorOutput>,
}

impl ExecutorTaskContext {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            // TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
            // TODO: limit the size based on serialized message size instead of element count
            buffer: VecDeque::with_capacity(128),
        }
    }

    pub(crate) fn add_output(&mut self, output: ExecutorOutput) {
        if self.buffer.len() == self.buffer.capacity() {
            self.buffer.pop_front();
        }
        self.buffer.push_back(output);
    }

    pub(crate) fn remove_output_until(&mut self, id: &str) {
        let index = self.buffer.iter().position(|x| x.id == id);
        if let Some(index) = index {
            self.buffer.drain(0..=index);
        }
    }

    pub(crate) fn iter_output(&self) -> impl Iterator<Item = &ExecutorOutput> {
        self.buffer.iter()
    }
}

enum ExecutorTaskResult {
    Paused(ExecutorTaskContext),
    Failed(SparkError),
    Completed,
}

impl Executor {
    pub(crate) fn new(metadata: ExecutorMetadata) -> Self {
        Self {
            metadata,
            state: ExecutorState::Idle,
        }
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: mpsc::Sender<ExecutorOutput>,
    ) -> SparkResult<()> {
        for output in context.iter_output() {
            tx.send(output.clone()).await?;
        }
        let schema = to_spark_schema(context.stream.schema())?;
        let out = ExecutorOutput::new(ExecutorBatch::Schema(schema));
        context.add_output(out.clone());
        tx.send(out).await?;
        while let Some(batch) = context.stream.next().await {
            let batch = batch?;
            let batch = to_arrow_batch(&batch, context.stream.schema()).await?;
            let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
            context.add_output(out.clone());
            tx.send(out).await?;
        }
        let out = ExecutorOutput::new(ExecutorBatch::Complete);
        context.add_output(out.clone());
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

    fn take_task(&mut self) -> SparkResult<ExecutorTask> {
        let state = mem::replace(&mut self.state, ExecutorState::Idle);
        match state {
            ExecutorState::Running(task) => Ok(task),
            _ => {
                self.state = state;
                Err(SparkError::internal("task not found for operation"))
            }
        }
    }

    fn take_task_context(&mut self) -> SparkResult<ExecutorTaskContext> {
        let state = mem::replace(&mut self.state, ExecutorState::Idle);
        match state {
            ExecutorState::Paused(context) => Ok(context),
            ExecutorState::Failed(e) => Err(e),
            _ => {
                self.state = state;
                Err(SparkError::internal("task context not found for operation"))
            }
        }
    }

    pub(crate) async fn start(
        &mut self,
        context: ExecutorTaskContext,
    ) -> SparkResult<ReceiverStream<ExecutorOutput>> {
        match self.state {
            ExecutorState::Idle => {}
            ExecutorState::Running(_) | ExecutorState::Paused(_) => {
                return Err(SparkError::internal("operation already started"));
            }
            ExecutorState::Failed(_) => {
                return Err(SparkError::internal("operation already failed"));
            }
            ExecutorState::Completed => {
                return Err(SparkError::invalid("operation already completed"));
            }
        }
        let (tx, rx) = mpsc::channel(1);
        let (notifier, listener) = oneshot::channel();
        let handle = tokio::spawn(async move { Executor::run(context, listener, tx).await });
        self.state = ExecutorState::Running(ExecutorTask { notifier, handle });
        Ok(ReceiverStream::new(rx))
    }

    pub(crate) async fn pause(&mut self, response_id: Option<String>) -> SparkResult<()> {
        let task = match self.take_task() {
            Ok(task) => task,
            Err(_) => return Ok(()),
        };
        let _ = task.notifier.send(());
        match task.handle.await? {
            ExecutorTaskResult::Paused(mut context) => {
                if let Some(response_id) = response_id {
                    context.remove_output_until(&response_id);
                }
                self.state = ExecutorState::Paused(context);
            }
            ExecutorTaskResult::Completed => {
                self.state = ExecutorState::Completed;
            }
            ExecutorTaskResult::Failed(e) => {
                self.state = ExecutorState::Failed(e);
            }
        };
        Ok(())
    }

    pub(crate) async fn restart(
        &mut self,
        response_id: Option<String>,
    ) -> SparkResult<ReceiverStream<ExecutorOutput>> {
        self.pause(response_id).await?;
        let context = self.take_task_context()?;
        self.start(context).await
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        if let ExecutorState::Running(ExecutorTask { handle, .. }) = &self.state {
            handle.abort();
        }
    }
}

pub(crate) async fn execute_plan(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> SparkResult<SendableRecordBatchStream> {
    let plan = ctx.state().create_physical_plan(plan).await?;
    Ok(execute_stream(plan, Arc::new(TaskContext::default()))?)
}
