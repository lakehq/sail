use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;

use arrow::ipc::writer::StreamWriter;
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use framework_common::utils::rename_physical_plan;
use framework_plan::execute_logical_plan;
use framework_plan::resolver::plan::NamedPlan;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::StreamExt;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{ArrowBatch, Metrics, ObservedMetrics};
use crate::spark::connect::DataType;

#[derive(Clone, Debug)]
pub(crate) enum ExecutorBatch {
    ArrowBatch(ArrowBatch),
    Schema(DataType),
    #[allow(dead_code)]
    Metrics(Metrics),
    #[allow(dead_code)]
    ObservedMetrics(Vec<ObservedMetrics>),
    Complete,
}

#[derive(Clone, Debug)]
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
    Pending(ExecutorTaskContext),
    Running(ExecutorTask),
    Failed(SparkError),
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
            buffer: VecDeque::with_capacity(8192),
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
    pub(crate) fn new(metadata: ExecutorMetadata, context: ExecutorTaskContext) -> Self {
        Self {
            metadata,
            state: ExecutorState::Pending(context),
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

        let mut empty = true;
        while let Some(batch) = context.stream.next().await {
            let batch = batch?;
            let batch = to_arrow_batch(&batch).await?;
            let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
            context.add_output(out.clone());
            tx.send(out).await?;
            empty = false;
        }
        if empty {
            let batch = RecordBatch::new_empty(context.stream.schema());
            let batch = to_arrow_batch(&batch).await?;
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

    pub(crate) async fn start(&mut self) -> SparkResult<ReceiverStream<ExecutorOutput>> {
        let state = mem::replace(&mut self.state, ExecutorState::Idle);
        let context = match state {
            ExecutorState::Pending(context) => context,
            ExecutorState::Failed(e) => {
                self.state = ExecutorState::Failed(SparkError::internal(
                    "task failed due to a previous error",
                ));
                return Err(e);
            }
            ExecutorState::Idle => {
                self.state = state;
                return Err(SparkError::internal("task context not found for operation"));
            }
            ExecutorState::Running(_) => {
                self.state = state;
                return Err(SparkError::internal("task is already running"));
            }
        };
        let (tx, rx) = mpsc::channel(1);
        let (notifier, listener) = oneshot::channel();
        let handle = tokio::spawn(async move { Executor::run(context, listener, tx).await });
        self.state = ExecutorState::Running(ExecutorTask { notifier, handle });
        Ok(ReceiverStream::new(rx))
    }

    async fn notify(task: ExecutorTask) -> SparkResult<ExecutorState> {
        let _ = task.notifier.send(());
        match task.handle.await? {
            ExecutorTaskResult::Paused(context) => Ok(ExecutorState::Pending(context)),
            ExecutorTaskResult::Completed => Ok(ExecutorState::Idle),
            ExecutorTaskResult::Failed(e) => Ok(ExecutorState::Failed(e)),
        }
    }

    pub(crate) async fn release(&mut self, response_id: Option<String>) -> SparkResult<()> {
        let state = mem::replace(&mut self.state, ExecutorState::Idle);
        self.state = match state {
            ExecutorState::Running(task) => Executor::notify(task).await?,
            _ => state,
        };
        if let Some(response_id) = response_id {
            if let ExecutorState::Pending(context) = &mut self.state {
                context.remove_output_until(&response_id);
            }
        }
        Ok(())
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
    plan: NamedPlan,
) -> SparkResult<SendableRecordBatchStream> {
    let NamedPlan { plan, fields } = plan;
    let df = execute_logical_plan(ctx, plan).await?;
    let plan = df.create_physical_plan().await?;
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, fields.as_slice())?
    } else {
        plan
    };
    Ok(execute_stream(plan, ctx.task_ctx())?)
}

pub(crate) async fn read_stream(
    mut stream: SendableRecordBatchStream,
) -> SparkResult<Vec<RecordBatch>> {
    let mut output = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        output.push(batch);
    }
    Ok(output)
}

pub(crate) async fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
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
pub(crate) async fn execute_query(
    ctx: &SessionContext,
    query: &str,
) -> SparkResult<Vec<RecordBatch>> {
    use std::collections::HashMap;
    use std::sync::Arc;

    use framework_plan::config::PlanConfig;
    use framework_plan::resolver::PlanResolver;

    use crate::spark::connect::relation::RelType;
    use crate::spark::connect::{Relation, Sql};

    let config = PlanConfig::default();
    let relation = Relation {
        common: None,
        rel_type: Some(RelType::Sql(Sql {
            query: query.to_string(),
            args: HashMap::new(),
            pos_args: vec![],
        })),
    };
    let plan = PlanResolver::new(ctx, Arc::new(config))
        .resolve_named_plan(relation.try_into()?)
        .await?;
    let stream = execute_plan(ctx, plan).await?;
    read_stream(stream).await
}
