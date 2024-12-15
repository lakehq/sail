use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::SessionContext;
use log::debug;
use sail_common::spec;
use sail_plan::resolve_and_execute_plan;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::error::{SparkError, SparkResult};
use crate::executor::{
    read_stream, to_arrow_batch, Executor, ExecutorBatch, ExecutorMetadata, ExecutorOutput,
};
use crate::session::SparkExtension;
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::{
    ResponseType, ResultComplete, SqlCommandResult,
};
use crate::spark::connect::{
    relation, CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction,
    CreateDataFrameViewCommand, ExecutePlanResponse, GetResourcesCommand, LocalRelation, Relation,
    SqlCommand, StreamingQueryCommand, StreamingQueryManagerCommand, WriteOperation,
    WriteOperationV2, WriteStreamOperationStart,
};

pub struct ExecutePlanResponseStream {
    session_id: String,
    operation_id: String,
    inner: ReceiverStream<ExecutorOutput>,
}

impl ExecutePlanResponseStream {
    pub(crate) fn new(
        session_id: String,
        operation_id: String,
        inner: ReceiverStream<ExecutorOutput>,
    ) -> Self {
        Self {
            session_id,
            operation_id,
            inner,
        }
    }
}

impl Stream for ExecutePlanResponseStream {
    type Item = Result<ExecutePlanResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ExecutePlanResponse, Status>>> {
        Pin::new(&mut self.inner).poll_next(cx).map(|poll| {
            poll.map(|item| {
                let mut response = ExecutePlanResponse::default();
                response.session_id.clone_from(&self.session_id);
                response.operation_id.clone_from(&self.operation_id.clone());
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::ArrowBatch(batch) => {
                        response.response_type = Some(ResponseType::ArrowBatch(batch));
                    }
                    ExecutorBatch::SqlCommandResult(result) => {
                        response.response_type = Some(ResponseType::SqlCommandResult(result));
                    }
                    ExecutorBatch::Schema(schema) => {
                        response.schema = Some(schema);
                    }
                    ExecutorBatch::Metrics(metrics) => {
                        response.metrics = Some(metrics);
                    }
                    ExecutorBatch::ObservedMetrics(metrics) => {
                        response.observed_metrics = metrics;
                    }
                    ExecutorBatch::Complete => {
                        response.response_type =
                            Some(ResponseType::ResultComplete(ResultComplete::default()));
                    }
                }
                debug!("{:?}", response);
                Ok(response)
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

enum ExecutePlanMode {
    /// Execute the plan lazily as the client reads the response stream.
    Lazy,
    /// Execute the plan eagerly and return an empty response stream.
    /// This is useful for executing command plans.
    EagerSilent,
}

async fn handle_execute_plan(
    ctx: &SessionContext,
    plan: spec::Plan,
    metadata: ExecutorMetadata,
    mode: ExecutePlanMode,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = SparkExtension::get(ctx)?;
    let operation_id = metadata.operation_id.clone();
    let plan = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = spark.job_runner().execute(ctx, plan).await?;
    let rx = match mode {
        ExecutePlanMode::Lazy => {
            let executor = Executor::new(metadata, stream);
            let rx = executor.start()?;
            spark.add_executor(executor)?;
            rx
        }
        ExecutePlanMode::EagerSilent => {
            let _ = read_stream(stream).await?;
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            if metadata.reattachable {
                tx.send(ExecutorOutput::complete()).await?;
            }
            ReceiverStream::new(rx)
        }
    };
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

pub(crate) async fn handle_execute_relation(
    ctx: &SessionContext,
    relation: Relation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = relation.try_into()?;
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::Lazy).await
}

pub(crate) async fn handle_execute_register_function(
    ctx: &SessionContext,
    udf: CommonInlineUserDefinedFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::RegisterFunction(
        udf.try_into()?,
    )));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_execute_write_operation(
    ctx: &SessionContext,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        write.try_into()?,
    )));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_execute_create_dataframe_view(
    ctx: &SessionContext,
    view: CreateDataFrameViewCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(view.try_into()?));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_execute_write_operation_v2(
    ctx: &SessionContext,
    write: WriteOperationV2,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        write.try_into()?,
    )));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_execute_sql_command(
    ctx: &SessionContext,
    sql: SqlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = SparkExtension::get(ctx)?;
    let relation = Relation {
        common: None,
        rel_type: Some(relation::RelType::Sql(sc::Sql {
            query: sql.sql,
            args: sql.args,
            pos_args: sql.pos_args,
        })),
    };
    let plan: spec::Plan = relation.clone().try_into()?;
    let relation = match plan {
        spec::Plan::Query(_) => relation,
        command @ spec::Plan::Command(_) => {
            let plan = resolve_and_execute_plan(ctx, spark.plan_config()?, command).await?;
            let stream = spark.job_runner().execute(ctx, plan).await?;
            let schema = stream.schema();
            let data = read_stream(stream).await?;
            let data = concat_batches(&schema, data.iter())?;
            Relation {
                common: None,
                rel_type: Some(relation::RelType::LocalRelation(LocalRelation {
                    data: Some(to_arrow_batch(&data)?.data),
                    schema: None,
                })),
            }
        }
    };
    let result = ExecutorBatch::SqlCommandResult(SqlCommandResult {
        relation: Some(relation),
    });
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tx.send(ExecutorOutput::new(result)).await?;
    if metadata.reattachable {
        tx.send(ExecutorOutput::complete()).await?;
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        ReceiverStream::new(rx),
    ))
}

pub(crate) async fn handle_execute_write_stream_operation_start(
    _ctx: &SessionContext,
    _start: WriteStreamOperationStart,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("write stream operation start"))
}

pub(crate) async fn handle_execute_streaming_query_command(
    _ctx: &SessionContext,
    _stream: StreamingQueryCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("streaming query command"))
}

pub(crate) async fn handle_execute_get_resources_command(
    _ctx: &SessionContext,
    _resource: GetResourcesCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("get resources command"))
}

pub(crate) async fn handle_execute_streaming_query_manager_command(
    _ctx: &SessionContext,
    _manager: StreamingQueryManagerCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("streaming query manager command"))
}

pub(crate) async fn handle_execute_register_table_function(
    ctx: &SessionContext,
    udtf: CommonInlineUserDefinedTableFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::RegisterTableFunction(udtf.try_into()?),
    ));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_interrupt_all(ctx: &SessionContext) -> SparkResult<Vec<String>> {
    let spark = SparkExtension::get(ctx)?;
    let mut results = vec![];
    for executor in spark.remove_all_executors()? {
        executor.pause_if_running().await?;
        results.push(executor.metadata.operation_id.clone());
    }
    Ok(results)
}

pub(crate) async fn handle_interrupt_tag(
    ctx: &SessionContext,
    tag: String,
) -> SparkResult<Vec<String>> {
    let spark = SparkExtension::get(ctx)?;
    let mut results = vec![];
    for executor in spark.remove_executors_by_tag(tag.as_str())? {
        executor.pause_if_running().await?;
        results.push(executor.metadata.operation_id.clone());
    }
    Ok(results)
}

pub(crate) async fn handle_interrupt_operation_id(
    ctx: &SessionContext,
    operation_id: String,
) -> SparkResult<Vec<String>> {
    let spark = SparkExtension::get(ctx)?;
    match spark.remove_executor(operation_id.as_str())? {
        Some(executor) => {
            executor.pause_if_running().await?;
            Ok(vec![executor.metadata.operation_id.clone()])
        }
        None => Ok(vec![]),
    }
}

pub(crate) async fn handle_reattach_execute(
    ctx: &SessionContext,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = SparkExtension::get(ctx)?;
    let executor = spark
        .get_executor(operation_id.as_str())?
        .ok_or_else(|| SparkError::invalid(format!("operation not found: {}", operation_id)))?;
    if !executor.metadata.reattachable {
        return Err(SparkError::invalid(format!(
            "operation not reattachable: {}",
            operation_id
        )));
    }
    executor.pause_if_running().await?;
    executor.release(response_id)?;
    let rx = executor.start()?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

pub(crate) async fn handle_release_execute(
    ctx: &SessionContext,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<()> {
    let spark = SparkExtension::get(ctx)?;
    // Some operations may not have an executor (e.g. DDL statements),
    // so it is a no-op if the executor is not found.
    if let Some(executor) = spark.get_executor(operation_id.as_str())? {
        executor.release(response_id)?;
    }
    Ok(())
}
