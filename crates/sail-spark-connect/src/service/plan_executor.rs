use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::SessionContext;
use futures::stream;
use log::debug;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::resolve_and_execute_plan;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{
    read_stream, to_arrow_batch, Executor, ExecutorBatch, ExecutorMetadata, ExecutorOutput,
    ExecutorOutputStream,
};
use crate::session::SparkSession;
use crate::spark::connect::execute_plan_response::{
    ResponseType, ResultComplete, SqlCommandResult,
};
use crate::spark::connect::{
    relation, CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction,
    CreateDataFrameViewCommand, ExecutePlanResponse, GetResourcesCommand, LocalRelation, Relation,
    SqlCommand, StreamingQueryCommand, StreamingQueryCommandResult,
    StreamingQueryListenerBusCommand, StreamingQueryManagerCommand,
    StreamingQueryManagerCommandResult, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart, WriteStreamOperationStartResult,
};

pub struct ExecutePlanResponseStream {
    session_id: String,
    operation_id: String,
    inner: ExecutorOutputStream,
}

impl ExecutePlanResponseStream {
    pub fn new(session_id: String, operation_id: String, inner: ExecutorOutputStream) -> Self {
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
        self.inner.as_mut().poll_next(cx).map(|poll| {
            poll.map(|item| {
                let mut response = ExecutePlanResponse::default();
                response.session_id.clone_from(&self.session_id);
                response.server_side_session_id.clone_from(&self.session_id);
                response.operation_id.clone_from(&self.operation_id.clone());
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::ArrowBatch(batch) => {
                        response.response_type = Some(ResponseType::ArrowBatch(batch));
                    }
                    ExecutorBatch::SqlCommandResult(result) => {
                        response.response_type = Some(ResponseType::SqlCommandResult(*result));
                    }
                    ExecutorBatch::WriteStreamOperationStartResult(result) => {
                        response.response_type =
                            Some(ResponseType::WriteStreamOperationStartResult(*result));
                    }
                    ExecutorBatch::StreamingQueryCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryCommandResult(*result));
                    }
                    ExecutorBatch::StreamingQueryManagerCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryManagerCommandResult(*result));
                    }
                    ExecutorBatch::Schema(schema) => {
                        response.schema = Some(*schema);
                    }
                    ExecutorBatch::Complete => {
                        response.response_type =
                            Some(ResponseType::ResultComplete(ResultComplete::default()));
                    }
                }
                debug!("{response:?}");
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
    let spark = ctx.extension::<SparkSession>()?;
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
        Box::pin(rx),
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
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::WriteTo(
        write.try_into()?,
    )));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

pub(crate) async fn handle_execute_sql_command(
    ctx: &SessionContext,
    sql: SqlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let relation = if let Some(input) = sql.input {
        input
    } else {
        Relation {
            common: None,
            #[expect(deprecated)]
            rel_type: Some(relation::RelType::Sql(crate::spark::connect::Sql {
                query: sql.sql,
                args: sql.args,
                pos_args: sql.pos_args,
                named_arguments: sql.named_arguments,
                pos_arguments: sql.pos_arguments,
            })),
        }
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
    let result = ExecutorBatch::SqlCommandResult(Box::new(SqlCommandResult {
        relation: Some(relation),
    }));
    let mut output = vec![ExecutorOutput::new(result)];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
    ))
}

pub(crate) async fn handle_execute_write_stream_operation_start(
    ctx: &SessionContext,
    start: WriteStreamOperationStart,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let operation_id = metadata.operation_id.clone();
    let reattachable = metadata.reattachable;
    let query_name = start.query_name.clone();
    let plan = spec::Plan::Command(spec::CommandPlan::new(start.try_into()?));
    let plan = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = spark.job_runner().execute(ctx, plan).await?;
    let executor = Executor::new(metadata, stream);
    let _rx = executor.start()?;
    spark.add_executor(executor)?;
    let result = WriteStreamOperationStartResult {
        query_id: None,
        name: query_name,
        query_started_event_json: None,
    };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::WriteStreamOperationStartResult(Box::new(result)),
    )];
    if reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        Box::pin(stream::iter(output)),
    ))
}

pub(crate) async fn handle_execute_streaming_query_command(
    ctx: &SessionContext,
    stream: StreamingQueryCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::streaming_query_command::{
        AwaitTerminationCommand, Command, ExplainCommand,
    };
    use crate::spark::connect::streaming_query_command_result::{
        AwaitTerminationResult, ExceptionResult, ExplainResult, RecentProgressResult, ResultType,
        StatusResult,
    };

    let spark = ctx.extension::<SparkSession>()?;
    let StreamingQueryCommand { query_id, command } = stream;
    let query_id = query_id.required("streaming query ID")?;
    let command = command.required("streaming query command")?;
    let result_type = match command {
        Command::Status(true) => Some(ResultType::Status(StatusResult {
            status_message: "".to_string(),
            is_data_available: false,
            is_trigger_active: false,
            is_active: false,
        })),
        Command::LastProgress(true) | Command::RecentProgress(true) => {
            Some(ResultType::RecentProgress(RecentProgressResult {
                recent_progress_json: vec![],
            }))
        }
        Command::Stop(true) => None,
        Command::ProcessAllAvailable(true) => None,
        Command::Explain(ExplainCommand { extended: _ }) => {
            Some(ResultType::Explain(ExplainResult {
                result: "".to_string(),
            }))
        }
        Command::Exception(true) => Some(ResultType::Exception(ExceptionResult {
            exception_message: None,
            error_class: None,
            stack_trace: None,
        })),
        Command::AwaitTermination(AwaitTerminationCommand { timeout_ms: _ }) => {
            Some(ResultType::AwaitTermination(AwaitTerminationResult {
                terminated: false,
            }))
        }
        Command::Status(false)
        | Command::LastProgress(false)
        | Command::RecentProgress(false)
        | Command::Stop(false)
        | Command::ProcessAllAvailable(false)
        | Command::Exception(false) => {
            return Err(SparkError::invalid(format!(
                "invalid streaming query command: {command:?}"
            )))
        }
    };
    let result = StreamingQueryCommandResult {
        query_id: Some(query_id),
        result_type,
    };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::StreamingQueryCommandResult(Box::new(result)),
    )];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
    ))
}

pub(crate) async fn handle_execute_get_resources_command(
    _ctx: &SessionContext,
    _resource: GetResourcesCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("get resources command"))
}

pub(crate) async fn handle_execute_streaming_query_manager_command(
    ctx: &SessionContext,
    command: StreamingQueryManagerCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::streaming_query_manager_command::{
        AwaitAnyTerminationCommand, Command,
    };
    use crate::spark::connect::streaming_query_manager_command_result::{
        ActiveResult, AwaitAnyTerminationResult, ResultType, StreamingQueryInstance,
    };

    let spark = ctx.extension::<SparkSession>()?;
    let StreamingQueryManagerCommand { command } = command;
    let command = command.required("streaming query manager command")?;
    let result_type = match command {
        Command::Active(true) => Some(ResultType::Active(ActiveResult {
            active_queries: vec![],
        })),
        Command::GetQuery(_id) => Some(ResultType::Query(StreamingQueryInstance {
            id: None,
            name: None,
        })),
        Command::AwaitAnyTermination(AwaitAnyTerminationCommand { timeout_ms: _ }) => {
            Some(ResultType::AwaitAnyTermination(AwaitAnyTerminationResult {
                terminated: false,
            }))
        }
        Command::ResetTerminated(true) => Some(ResultType::ResetTerminated(true)),
        Command::AddListener(_) => {
            return Err(SparkError::NotImplemented("add listener".to_string()))
        }
        Command::RemoveListener(_) => {
            return Err(SparkError::NotImplemented("remove listener".to_string()))
        }
        Command::ListListeners(_) => {
            return Err(SparkError::NotImplemented("list listeners".to_string()))
        }
        Command::Active(false) | Command::ResetTerminated(false) => {
            return Err(SparkError::invalid(format!(
                "invalid streaming query manager command: {command:?}"
            )))
        }
    };
    let result = StreamingQueryManagerCommandResult { result_type };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::StreamingQueryManagerCommandResult(Box::new(result)),
    )];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
    ))
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

pub(crate) async fn handle_execute_streaming_query_listener_bus_command(
    _ctx: &SessionContext,
    _command: StreamingQueryListenerBusCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::NotImplemented(
        "streaming query listener bus".to_string(),
    ))
}

pub(crate) async fn handle_interrupt_all(ctx: &SessionContext) -> SparkResult<Vec<String>> {
    let spark = ctx.extension::<SparkSession>()?;
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
    let spark = ctx.extension::<SparkSession>()?;
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
    let spark = ctx.extension::<SparkSession>()?;
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
    let spark = ctx.extension::<SparkSession>()?;
    let executor = spark
        .get_executor(operation_id.as_str())?
        .ok_or_else(|| SparkError::invalid(format!("operation not found: {operation_id}")))?;
    if !executor.metadata.reattachable {
        return Err(SparkError::invalid(format!(
            "operation not reattachable: {operation_id}"
        )));
    }
    executor.pause_if_running().await?;
    executor.release(response_id)?;
    let rx = executor.start()?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        Box::pin(rx),
    ))
}

pub(crate) async fn handle_release_execute(
    ctx: &SessionContext,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<()> {
    let spark = ctx.extension::<SparkSession>()?;
    // Some operations may not have an executor (e.g. DDL statements),
    // so it is a no-op if the executor is not found.
    if let Some(executor) = spark.get_executor(operation_id.as_str())? {
        executor.release(response_id)?;
    }
    Ok(())
}
