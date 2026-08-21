use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::future::FutureExt;
use futures::stream;
use log::{debug, warn};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::resolve_and_execute_plan;
use tonic::Status;
use tonic::codegen::tokio_stream::Stream;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{
    Executor, ExecutorBatch, ExecutorMetadata, ExecutorMode, ExecutorOutput, ExecutorOutputStream,
    to_arrow_batch,
};
use crate::session::SparkSession;
use crate::spark::connect::execute_plan_response::{
    ResponseType, ResultComplete, SqlCommandResult,
};
use crate::spark::connect::{
    CachedRemoteRelation, CheckpointCommand, CheckpointCommandResult,
    CommonInlineUserDefinedDataSource, CommonInlineUserDefinedFunction,
    CommonInlineUserDefinedTableFunction, CreateDataFrameViewCommand, ExecutePlanResponse,
    GetResourcesCommand, LocalRelation, MergeIntoTableCommand, Relation,
    RemoveCachedRemoteRelationCommand, SqlCommand, StreamingQueryCommand,
    StreamingQueryCommandResult, StreamingQueryListenerBusCommand, StreamingQueryManagerCommand,
    StreamingQueryManagerCommandResult, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart, WriteStreamOperationStartResult, relation,
};
use crate::streaming::timeout_millis;

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
                let item = item.map_err(Status::from)?;
                let mut response = ExecutePlanResponse::default();
                response.session_id.clone_from(&self.session_id);
                response.server_side_session_id.clone_from(&self.session_id);
                response.operation_id.clone_from(&self.operation_id.clone());
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::Heartbeat => {}
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
                    ExecutorBatch::CheckpointCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::CheckpointCommandResult(*result));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SparkConnectOutputEncodingPolicy {
    expand_views: bool,
    use_large_var_types: bool,
}

fn apply_spark_connect_output_encoding(
    plan: Arc<dyn ExecutionPlan>,
    policy: SparkConnectOutputEncodingPolicy,
) -> SparkResult<Arc<dyn ExecutionPlan>> {
    if !policy.expand_views {
        return Ok(plan);
    }

    let mut changed = false;
    let input_schema = plan.schema();
    let expressions = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column = Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>;
            let target_type = match field.data_type() {
                DataType::Utf8View if policy.use_large_var_types => Some(DataType::LargeUtf8),
                DataType::Utf8View => Some(DataType::Utf8),
                DataType::BinaryView if policy.use_large_var_types => Some(DataType::LargeBinary),
                DataType::BinaryView => Some(DataType::Binary),
                _ => None,
            };
            let expression = if let Some(target_type) = target_type {
                changed = true;
                let target_field = Arc::new(field.as_ref().clone().with_data_type(target_type));
                Arc::new(CastExpr::new_with_target_field(column, target_field, None))
                    as Arc<dyn PhysicalExpr>
            } else {
                column
            };
            (expression, field.name().to_string())
        })
        .collect::<Vec<_>>();

    if changed {
        Ok(Arc::new(ProjectionExec::try_new(expressions, plan)?))
    } else {
        Ok(plan)
    }
}

async fn handle_execute_plan(
    ctx: &SessionContext,
    plan: spec::Plan,
    metadata: ExecutorMetadata,
    mode: ExecutorMode,
) -> SparkResult<ExecutePlanResponseStream> {
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let query_output = matches!(&mode, ExecutorMode::Query);
    let plan_config = spark.plan_config()?;
    let output_policy = SparkConnectOutputEncodingPolicy {
        expand_views: spark.options().expand_views_at_output,
        use_large_var_types: plan_config.arrow_use_large_var_types,
    };
    let (plan, _) = resolve_and_execute_plan(ctx, plan_config, plan).await?;
    let plan = if query_output {
        apply_spark_connect_output_encoding(plan, output_policy)?
    } else {
        plan
    };
    let stream = {
        let span = Span::enter_with_parent("JobRunner::execute", &span);
        service.runner().execute(ctx, plan).in_span(span).await?
    };
    let _guard = span.set_local_parent();
    let executor = Executor::new(
        metadata,
        stream,
        spark.options().execution_heartbeat_interval,
        mode,
    );
    let rx = executor.start()?;
    spark.add_executor(executor)?;
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
    handle_execute_plan(ctx, plan, metadata, ExecutorMode::Query).await
}

pub(crate) async fn handle_execute_register_function(
    ctx: &SessionContext,
    udf: CommonInlineUserDefinedFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::RegisterFunction(
        udf.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_write_operation(
    ctx: &SessionContext,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        write.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_create_dataframe_view(
    ctx: &SessionContext,
    view: CreateDataFrameViewCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(view.try_into()?));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_write_operation_v2(
    ctx: &SessionContext,
    write: WriteOperationV2,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::WriteTo(
        write.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_merge_into_table_command(
    ctx: &SessionContext,
    command: MergeIntoTableCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(command.try_into()?));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

/// Handles execution of a SQL command.
/// If a string is sent over we convert it to a relation then convert it to a plan, then execute it.
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
    match plan {
        spec::Plan::Command(command) => {
            let mode = ExecutorMode::command_with_completion(move |schema, data| {
                let data = concat_batches(&schema, data.iter())?;
                let relation = Relation {
                    common: None,
                    rel_type: Some(relation::RelType::LocalRelation(LocalRelation {
                        data: Some(to_arrow_batch(&data)?.data),
                        schema: None,
                    })),
                };
                Ok(Some(ExecutorOutput::new(ExecutorBatch::SqlCommandResult(
                    Box::new(SqlCommandResult {
                        relation: Some(relation),
                    }),
                ))))
            });
            handle_execute_plan(ctx, spec::Plan::Command(command), metadata, mode).await
        }
        spec::Plan::Query(_) => {
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
                Box::pin(stream::iter(output.into_iter().map(Ok))),
            ))
        }
    }
}

pub(crate) async fn handle_execute_write_stream_operation_start(
    ctx: &SessionContext,
    start: WriteStreamOperationStart,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let reattachable = metadata.reattachable;
    let query_name = start.query_name.clone();
    let plan = spec::Plan::Command(spec::CommandPlan::new(start.try_into()?));
    let (plan, info) = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = service.runner().execute(ctx, plan).await?;
    let id = spark.start_streaming_query(query_name.clone(), info, stream)?;
    let result = WriteStreamOperationStartResult {
        query_id: Some(id.into()),
        name: query_name,
        // The event is for the client-side listener, which is not supported yet.
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
        Box::pin(stream::iter(output.into_iter().map(Ok))),
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
        Command::Status(true) => {
            let status = spark.get_streaming_query_status(&query_id.clone().into())?;
            Some(ResultType::Status(StatusResult {
                status_message: status.message,
                is_data_available: true,
                is_trigger_active: true,
                is_active: status.is_active,
            }))
        }
        Command::LastProgress(true) | Command::RecentProgress(true) => {
            Some(ResultType::RecentProgress(RecentProgressResult {
                recent_progress_json: vec![],
            }))
        }
        Command::Stop(true) => {
            spark.stop_streaming_query(&query_id.clone().into())?;
            None
        }
        Command::ProcessAllAvailable(true) => None,
        Command::Explain(ExplainCommand { extended }) => {
            let mut result = spark.explain_streaming_query(&query_id.clone().into(), extended)?;
            while result.ends_with('\n') {
                result.pop();
            }
            Some(ResultType::Explain(ExplainResult { result }))
        }
        Command::Exception(true) => {
            let (message, class) = if let Some(throwable) =
                spark.get_streaming_query_exception(&query_id.clone().into())?
            {
                (
                    Some(throwable.message().to_string()),
                    Some(throwable.class_name().to_string()),
                )
            } else {
                (None, None)
            };
            Some(ResultType::Exception(ExceptionResult {
                exception_message: message,
                error_class: class,
                stack_trace: None,
            }))
        }
        Command::AwaitTermination(AwaitTerminationCommand { timeout_ms }) => {
            let timeout = timeout_ms.map(timeout_millis).transpose()?;
            let handle = spark.await_streaming_query(&query_id.clone().into())?;
            let terminated = if let Some(handle) = handle {
                handle.terminated(timeout).await?
            } else {
                true
            };
            Some(ResultType::AwaitTermination(AwaitTerminationResult {
                terminated,
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
            )));
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
        Box::pin(stream::iter(output.into_iter().map(Ok))),
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
        Command::Active(true) => {
            let active_queries = spark
                .list_active_streaming_queries()?
                .into_iter()
                .map(|(id, status)| StreamingQueryInstance {
                    id: Some(id.into()),
                    name: Some(status.name),
                })
                .collect();
            Some(ResultType::Active(ActiveResult { active_queries }))
        }
        Command::GetQuery(id) => {
            let (id, status) = spark.find_streaming_query_by_query_id(&id)?;
            Some(ResultType::Query(StreamingQueryInstance {
                id: Some(id.into()),
                name: Some(status.name),
            }))
        }
        Command::AwaitAnyTermination(AwaitAnyTerminationCommand { timeout_ms }) => {
            let timeout = timeout_ms.map(timeout_millis).transpose()?;
            let handles = spark.await_streaming_queries()?;
            let terminated = handles.any_terminated(timeout).await?;
            Some(ResultType::AwaitAnyTermination(AwaitAnyTerminationResult {
                terminated,
            }))
        }
        Command::ResetTerminated(true) => {
            spark.reset_terminated_streaming_queries()?;
            Some(ResultType::ResetTerminated(true))
        }
        Command::AddListener(_) => {
            return Err(SparkError::NotImplemented("add listener".to_string()));
        }
        Command::RemoveListener(_) => {
            return Err(SparkError::NotImplemented("remove listener".to_string()));
        }
        Command::ListListeners(_) => {
            return Err(SparkError::NotImplemented("list listeners".to_string()));
        }
        Command::Active(false) | Command::ResetTerminated(false) => {
            return Err(SparkError::invalid(format!(
                "invalid streaming query manager command: {command:?}"
            )));
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
        Box::pin(stream::iter(output.into_iter().map(Ok))),
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
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
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

pub(crate) async fn handle_execute_checkpoint_command(
    ctx: &SessionContext,
    checkpoint: CheckpointCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let CheckpointCommand {
        relation,
        local: _,
        eager,
        storage_level,
    } = checkpoint;
    if !eager {
        return Err(SparkError::unsupported("lazy DataFrame checkpoint"));
    }
    if storage_level.is_some() {
        return Err(SparkError::unsupported(
            "checkpoint StorageLevel; Sail checkpoints are object-store backed",
        ));
    }
    let relation = relation.required("checkpoint relation")?;
    let query: spec::QueryPlan = relation.try_into()?;
    let relation_id = uuid::Uuid::new_v4().to_string();
    let plan = spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::RemoteCheckpoint {
            relation_id: relation_id.clone(),
            input: Box::new(query),
        },
    ));
    let mode = ExecutorMode::command_with_completion(move |_, _| {
        Ok(Some(ExecutorOutput::new(
            ExecutorBatch::CheckpointCommandResult(Box::new(CheckpointCommandResult {
                relation: Some(CachedRemoteRelation { relation_id }),
            })),
        )))
    });
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_remove_cached_remote_relation_command(
    ctx: &SessionContext,
    _command: RemoveCachedRemoteRelationCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    // TODO: Remove checkpoint data on a best-effort basis when the client releases the relation.
    // Checkpoints have session lifetime so plans can safely share their immutable relation ID.
    // The registry and object-store namespace are cleared together after the job runner stops.
    let output = metadata
        .reattachable
        .then(ExecutorOutput::complete)
        .into_iter();
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
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
    if let Some(response_id) = response_id {
        executor.release(response_id)?;
    }
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
    let spark = ctx.extension::<SparkSession>()?;
    let executor = spark.get_executor(operation_id.as_str())?;
    // TODO: clean up non-reattachable executors which the client does not release explicitly
    let Some(executor) = executor.filter(|executor| executor.metadata.reattachable) else {
        return Ok(());
    };
    if let Some(response_id) = response_id {
        executor.release(response_id)?;
    } else if let Some(executor) = spark.remove_executor(operation_id.as_str())? {
        executor.pause_if_running().await?;
    }
    Ok(())
}

pub(crate) async fn handle_execute_register_datasource(
    ctx: &SessionContext,
    datasource: CommonInlineUserDefinedDataSource,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::common_inline_user_defined_data_source::DataSource;

    log::info!(
        "RegisterDataSource handler called for datasource: {}",
        datasource.name
    );

    let spark = ctx.extension::<SparkSession>()?;
    let name = datasource.name.clone();

    // Extract the pickled Python datasource class
    let command = match datasource.data_source {
        Some(DataSource::PythonDataSource(pds)) => pds.command,
        None => {
            return Err(SparkError::invalid(
                "RegisterDataSource requires a python_data_source",
            ));
        }
    };

    // Register in the session-scoped TableFormatRegistry with embedded pickled bytes
    {
        use std::sync::Arc;

        use sail_common_datafusion::datasource::TableFormatRegistry;
        use sail_data_source::formats::python::PythonTableFormat;

        // Register format in session's TableFormatRegistry with embedded pickled class
        // This provides session isolation - the format is only visible to this session
        match ctx.extension::<TableFormatRegistry>() {
            Ok(registry) => {
                let format = Arc::new(PythonTableFormat::with_pickled_class(name.clone(), command));
                // Ignore error if already registered (allows re-registration to update)
                if let Err(e) = registry.register(format) {
                    warn!("Failed to register python datasource {}: {}", name, e);
                }
                log::info!("Registered session-scoped datasource: {}", name);
            }
            _ => {
                return Err(SparkError::internal(
                    "TableFormatRegistry not found in session context",
                ));
            }
        }
    }

    // Return empty success response
    let mut output = vec![];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn view_plan() -> Arc<dyn ExecutionPlan> {
        let nested = DataType::Struct(Fields::from(vec![Field::new(
            "nested",
            DataType::Utf8View,
            true,
        )]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8View, true),
            Field::new("bytes", DataType::BinaryView, true),
            Field::new("nested", nested, true),
            Field::new("plain", DataType::Utf8, true),
        ]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn output_encoding_materializes_only_top_level_views() -> SparkResult<()> {
        for (use_large_var_types, expected_text, expected_bytes) in [
            (false, DataType::Utf8, DataType::Binary),
            (true, DataType::LargeUtf8, DataType::LargeBinary),
        ] {
            let input = view_plan();
            let output = apply_spark_connect_output_encoding(
                Arc::clone(&input),
                SparkConnectOutputEncodingPolicy {
                    expand_views: true,
                    use_large_var_types,
                },
            )?;

            let output_schema = output.schema();
            assert!(output.downcast_ref::<ProjectionExec>().is_some());
            assert_eq!(output_schema.field(0).data_type(), &expected_text);
            assert_eq!(output_schema.field(1).data_type(), &expected_bytes);
            assert_eq!(
                output_schema.field(2).data_type(),
                input.schema().field(2).data_type()
            );
            assert_eq!(output_schema.field(3).data_type(), &DataType::Utf8);
            let DataType::Struct(nested) = output_schema.field(2).data_type() else {
                return Err(SparkError::internal("nested field must remain a struct"));
            };
            assert_eq!(nested[0].data_type(), &DataType::Utf8View);
        }
        Ok(())
    }

    #[test]
    fn disabled_output_encoding_preserves_the_plan() -> SparkResult<()> {
        let input = view_plan();
        let output = apply_spark_connect_output_encoding(
            Arc::clone(&input),
            SparkConnectOutputEncodingPolicy {
                expand_views: false,
                use_large_var_types: false,
            },
        )?;
        assert!(Arc::ptr_eq(&input, &output));
        Ok(())
    }
}
