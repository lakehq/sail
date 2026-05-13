use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::parquet::arrow::async_writer::ParquetObjectWriter;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use fastrace::collector::SpanContext;
use fastrace::future::FutureExt;
use fastrace::Span;
use futures::{stream, StreamExt, TryStreamExt};
use log::{debug, warn};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStoreScheme;
use sail_catalog::manager::tracker::CatalogCachedRelation;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::resolve_and_execute_plan;
use sail_plan::resolver::plan::NamedPlan;
use sail_plan::resolver::PlanResolver;
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
    relation, CachedRemoteRelation, CheckpointCommand, CheckpointCommandResult,
    CommonInlineUserDefinedDataSource, CommonInlineUserDefinedFunction,
    CommonInlineUserDefinedTableFunction, CreateDataFrameViewCommand, ExecutePlanResponse,
    GetResourcesCommand, LocalRelation, MergeIntoTableCommand, Relation,
    RemoveCachedRemoteRelationCommand, SqlCommand, StreamingQueryCommand,
    StreamingQueryCommandResult, StreamingQueryListenerBusCommand, StreamingQueryManagerCommand,
    StreamingQueryManagerCommandResult, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart, WriteStreamOperationStartResult,
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
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let (plan, _) = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = {
        let span = Span::enter_with_parent("JobRunner::execute", &span);
        service.runner().execute(ctx, plan).in_span(span).await?
    };
    let rx = match mode {
        ExecutePlanMode::Lazy => {
            let _guard = span.set_local_parent();
            let executor = Executor::new(
                metadata,
                stream,
                spark.options().execution_heartbeat_interval,
            );
            let rx = executor.start()?;
            spark.add_executor(executor)?;
            rx
        }
        ExecutePlanMode::EagerSilent => {
            let _ = read_stream(stream).in_span(span).await?;
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

pub(crate) async fn handle_execute_merge_into_table_command(
    ctx: &SessionContext,
    command: MergeIntoTableCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(command.try_into()?));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}

/// Handles execution of a SQL command.
/// If a string is sent over we convert it to a relation then convert it to a plan, then execute it.
pub(crate) async fn handle_execute_sql_command(
    ctx: &SessionContext,
    sql: SqlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
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
            let (plan, _) = resolve_and_execute_plan(ctx, spark.plan_config()?, command).await?;
            let stream = service.runner().execute(ctx, plan).await?;
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

pub(crate) async fn handle_execute_checkpoint_command(
    ctx: &SessionContext,
    checkpoint: CheckpointCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let CheckpointCommand {
        relation,
        local,
        // The `eager` flag is a Spark hint requesting that materialization be
        // deferred until the cached relation is first used.  Sail does not
        // implement deferred materialization (which would require intercepting
        // `CachedRemoteRelation` reads and updating the cached entry on first
        // use), so we always materialize eagerly.  This is a correctness-
        // preserving choice: lineage is truncated and the checkpoint exists
        // immediately, matching Spark's observable semantics for `eager=true`.
        eager: _,
        storage_level,
    } = checkpoint;
    let relation = relation.required("checkpoint relation")?;
    let plan = spec::Plan::Query((relation).try_into()?);

    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan {
        plan: logical_plan,
        fields,
    } = resolver.resolve_named_plan(plan).await?;
    let fields = named_plan_fields(&logical_plan, fields);

    let use_disk = checkpoint_uses_disk(local, storage_level.as_ref());
    let (cached_plan, storage_uri) = if use_disk {
        materialize_logical_plan_to_disk(ctx, logical_plan).await?
    } else {
        (
            materialize_logical_plan_to_memory(ctx, logical_plan).await?,
            None,
        )
    };

    let relation_id = uuid::Uuid::new_v4().to_string();
    let manager = ctx.extension::<CatalogManager>()?;
    if let Err(error) = manager.track_cached_relation(
        relation_id.clone(),
        CatalogCachedRelation {
            plan: Arc::new(cached_plan),
            fields,
            storage_uri: storage_uri.clone(),
        },
    ) {
        if let Some(uri) = storage_uri {
            cleanup_checkpoint_path(ctx, &uri).await;
        }
        return Err(SparkError::internal(format!(
            "failed to track cached relation: {error}"
        )));
    }

    let result = CheckpointCommandResult {
        relation: Some(CachedRemoteRelation { relation_id }),
    };
    let mut output = vec![ExecutorOutput::new(ExecutorBatch::CheckpointCommandResult(
        Box::new(result),
    ))];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
    ))
}

fn named_plan_fields(plan: &LogicalPlan, fields: Option<Vec<String>>) -> Vec<String> {
    fields.unwrap_or_else(|| {
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    })
}

/// Determines whether a checkpoint should be materialized to disk.
///
/// Non-local (reliable) checkpoints always use disk.  For local checkpoints
/// the decision follows the explicit `StorageLevel` when provided.  When the
/// client omits the storage level (which is the common case – PySpark's
/// `localCheckpoint()` defaults to `MEMORY_AND_DISK` but does **not** send the
/// level over the wire), we default to **disk** to avoid collecting the entire
/// DataFrame into server memory, which could easily OOM for large datasets.
fn checkpoint_uses_disk(
    local: bool,
    storage_level: Option<&crate::spark::connect::StorageLevel>,
) -> bool {
    if !local {
        return true;
    }
    storage_level.map(|level| level.use_disk).unwrap_or(true)
}

/// Eagerly executes a logical plan via the [`JobService`] runner used by
/// regular query execution.  This ensures checkpoint materialization runs on
/// the same execution path as user queries (including in cluster mode) and
/// participates in job tracking and cancellation.
async fn execute_plan_via_runner_stream(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> SparkResult<(SchemaRef, SendableRecordBatchStream)> {
    let arrow_schema = plan.schema().inner().clone();
    let df = ctx.execute_logical_plan(plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let physical_plan = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await?;
    let service = ctx.extension::<JobService>()?;
    let stream = service.runner().execute(ctx, physical_plan).await?;
    Ok((arrow_schema, stream))
}

async fn collect_plan_via_runner(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> SparkResult<(SchemaRef, Vec<datafusion::arrow::array::RecordBatch>)> {
    let (arrow_schema, stream) = execute_plan_via_runner_stream(ctx, plan).await?;
    let batches = read_stream(stream).await?;
    Ok((arrow_schema, batches))
}

/// Eagerly executes the resolved logical plan and returns a new logical plan
/// that scans the materialized data from an in-memory table.
async fn materialize_logical_plan_to_memory(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> SparkResult<LogicalPlan> {
    let (arrow_schema, batches) = collect_plan_via_runner(ctx, plan).await?;
    let table = Arc::new(MemTable::try_new(arrow_schema, vec![batches])?);
    let scan = LogicalPlanBuilder::scan(
        datafusion::common::TableReference::bare(UNNAMED_TABLE),
        provider_as_source(table),
        None,
    )?
    .build()?;
    Ok(scan)
}

fn directory_file_url(path: PathBuf) -> SparkResult<url::Url> {
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    url::Url::from_directory_path(&path).map_err(|_| {
        SparkError::invalid(format!(
            "checkpoint directory is not a valid file path: {}",
            path.display()
        ))
    })
}

fn ensure_directory_url(mut url: url::Url) -> SparkResult<url::Url> {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn is_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn is_uri_like(path: &str) -> bool {
    path.contains("://")
}

/// Returns the configured checkpoint root location.  When the configuration
/// option is empty, falls back to a `sail-checkpoints/` subdirectory of the
/// system temporary directory.
///
/// In cluster deployments the operator should configure `spark.checkpoint_dir`
/// to a location that is shared and readable by all workers, such as an
/// object-store URI or a shared filesystem path.  The default node-local path
/// is suitable for single-node deployments and tests but will not work
/// correctly for distributed execution since remote workers cannot read from
/// the driver's local temporary directory.
fn resolve_checkpoint_root(spark: &SparkSession) -> SparkResult<url::Url> {
    let configured = spark.options().checkpoint_dir.trim();
    if configured.is_empty() {
        directory_file_url(std::env::temp_dir().join("sail-checkpoints"))
    } else if Path::new(configured).is_absolute() || is_windows_drive_path(configured) {
        directory_file_url(PathBuf::from(configured))
    } else if is_uri_like(configured) {
        let url = url::Url::parse(configured)
            .map_err(|e| SparkError::invalid(format!("invalid checkpoint URI: {e}")))?;
        ensure_directory_url(url)
    } else {
        directory_file_url(PathBuf::from(configured))
    }
}

fn checkpoint_store_and_path(
    ctx: &SessionContext,
    checkpoint_url: &url::Url,
) -> SparkResult<(Arc<dyn object_store::ObjectStore>, ObjectStorePath)> {
    let (_, checkpoint_path) = ObjectStoreScheme::parse(checkpoint_url)
        .map_err(|e| SparkError::invalid(format!("invalid checkpoint location: {e}")))?;
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(checkpoint_url)?;
    Ok((object_store, checkpoint_path))
}

/// Eagerly executes the resolved logical plan, streaming the result directly
/// to a checkpoint Parquet file on disk, and returns a new logical plan that
/// scans those files.
///
/// Streaming the physical plan output through an `AsyncArrowWriter` avoids
/// buffering the entire materialized DataFrame in driver memory, which is
/// the whole point of using the on-disk checkpoint path.  Execution still
/// goes through the [`JobService`] runner so that the work participates in
/// distributed execution, job tracking, and cancellation.
async fn materialize_logical_plan_to_disk(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> SparkResult<(LogicalPlan, Option<String>)> {
    // Spark Connect does not send a user checkpoint directory with this command,
    // so Sail uses the directory configured by `spark.checkpoint_dir` (or a
    // server-local temporary area if unset).  Files are best-effort cleaned
    // up when the client releases the cached remote relation or when the
    // session ends.
    let spark = ctx.extension::<SparkSession>()?;
    let checkpoint_root = resolve_checkpoint_root(&spark)?;
    let checkpoint_url = checkpoint_root
        .join(&format!("{}/", uuid::Uuid::new_v4()))
        .map_err(|e| SparkError::invalid(format!("invalid checkpoint location: {e}")))?;
    let checkpoint_uri = checkpoint_url.to_string();
    let (object_store, checkpoint_path) = checkpoint_store_and_path(ctx, &checkpoint_url)?;

    let (arrow_schema, mut stream) = execute_plan_via_runner_stream(ctx, plan).await?;

    // Stream batches directly to a single Parquet file inside the checkpoint
    // directory without collecting them all into memory first.
    let write_result: SparkResult<()> = async {
        let parquet_path = ObjectStorePath::from(format!("{checkpoint_path}/part-0.parquet"));
        let object_writer = ParquetObjectWriter::new(object_store.clone(), parquet_path);
        let mut writer = AsyncArrowWriter::try_new(object_writer, arrow_schema, None)
            .map_err(|e| SparkError::internal(format!("failed to create parquet writer: {e}")))?;
        while let Some(batch) = stream.try_next().await? {
            writer
                .write(&batch)
                .await
                .map_err(|e| SparkError::internal(format!("failed to write parquet batch: {e}")))?;
        }
        writer
            .close()
            .await
            .map_err(|e| SparkError::internal(format!("failed to close parquet writer: {e}")))?;
        Ok(())
    }
    .await;

    if let Err(error) = write_result {
        cleanup_checkpoint_path(ctx, &checkpoint_uri).await;
        return Err(error);
    }

    let df = match ctx
        .read_parquet(&checkpoint_uri, ParquetReadOptions::default())
        .await
    {
        Ok(df) => df,
        Err(error) => {
            cleanup_checkpoint_path(ctx, &checkpoint_uri).await;
            return Err(error.into());
        }
    };
    Ok((df.into_unoptimized_plan(), Some(checkpoint_uri)))
}

async fn cleanup_checkpoint_path(ctx: &SessionContext, checkpoint_uri: &str) {
    let Ok(checkpoint_url) = url::Url::parse(checkpoint_uri) else {
        warn!("failed to parse checkpoint location for cleanup: {checkpoint_uri}");
        return;
    };
    let Ok((object_store, checkpoint_path)) = checkpoint_store_and_path(ctx, &checkpoint_url)
    else {
        warn!("failed to resolve checkpoint object store for cleanup: {checkpoint_uri}");
        return;
    };
    let locations = object_store
        .list(Some(&checkpoint_path))
        .map_ok(|meta| meta.location)
        .boxed();
    if let Err(e) = object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
    {
        warn!("failed to remove checkpoint location {checkpoint_uri}: {e}");
    }
}

pub(crate) async fn handle_execute_remove_cached_remote_relation_command(
    ctx: &SessionContext,
    command: RemoveCachedRemoteRelationCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let RemoveCachedRemoteRelationCommand { relation } = command;
    let relation = relation.required("remove cached remote relation")?;
    let manager = ctx.extension::<CatalogManager>()?;
    // Removing a relation that does not exist is treated as a no-op to match
    // Spark Connect, which tolerates duplicate or out-of-order cleanup calls.
    let removed = manager
        .remove_cached_relation(&relation.relation_id)
        .map_err(|e| SparkError::internal(format!("failed to remove cached relation: {e}")))?;
    if let Some(uri) = removed.and_then(|relation| relation.storage_uri) {
        cleanup_checkpoint_path(ctx, &uri).await;
    }

    let spark = ctx.extension::<SparkSession>()?;
    let mut output = Vec::new();
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output)),
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
            ))
        }
    };

    // Register in the session-scoped TableFormatRegistry with embedded pickled bytes
    {
        use std::sync::Arc;

        use sail_common_datafusion::datasource::TableFormatRegistry;
        use sail_data_source::formats::python::PythonTableFormat;

        // Register format in session's TableFormatRegistry with embedded pickled class
        // This provides session isolation - the format is only visible to this session
        if let Ok(registry) = ctx.extension::<TableFormatRegistry>() {
            let format = Arc::new(PythonTableFormat::with_pickled_class(name.clone(), command));
            // Ignore error if already registered (allows re-registration to update)
            if let Err(e) = registry.register(format) {
                warn!("Failed to register python datasource {}: {}", name, e);
            }
            log::info!("Registered session-scoped datasource: {}", name);
        } else {
            return Err(SparkError::internal(
                "TableFormatRegistry not found in session context",
            ));
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
        Box::pin(stream::iter(output)),
    ))
}
