use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::common::file_options::StatementOptions;
use datafusion::common::{FileType, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::dml::CopyOptions;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;
use tracing::debug;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{
    execute_plan, Executor, ExecutorBatch, ExecutorMetadata, ExecutorOutput, ExecutorTaskContext,
};
use crate::plan::from_spark_relation;
use crate::session::Session;
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::{ResponseType, ResultComplete};
use crate::spark::connect::relation;
use crate::spark::connect::write_operation::{SaveMode, SaveType};
use crate::spark::connect::{
    CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction,
    CreateDataFrameViewCommand, ExecutePlanResponse, GetResourcesCommand, Relation, SqlCommand,
    StreamingQueryCommand, StreamingQueryManagerCommand, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart,
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
                response.session_id = self.session_id.clone();
                response.operation_id = self.operation_id.clone();
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::ArrowBatch(batch) => {
                        response.response_type = Some(ResponseType::ArrowBatch(batch));
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

async fn handle_execute_plan(
    session: Arc<Session>,
    plan: LogicalPlan,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let ctx = session.context();
    let operation_id = metadata.operation_id.clone();
    let stream = execute_plan(&ctx, &plan).await?;
    let mut executor = Executor::new(metadata, ExecutorTaskContext::new(stream));
    let rx = executor.start().await?;
    session.lock()?.add_executor(executor);
    let session_id = session.session_id().to_string();
    Ok(ExecutePlanResponseStream::new(session_id, operation_id, rx))
}

pub(crate) async fn handle_execute_relation(
    session: Arc<Session>,
    relation: Relation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let ctx = session.context();
    let plan = from_spark_relation(&ctx, &relation).await?;
    handle_execute_plan(session, plan, metadata).await
}

pub(crate) async fn handle_execute_register_function(
    _session: Arc<Session>,
    _udf: CommonInlineUserDefinedFunction,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_write_operation(
    session: Arc<Session>,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let relation = write.input.required("input")?;
    let ctx = session.context();
    let _ = SaveMode::try_from(write.mode).required("save mode")?;
    if !write.sort_column_names.is_empty() {
        return Err(SparkError::unsupported("sort column names"));
    }
    if !write.partitioning_columns.is_empty() {
        return Err(SparkError::unsupported("partitioning columns"));
    }
    if let Some(_) = write.bucket_by {
        return Err(SparkError::unsupported("bucketing"));
    }
    // TODO: option compatibility
    let options = CopyOptions::SQLOptions(StatementOptions::from(&write.options));
    let plan = from_spark_relation(&ctx, &relation).await?;
    let plan = match write.save_type.required("save type")? {
        SaveType::Path(path) => {
            let source = write.source.required("source")?;
            let format = match source.as_str() {
                "json" => FileType::JSON,
                "parquet" => FileType::PARQUET,
                "csv" => FileType::CSV,
                "arrow" => FileType::ARROW,
                _ => {
                    return Err(SparkError::invalid(format!(
                        "unsupported source: {}",
                        source
                    )))
                }
            };
            LogicalPlanBuilder::copy_to(plan, path, format, false, options)
                .or_else(|e| Err(SparkError::from(e)))?
                .build()
                .or_else(|e| Err(SparkError::from(e)))?
        }
        SaveType::Table(_) => {
            todo!()
        }
    };
    handle_execute_plan(session, plan, metadata).await
}

pub(crate) async fn handle_execute_create_dataframe_view(
    session: Arc<Session>,
    view: CreateDataFrameViewCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let ctx = session.context();
    let relation = view.input.required("input relation")?;
    let plan = from_spark_relation(&ctx, &relation).await?;
    let df = DataFrame::new(ctx.state(), plan);
    let table_ref = TableReference::from(view.name.as_str());
    let _ = view.is_global;
    if view.replace {
        ctx.deregister_table(table_ref.clone())?;
    }
    ctx.register_table(table_ref, df.into_view())?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(ExecutorOutput::new(ExecutorBatch::Complete))
        .await?;
    Ok(ExecutePlanResponseStream::new(
        session.session_id().to_string(),
        metadata.operation_id,
        ReceiverStream::new(rx),
    ))
}

pub(crate) async fn handle_execute_write_operation_v2(
    _session: Arc<Session>,
    _write: WriteOperationV2,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_sql_command(
    session: Arc<Session>,
    sql: SqlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let relation = Relation {
        common: None,
        rel_type: Some(relation::RelType::Sql(sc::Sql {
            query: sql.sql,
            args: sql.args,
            pos_args: sql.pos_args,
        })),
    };
    handle_execute_relation(session, relation, metadata).await
}

pub(crate) async fn handle_execute_write_stream_operation_start(
    _session: Arc<Session>,
    _start: WriteStreamOperationStart,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_streaming_query_command(
    _session: Arc<Session>,
    _stream: StreamingQueryCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_get_resources_command(
    _session: Arc<Session>,
    _resource: GetResourcesCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_streaming_query_manager_command(
    _session: Arc<Session>,
    _manager: StreamingQueryManagerCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_execute_register_table_function(
    _session: Arc<Session>,
    _udtf: CommonInlineUserDefinedTableFunction,
) -> SparkResult<ExecutePlanResponseStream> {
    todo!()
}

pub(crate) async fn handle_interrupt_all(session: Arc<Session>) -> SparkResult<Vec<String>> {
    let mut state = session.lock()?;
    let mut out = vec![];
    for executor in state.remove_all_executors().iter() {
        out.push(executor.metadata.operation_id.clone());
    }
    Ok(out)
}

pub(crate) async fn handle_interrupt_tag(
    session: Arc<Session>,
    tag: String,
) -> SparkResult<Vec<String>> {
    let mut state = session.lock()?;
    let mut out = vec![];
    for executor in state.remove_executors_by_tag(tag.as_str()).iter() {
        out.push(executor.metadata.operation_id.clone());
    }
    Ok(out)
}

pub(crate) async fn handle_interrupt_operation_id(
    session: Arc<Session>,
    operation_id: String,
) -> SparkResult<Vec<String>> {
    let executor = session.lock()?.remove_executor(operation_id.as_str());
    if let Some(_) = executor {
        Ok(vec![operation_id])
    } else {
        Ok(vec![])
    }
}

pub(crate) async fn handle_reattach_execute(
    session: Arc<Session>,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<ExecutePlanResponseStream> {
    let mut executor = session
        .lock()?
        .remove_executor(operation_id.as_str())
        .ok_or_else(|| SparkError::invalid(format!("operation not found: {}", operation_id)))?;
    if !executor.metadata.reattachable {
        return Err(SparkError::invalid(format!(
            "operation not reattachable: {}",
            operation_id
        )));
    }
    executor.release(response_id).await?;
    let rx = executor.start().await?;
    session.lock()?.add_executor(executor);
    let session_id = session.session_id().to_string();
    Ok(ExecutePlanResponseStream::new(session_id, operation_id, rx))
}

pub(crate) async fn handle_release_execute(
    session: Arc<Session>,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<()> {
    let executor = session.lock()?.remove_executor(operation_id.as_str());
    if let Some(mut executor) = executor {
        executor.release(response_id).await?;
        session.lock()?.add_executor(executor);
    }
    Ok(())
}
