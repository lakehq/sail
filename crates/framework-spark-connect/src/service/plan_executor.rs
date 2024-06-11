use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::common::{FileType, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_common::{
    config::{FormatOptions, TableOptions},
    DFSchema, DataFusionError,
};
use datafusion_expr::{expr, ExprSchemable, ScalarUDF};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;
use tracing::debug;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{
    execute_plan, Executor, ExecutorBatch, ExecutorMetadata, ExecutorOutput, ExecutorTaskContext,
};
use crate::session::Session;
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::{ResponseType, ResultComplete};
use crate::spark::connect::relation;
use crate::spark::connect::write_operation::{save_table::TableSaveMethod, SaveMode, SaveType};
use crate::spark::connect::{
    CommonInlineUserDefinedFunction as SCCommonInlineUserDefinedFunction,
    CommonInlineUserDefinedTableFunction as SCCommonInlineUserDefinedTableFunction,
    CreateDataFrameViewCommand, ExecutePlanResponse, GetResourcesCommand, Relation, SqlCommand,
    StreamingQueryCommand, StreamingQueryManagerCommand, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart,
};
use framework_common::spec::{
    CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction, FunctionDefinition,
};
use framework_plan::error::PlanResult;
use framework_plan::resolver::PlanResolver;
use framework_python::cereal::partial_pyspark_udf::{
    deserialize_partial_pyspark_udf, PartialPySparkUDF,
};
use framework_python::udf::pyspark_udf::PySparkUDF;

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
    let stream = execute_plan(ctx, plan).await?;
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
    let resolver = PlanResolver::new(ctx);
    let plan = resolver.resolve_plan(relation.try_into()?).await?;
    handle_execute_plan(session, plan, metadata).await
}

pub(crate) async fn handle_execute_register_function(
    session: Arc<Session>,
    udf: SCCommonInlineUserDefinedFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    // TODO: Should probably just call PlanNode::RegisterFunction
    //  even though SC implementation creates the UDF and registers it directly.
    let ctx = session.context();
    let resolver = PlanResolver::new(ctx);

    let udf: CommonInlineUserDefinedFunction = udf.try_into()?;
    let CommonInlineUserDefinedFunction {
        function_name,
        deterministic,
        arguments,
        function,
    } = udf;
    let function_name: &str = function_name.as_str();

    // TODO: args always empty so dont need schema and input types.
    //  Register UnresolvedPysparkUDF after we create it.
    let schema = DFSchema::empty();
    let arguments: Vec<expr::Expr> = arguments
        .into_iter()
        .map(|x| resolver.resolve_expression(x, &schema))
        .collect::<PlanResult<Vec<expr::Expr>>>()?;
    let input_types: Vec<ArrowDataType> =
        arguments
            .iter()
            .map(|arg| arg.get_type(&schema))
            .collect::<datafusion_common::Result<Vec<ArrowDataType>, DataFusionError>>()?;

    let (output_type, eval_type, command, python_version) = match function {
        FunctionDefinition::PythonUdf {
            output_type,
            eval_type,
            command,
            python_version,
        } => (output_type, eval_type, command, python_version),
        _ => {
            return Err(SparkError::invalid("UDF function type must be Python UDF"));
        }
    };
    let output_type: ArrowDataType = output_type.try_into()?;

    let python_function: PartialPySparkUDF = deserialize_partial_pyspark_udf(
        &python_version,
        &command,
        &eval_type,
        &(arguments.len() as i32),
    )
    .map_err(|e| SparkError::invalid(format!("Python UDF deserialization error: {:?}", e)))?;

    let python_udf: PySparkUDF = PySparkUDF::new(
        function_name.to_owned(),
        deterministic,
        input_types,
        eval_type,
        python_function,
        output_type,
    );

    let scalar_udf = ScalarUDF::from(python_udf);
    //  TODO: Register UnresolvedPySparkUDF after we create iot
    ctx.register_udf(scalar_udf);

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(ExecutorOutput::new(ExecutorBatch::Complete))
        .await?;
    Ok(ExecutePlanResponseStream::new(
        session.session_id().to_string(),
        metadata.operation_id,
        ReceiverStream::new(rx),
    ))
}

pub(crate) async fn handle_execute_write_operation(
    session: Arc<Session>,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let relation = write.input.required("input")?;
    let ctx = session.context();
    let save_mode = SaveMode::try_from(write.mode).required("save mode")?;
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
    let mut table_options = TableOptions::default_from_session_config(ctx.state().config_options());
    table_options.alter_with_string_hash_map(&write.options)?;
    let resolver = PlanResolver::new(ctx);
    let plan = resolver.resolve_plan(relation.try_into()?).await?;
    let plan = match write.save_type.required("save type")? {
        SaveType::Path(path) => {
            // always write multi-file output
            let path = if path.ends_with("/") {
                path
            } else {
                format!("{}/", path)
            };
            let source = write.source.required("source")?;
            let format_options = match source.as_str() {
                "json" => {
                    table_options.set_file_format(FileType::JSON);
                    FormatOptions::JSON(table_options.json)
                }
                "parquet" => {
                    table_options.set_file_format(FileType::PARQUET);
                    FormatOptions::PARQUET(table_options.parquet)
                }
                "csv" => {
                    table_options.set_file_format(FileType::CSV);
                    FormatOptions::CSV(table_options.csv)
                }
                "arrow" => {
                    table_options.set_file_format(FileType::ARROW);
                    FormatOptions::ARROW
                }
                _ => {
                    return Err(SparkError::invalid(format!(
                        "unsupported source: {}",
                        source
                    )))
                }
            };
            LogicalPlanBuilder::copy_to(
                plan,
                path,
                format_options,
                write.options,
                write.partitioning_columns,
            )
            .or_else(|e| Err(SparkError::from(e)))?
            .build()
            .or_else(|e| Err(SparkError::from(e)))?
        }
        SaveType::Table(save) => {
            let table_name = save.table_name.as_str();
            let table_ref = TableReference::from(table_name.to_string());
            let save_method =
                TableSaveMethod::try_from(save.save_method).required("save method")?;
            let df = DataFrame::new(ctx.state(), plan.clone());

            match save_method {
                TableSaveMethod::SaveAsTable => {
                    ctx.register_table(table_ref, df.into_view())?;
                    plan
                }
                TableSaveMethod::InsertInto => {
                    let arrow_schema = ArrowSchema::from(df.schema());
                    let overwrite = match save_mode {
                        SaveMode::Overwrite => true,
                        _ => false,
                    };
                    LogicalPlanBuilder::insert_into(plan, table_ref, &arrow_schema, overwrite)
                        .or_else(|e| Err(SparkError::from(e)))?
                        .build()
                        .or_else(|e| Err(SparkError::from(e)))?
                }
                _ => {
                    return Err(SparkError::invalid(format!(
                        "WriteOperation:SaveTable:TableSaveMethod not supported: {}",
                        save_method.as_str_name()
                    )))
                }
            }
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
    let resolver = PlanResolver::new(ctx);
    let plan = resolver.resolve_plan(relation.try_into()?).await?;
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
    Err(SparkError::todo("write operation v2"))
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
    Err(SparkError::todo("write stream operation start"))
}

pub(crate) async fn handle_execute_streaming_query_command(
    _session: Arc<Session>,
    _stream: StreamingQueryCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("streaming query command"))
}

pub(crate) async fn handle_execute_get_resources_command(
    _session: Arc<Session>,
    _resource: GetResourcesCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("get resources command"))
}

pub(crate) async fn handle_execute_streaming_query_manager_command(
    _session: Arc<Session>,
    _manager: StreamingQueryManagerCommand,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("streaming query manager command"))
}

pub(crate) async fn handle_execute_register_table_function(
    _session: Arc<Session>,
    _udtf: SCCommonInlineUserDefinedTableFunction,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("register table function"))
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
