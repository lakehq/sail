use datafusion::prelude::SessionContext;
use log::warn;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_plan::explain::{explain_string, ExplainOptions};
use sail_plan::resolver::plan::NamedPlan;
use sail_plan::resolver::PlanResolver;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::proto::data_type::parse_spark_data_type;
use crate::proto::data_type_json::parse_spark_json_data_type;
use crate::schema::{to_ddl_string, to_spark_schema, to_tree_string};
use crate::session::SparkSession;
use crate::spark::connect as sc;
use crate::spark::connect::analyze_plan_request::explain::ExplainMode;
use crate::spark::connect::analyze_plan_request::{
    DdlParse as DdlParseRequest, Explain as ExplainRequest,
    GetStorageLevel as GetStorageLevelRequest, InputFiles as InputFilesRequest,
    IsLocal as IsLocalRequest, IsStreaming as IsStreamingRequest, JsonToDdl as JsonToDdlRequest,
    Persist as PersistRequest, SameSemantics as SameSemanticsRequest, Schema as SchemaRequest,
    SemanticHash as SemanticHashRequest, SparkVersion as SparkVersionRequest,
    TreeString as TreeStringRequest, Unpersist as UnpersistRequest,
};
use crate::spark::connect::analyze_plan_response::{
    DdlParse as DdlParseResponse, Explain as ExplainResponse,
    GetStorageLevel as GetStorageLevelResponse, InputFiles as InputFilesResponse,
    IsLocal as IsLocalResponse, IsStreaming as IsStreamingResponse, JsonToDdl as JsonToDdlResponse,
    Persist as PersistResponse, SameSemantics as SameSemanticsResponse, Schema as SchemaResponse,
    SemanticHash as SemanticHashResponse, SparkVersion as SparkVersionResponse,
    TreeString as TreeStringResponse, Unpersist as UnpersistResponse,
};
use crate::spark::connect::{plan, StorageLevel};

async fn analyze_schema(ctx: &SessionContext, plan: sc::Plan) -> SparkResult<sc::DataType> {
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan { plan, fields } = resolver
        .resolve_named_plan(spec::Plan::Query(plan.try_into()?))
        .await?;
    let schema = if let Some(fields) = fields {
        rename_schema(plan.schema().inner(), fields.as_slice())?
    } else {
        plan.schema().inner().clone()
    };
    to_spark_schema(schema)
}

pub(crate) async fn handle_analyze_schema(
    ctx: &SessionContext,
    request: SchemaRequest,
) -> SparkResult<SchemaResponse> {
    let SchemaRequest { plan } = request;
    let plan = plan.required("plan")?;
    let schema = analyze_schema(ctx, plan).await?;
    Ok(SchemaResponse {
        schema: Some(schema),
    })
}

pub(crate) async fn handle_analyze_explain(
    ctx: &SessionContext,
    request: ExplainRequest,
) -> SparkResult<ExplainResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let ExplainRequest { plan, explain_mode } = request;
    let plan = plan.required("plan")?;
    let explain_mode = ExplainMode::try_from(explain_mode)?;
    let spec_mode = explain_mode.try_into()?;
    let options = ExplainOptions::from_mode(spec_mode);
    let explain = explain_string(
        ctx,
        spark.plan_config()?,
        spec::Plan::Query(plan.try_into()?),
        options,
    )
    .await?;
    Ok(ExplainResponse {
        explain_string: explain.output,
    })
}

pub(crate) async fn handle_analyze_tree_string(
    ctx: &SessionContext,
    request: TreeStringRequest,
) -> SparkResult<TreeStringResponse> {
    let TreeStringRequest { plan, level } = request;
    let plan = plan.required("plan")?;
    let schema = analyze_schema(ctx, plan).await?;
    Ok(TreeStringResponse {
        tree_string: to_tree_string(&schema, level),
    })
}

pub(crate) async fn handle_analyze_is_local(
    _ctx: &SessionContext,
    request: IsLocalRequest,
) -> SparkResult<IsLocalResponse> {
    let IsLocalRequest { plan } = request;
    let plan = plan.required("plan")?;
    let is_local = analyze_is_local(plan)?;
    Ok(IsLocalResponse { is_local })
}

pub(crate) async fn handle_analyze_is_streaming(
    _ctx: &SessionContext,
    request: IsStreamingRequest,
) -> SparkResult<IsStreamingResponse> {
    let IsStreamingRequest { plan } = request;
    let plan = plan.required("plan")?;
    let is_streaming = analyze_is_streaming(plan)?;
    Ok(IsStreamingResponse { is_streaming })
}

pub(crate) async fn handle_analyze_input_files(
    _ctx: &SessionContext,
    _request: InputFilesRequest,
) -> SparkResult<InputFilesResponse> {
    Err(SparkError::todo("handle analyze input files"))
}

pub(crate) async fn handle_analyze_spark_version(
    _ctx: &SessionContext,
    _request: SparkVersionRequest,
) -> SparkResult<SparkVersionResponse> {
    let version = get_pyspark_version()?;
    Ok(SparkVersionResponse { version })
}

fn get_pyspark_version() -> SparkResult<String> {
    use pyo3::prelude::PyAnyMethods;
    use pyo3::types::PyModule;
    use pyo3::Python;

    Python::attach(|py| {
        let module = PyModule::import(py, "pyspark")?;
        let version: String = module.getattr("__version__")?.extract()?;
        Ok(version)
    })
    .map_err(|e: pyo3::PyErr| SparkError::invalid(format!("failed to get PySpark version: {e}")))
}

pub(crate) async fn handle_analyze_ddl_parse(
    ctx: &SessionContext,
    request: DdlParseRequest,
) -> SparkResult<DdlParseResponse> {
    let data_type = parse_spark_data_type(request.ddl_string.as_str())?;
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let data_type = resolver.resolve_data_type_for_plan(&data_type)?;
    Ok(DdlParseResponse {
        parsed: Some(data_type.try_into()?),
    })
}

pub(crate) async fn handle_analyze_same_semantics(
    _ctx: &SessionContext,
    _request: SameSemanticsRequest,
) -> SparkResult<SameSemanticsResponse> {
    Err(SparkError::todo("handle analyze same semantics"))
}

pub(crate) async fn handle_analyze_semantic_hash(
    _ctx: &SessionContext,
    _request: SemanticHashRequest,
) -> SparkResult<SemanticHashResponse> {
    Err(SparkError::todo("handle analyze semantic hash"))
}

pub(crate) async fn handle_analyze_persist(
    _ctx: &SessionContext,
    _request: PersistRequest,
) -> SparkResult<PersistResponse> {
    // TODO: Implement
    warn!("Persist operation is not yet supported and is a no-op");
    Ok(PersistResponse {})
}

pub(crate) async fn handle_analyze_unpersist(
    _ctx: &SessionContext,
    _request: UnpersistRequest,
) -> SparkResult<UnpersistResponse> {
    // TODO: Implement
    warn!("Unpersist operation is not yet supported and is a no-op");
    Ok(UnpersistResponse {})
}

pub(crate) async fn handle_analyze_get_storage_level(
    _ctx: &SessionContext,
    _request: GetStorageLevelRequest,
) -> SparkResult<GetStorageLevelResponse> {
    Ok(GetStorageLevelResponse {
        storage_level: Some(StorageLevel {
            use_disk: false,
            use_memory: true,
            use_off_heap: true,
            deserialized: false,
            replication: 1,
        }),
    })
}

pub(crate) async fn handle_analyze_json_to_ddl(
    _ctx: &SessionContext,
    request: JsonToDdlRequest,
) -> SparkResult<JsonToDdlResponse> {
    let data_type = parse_spark_json_data_type(&request.json_string)?;
    let ddl_string = to_ddl_string(&data_type)?;
    Ok(JsonToDdlResponse { ddl_string })
}

fn analyze_is_local(plan: sc::Plan) -> SparkResult<bool> {
    let sc::Plan { op_type: op } = plan;
    match op.required("plan op")? {
        plan::OpType::Command(_) => Ok(true),
        plan::OpType::Root(relation) => {
            let plan: spec::Plan = relation.try_into()?;
            Ok(matches!(
                plan,
                spec::Plan::Command(_)
                    | spec::Plan::Query(spec::QueryPlan {
                        node: spec::QueryNode::LocalRelation { .. }
                            | spec::QueryNode::CachedLocalRelation { .. },
                        ..
                    })
            ))
        }
        plan::OpType::CompressedOperation(_) => {
            Err(SparkError::unsupported("compressed operation"))
        }
    }
}

fn analyze_is_streaming(plan: sc::Plan) -> SparkResult<bool> {
    let sc::Plan { op_type: op } = plan;
    match op.required("plan op")? {
        plan::OpType::Command(_) => Ok(false),
        plan::OpType::Root(relation) => {
            let plan: spec::Plan = relation.try_into()?;
            match plan {
                spec::Plan::Command(_) => Ok(false),
                spec::Plan::Query(query) => Ok(is_streaming_query_plan(&query)),
            }
        }
        plan::OpType::CompressedOperation(_) => {
            Err(SparkError::unsupported("compressed operation"))
        }
    }
}

fn is_streaming_query_plan(plan: &spec::QueryPlan) -> bool {
    is_streaming_query_node(&plan.node)
}

fn is_streaming_query_node(node: &spec::QueryNode) -> bool {
    match node {
        spec::QueryNode::Read { is_streaming, .. } => *is_streaming,
        // leaf nodes with no query plan inputs
        spec::QueryNode::LocalRelation { .. }
        | spec::QueryNode::CachedLocalRelation { .. }
        | spec::QueryNode::CachedRemoteRelation { .. }
        | spec::QueryNode::Range(_)
        | spec::QueryNode::Empty { .. }
        | spec::QueryNode::Values(_)
        | spec::QueryNode::CommonInlineUserDefinedTableFunction(_) => false,
        // single required input
        spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::Sort { input, .. }
        | spec::QueryNode::Limit { input, .. }
        | spec::QueryNode::SubqueryAlias { input, .. }
        | spec::QueryNode::Repartition { input, .. }
        | spec::QueryNode::ToDf { input, .. }
        | spec::QueryNode::WithColumnsRenamed { input, .. }
        | spec::QueryNode::Drop { input, .. }
        | spec::QueryNode::Tail { input, .. }
        | spec::QueryNode::WithColumns { input, .. }
        | spec::QueryNode::Hint { input, .. }
        | spec::QueryNode::ToSchema { input, .. }
        | spec::QueryNode::RepartitionByExpression { input, .. }
        | spec::QueryNode::MapPartitions { input, .. }
        | spec::QueryNode::CollectMetrics { input, .. }
        | spec::QueryNode::FillNa { input, .. }
        | spec::QueryNode::DropNa { input, .. }
        | spec::QueryNode::Replace { input, .. }
        | spec::QueryNode::StatSummary { input, .. }
        | spec::QueryNode::StatDescribe { input, .. }
        | spec::QueryNode::StatCrosstab { input, .. }
        | spec::QueryNode::StatCov { input, .. }
        | spec::QueryNode::StatCorr { input, .. }
        | spec::QueryNode::StatApproxQuantile { input, .. }
        | spec::QueryNode::StatFreqItems { input, .. }
        | spec::QueryNode::StatSampleBy { input, .. }
        | spec::QueryNode::WithParameters { input, .. }
        | spec::QueryNode::TableAlias { input, .. }
        | spec::QueryNode::TableSample { input, .. } => is_streaming_query_plan(input),
        // single optional input - None input means no source, which is not streaming
        spec::QueryNode::Project { input, .. } | spec::QueryNode::LateralView { input, .. } => {
            input.as_ref().is_some_and(|i| is_streaming_query_plan(i))
        }
        // nested struct with single input
        spec::QueryNode::Aggregate(agg) => is_streaming_query_plan(&agg.input),
        spec::QueryNode::Sample(s) => is_streaming_query_plan(&s.input),
        spec::QueryNode::Deduplicate(d) => is_streaming_query_plan(&d.input),
        spec::QueryNode::Pivot(p) => is_streaming_query_plan(&p.input),
        spec::QueryNode::Unpivot(u) => is_streaming_query_plan(&u.input),
        spec::QueryNode::Parse(p) => is_streaming_query_plan(&p.input),
        spec::QueryNode::WithWatermark(w) => is_streaming_query_plan(&w.input),
        spec::QueryNode::ApplyInPandasWithState(a) => is_streaming_query_plan(&a.input),
        // multiple inputs
        spec::QueryNode::Join(j) => {
            is_streaming_query_plan(&j.left) || is_streaming_query_plan(&j.right)
        }
        spec::QueryNode::SetOperation(s) => {
            is_streaming_query_plan(&s.left) || is_streaming_query_plan(&s.right)
        }
        spec::QueryNode::CoGroupMap(c) => {
            is_streaming_query_plan(&c.input) || is_streaming_query_plan(&c.other)
        }
        spec::QueryNode::GroupMap(g) => {
            is_streaming_query_plan(&g.input)
                || g.initial_input
                    .as_ref()
                    .is_some_and(|i| is_streaming_query_plan(i))
        }
        spec::QueryNode::WithCtes { input, ctes, .. } => {
            is_streaming_query_plan(input)
                || ctes.iter().any(|(_name, p)| is_streaming_query_plan(p))
        }
        spec::QueryNode::WithRelations { root, references } => {
            is_streaming_query_plan(root) || references.iter().any(is_streaming_query_plan)
        }
        spec::QueryNode::LateralJoin { left, right, .. } => {
            is_streaming_query_plan(left) || is_streaming_query_plan(right)
        }
    }
}
