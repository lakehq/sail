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
use crate::spark::connect::StorageLevel;
use crate::SPARK_VERSION;

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
    _request: IsLocalRequest,
) -> SparkResult<IsLocalResponse> {
    Err(SparkError::todo("handle analyze is local"))
}

pub(crate) async fn handle_analyze_is_streaming(
    _ctx: &SessionContext,
    _request: IsStreamingRequest,
) -> SparkResult<IsStreamingResponse> {
    // TODO: support streaming
    Ok(IsStreamingResponse {
        is_streaming: false,
    })
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
    Ok(SparkVersionResponse {
        version: SPARK_VERSION.to_string(),
    })
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
