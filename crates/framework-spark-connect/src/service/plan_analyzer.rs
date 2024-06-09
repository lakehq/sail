use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::proto::data_type::parse_spark_schema;
use crate::schema::to_spark_schema;
use crate::session::Session;
use crate::spark::connect as sc;
use crate::spark::connect::analyze_plan_request::{
    DdlParse as DdlParseRequest, Explain as ExplainRequest,
    GetStorageLevel as GetStorageLevelRequest, InputFiles as InputFilesRequest,
    IsLocal as IsLocalRequest, IsStreaming as IsStreamingRequest, Persist as PersistRequest,
    SameSemantics as SameSemanticsRequest, Schema as SchemaRequest,
    SemanticHash as SemanticHashRequest, SparkVersion as SparkVersionRequest,
    TreeString as TreeStringRequest, Unpersist as UnpersistRequest,
};
use crate::spark::connect::analyze_plan_response::{
    DdlParse as DdlParseResponse, Explain as ExplainResponse,
    GetStorageLevel as GetStorageLevelResponse, InputFiles as InputFilesResponse,
    IsLocal as IsLocalResponse, IsStreaming as IsStreamingResponse, Persist as PersistResponse,
    SameSemantics as SameSemanticsResponse, Schema as SchemaResponse,
    SemanticHash as SemanticHashResponse, SparkVersion as SparkVersionResponse,
    TreeString as TreeStringResponse, Unpersist as UnpersistResponse,
};
use crate::spark::connect::plan;
use crate::spark::connect::StorageLevel;
use crate::SPARK_VERSION;
use framework_plan::resolver::PlanResolver;

pub(crate) async fn handle_analyze_schema(
    session: Arc<Session>,
    request: SchemaRequest,
) -> SparkResult<SchemaResponse> {
    let ctx = session.context();
    let sc::Plan { op_type: op } = request.plan.required("plan")?;
    let relation = match op.required("plan op")? {
        plan::OpType::Root(relation) => relation,
        plan::OpType::Command(_) => return Err(SparkError::invalid("relation expected")),
    };
    let resolver = PlanResolver::new(ctx);
    let plan = resolver.resolve_plan(relation.try_into()?).await?;
    let schema: SchemaRef = Arc::new(plan.schema().as_ref().into());
    Ok(SchemaResponse {
        schema: Some(to_spark_schema(schema)?),
    })
}

pub(crate) async fn handle_analyze_explain(
    _session: Arc<Session>,
    _request: ExplainRequest,
) -> SparkResult<ExplainResponse> {
    Err(SparkError::todo("handle analyze explain"))
}

pub(crate) async fn handle_analyze_tree_string(
    _session: Arc<Session>,
    _request: TreeStringRequest,
) -> SparkResult<TreeStringResponse> {
    Err(SparkError::todo("handle analyze tree string"))
}

pub(crate) async fn handle_analyze_is_local(
    _session: Arc<Session>,
    _request: IsLocalRequest,
) -> SparkResult<IsLocalResponse> {
    Err(SparkError::todo("handle analyze is local"))
}

pub(crate) async fn handle_analyze_is_streaming(
    _session: Arc<Session>,
    _request: IsStreamingRequest,
) -> SparkResult<IsStreamingResponse> {
    // TODO: support streaming
    Ok(IsStreamingResponse {
        is_streaming: false,
    })
}

pub(crate) async fn handle_analyze_input_files(
    _session: Arc<Session>,
    _request: InputFilesRequest,
) -> SparkResult<InputFilesResponse> {
    Err(SparkError::todo("handle analyze input files"))
}

pub(crate) async fn handle_analyze_spark_version(
    _session: Arc<Session>,
    _request: SparkVersionRequest,
) -> SparkResult<SparkVersionResponse> {
    Ok(SparkVersionResponse {
        version: SPARK_VERSION.to_string(),
    })
}

pub(crate) async fn handle_analyze_ddl_parse(
    _session: Arc<Session>,
    request: DdlParseRequest,
) -> SparkResult<DdlParseResponse> {
    let schema = parse_spark_schema(request.ddl_string.as_str())?;
    Ok(DdlParseResponse {
        parsed: Some(schema.try_into()?),
    })
}

pub(crate) async fn handle_analyze_same_semantics(
    _session: Arc<Session>,
    _request: SameSemanticsRequest,
) -> SparkResult<SameSemanticsResponse> {
    Err(SparkError::todo("handle analyze same semantics"))
}

pub(crate) async fn handle_analyze_semantic_hash(
    _session: Arc<Session>,
    _request: SemanticHashRequest,
) -> SparkResult<SemanticHashResponse> {
    Err(SparkError::todo("handle analyze semantic hash"))
}

pub(crate) async fn handle_analyze_persist(
    _session: Arc<Session>,
    _request: PersistRequest,
) -> SparkResult<PersistResponse> {
    Ok(PersistResponse {})
}

pub(crate) async fn handle_analyze_unpersist(
    _session: Arc<Session>,
    _request: UnpersistRequest,
) -> SparkResult<UnpersistResponse> {
    Ok(UnpersistResponse {})
}

pub(crate) async fn handle_analyze_get_storage_level(
    _session: Arc<Session>,
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
