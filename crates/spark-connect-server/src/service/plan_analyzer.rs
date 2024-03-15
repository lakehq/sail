use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use tonic::Status;

use crate::error::ProtoFieldExt;
use crate::plan::from_spark_relation;
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

pub(crate) async fn handle_analyze_schema(
    session: Arc<Session>,
    request: SchemaRequest,
) -> Result<SchemaResponse, Status> {
    let ctx = session.context();
    let sc::Plan { op_type: op } = request.plan.required("plan")?;
    let relation = match op.required("plan op")? {
        plan::OpType::Root(relation) => relation,
        plan::OpType::Command(_) => return Err(Status::invalid_argument("relation expected")),
    };
    let plan = from_spark_relation(&ctx, &relation).await?;
    let schema: SchemaRef = Arc::new(plan.schema().as_ref().into());
    Ok(SchemaResponse {
        schema: Some(to_spark_schema(schema)?),
    })
}

pub(crate) async fn handle_analyze_explain(
    session: Arc<Session>,
    request: ExplainRequest,
) -> Result<ExplainResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_tree_string(
    session: Arc<Session>,
    request: TreeStringRequest,
) -> Result<TreeStringResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_is_local(
    session: Arc<Session>,
    request: IsLocalRequest,
) -> Result<IsLocalResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_is_streaming(
    session: Arc<Session>,
    request: IsStreamingRequest,
) -> Result<IsStreamingResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_input_files(
    session: Arc<Session>,
    request: InputFilesRequest,
) -> Result<InputFilesResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_spark_version(
    session: Arc<Session>,
    request: SparkVersionRequest,
) -> Result<SparkVersionResponse, Status> {
    Ok(SparkVersionResponse {
        version: SPARK_VERSION.to_string(),
    })
}

pub(crate) async fn handle_analyze_ddl_parse(
    session: Arc<Session>,
    request: DdlParseRequest,
) -> Result<DdlParseResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_same_semantics(
    session: Arc<Session>,
    request: SameSemanticsRequest,
) -> Result<SameSemanticsResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_semantic_hash(
    session: Arc<Session>,
    request: SemanticHashRequest,
) -> Result<SemanticHashResponse, Status> {
    todo!()
}

pub(crate) async fn handle_analyze_persist(
    session: Arc<Session>,
    request: PersistRequest,
) -> Result<PersistResponse, Status> {
    Ok(PersistResponse {})
}

pub(crate) async fn handle_analyze_unpersist(
    session: Arc<Session>,
    request: UnpersistRequest,
) -> Result<UnpersistResponse, Status> {
    Ok(UnpersistResponse {})
}

pub(crate) async fn handle_analyze_get_storage_level(
    session: Arc<Session>,
    request: GetStorageLevelRequest,
) -> Result<GetStorageLevelResponse, Status> {
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
