use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use datafusion::prelude::SessionContext;
use log::warn;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_plan::explain::{explain_string, ExplainOptions};
use sail_plan::resolver::plan::NamedPlan;
use sail_plan::resolver::PlanResolver;

use crate::config::get_pyspark_version;
use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::proto::data_type::parse_spark_data_type;
use crate::proto::data_type_json::parse_spark_json_data_type;
use crate::proto::plan::decompress_operation;
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
    request: InputFilesRequest,
) -> SparkResult<InputFilesResponse> {
    let InputFilesRequest { plan } = request;
    let plan = plan.required("input files plan")?;
    let plan: spec::QueryPlan = plan.try_into()?;
    let mut files = BTreeSet::new();
    collect_input_files_query_plan(&plan, &mut files)?;
    Ok(InputFilesResponse {
        files: files.into_iter().collect(),
    })
}

pub(crate) async fn handle_analyze_spark_version(
    _ctx: &SessionContext,
    _request: SparkVersionRequest,
) -> SparkResult<SparkVersionResponse> {
    let version = get_pyspark_version()?;
    Ok(SparkVersionResponse { version })
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
    request: SameSemanticsRequest,
) -> SparkResult<SameSemanticsResponse> {
    let SameSemanticsRequest {
        target_plan,
        other_plan,
    } = request;
    let target_plan = target_plan.required("target plan")?;
    let other_plan = other_plan.required("other plan")?;
    let target = semantic_plan_key(target_plan)?;
    let other = semantic_plan_key(other_plan)?;
    Ok(SameSemanticsResponse {
        result: target == other,
    })
}

pub(crate) async fn handle_analyze_semantic_hash(
    _ctx: &SessionContext,
    request: SemanticHashRequest,
) -> SparkResult<SemanticHashResponse> {
    let SemanticHashRequest { plan } = request;
    let plan = plan.required("plan")?;
    let key = semantic_plan_key(plan)?;
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    Ok(SemanticHashResponse {
        result: (hasher.finish() & 0xffff_ffff) as u32 as i32,
    })
}

pub(crate) async fn handle_analyze_persist(
    ctx: &SessionContext,
    request: PersistRequest,
) -> SparkResult<PersistResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let PersistRequest {
        relation,
        storage_level,
    } = request;
    let relation = relation.required("persist relation")?;
    let relation_key = relation_semantic_key(relation)?;
    let storage_level = storage_level.unwrap_or_else(default_storage_level);
    spark.persist_relation(relation_key, storage_level)?;
    warn!("Persist operation does not cache data yet; only storage level state is tracked");
    Ok(PersistResponse {})
}

pub(crate) async fn handle_analyze_unpersist(
    ctx: &SessionContext,
    request: UnpersistRequest,
) -> SparkResult<UnpersistResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let UnpersistRequest { relation, .. } = request;
    let relation = relation.required("unpersist relation")?;
    let relation_key = relation_semantic_key(relation)?;
    spark.unpersist_relation(&relation_key)?;
    Ok(UnpersistResponse {})
}

pub(crate) async fn handle_analyze_get_storage_level(
    ctx: &SessionContext,
    request: GetStorageLevelRequest,
) -> SparkResult<GetStorageLevelResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let GetStorageLevelRequest { relation } = request;
    let relation = relation.required("storage level relation")?;
    let relation_key = relation_semantic_key(relation)?;
    Ok(GetStorageLevelResponse {
        storage_level: Some(spark.get_storage_level(&relation_key)?),
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
    let op = match op.required("plan op")? {
        plan::OpType::CompressedOperation(operation) => decompress_operation(operation)?,
        op => op,
    };
    match op {
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
            Err(SparkError::internal("nested compressed operation"))
        }
    }
}

fn semantic_plan_key(plan: sc::Plan) -> SparkResult<String> {
    let sc::Plan { op_type: op } = plan;
    let op = match op.required("plan op")? {
        plan::OpType::CompressedOperation(operation) => decompress_operation(operation)?,
        op => op,
    };
    match op {
        plan::OpType::Command(_) => return Err(SparkError::invalid("relation expected")),
        plan::OpType::Root(relation) => relation_semantic_key(relation),
        plan::OpType::CompressedOperation(_) => {
            return Err(SparkError::internal("nested compressed operation"))
        }
    }
}

fn relation_semantic_key(relation: sc::Relation) -> SparkResult<String> {
    let plan = spec::Plan::try_from(relation)?;
    let mut value = serde_json::to_value(plan)?;
    normalize_semantic_value(&mut value);
    Ok(value.to_string())
}

fn default_storage_level() -> StorageLevel {
    StorageLevel {
        use_disk: true,
        use_memory: true,
        use_off_heap: false,
        deserialized: true,
        replication: 1,
    }
}

fn normalize_semantic_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.remove("planId");
            if let Some(serde_json::Value::Object(alias)) = map.get_mut("alias") {
                if alias.contains_key("expr") && alias.contains_key("name") {
                    alias.insert("name".to_string(), serde_json::Value::Array(vec![]));
                    alias.remove("metadata");
                }
            }
            for value in map.values_mut() {
                normalize_semantic_value(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_semantic_value(value);
            }
        }
        _ => {}
    }
}

fn analyze_is_streaming(plan: sc::Plan) -> SparkResult<bool> {
    let sc::Plan { op_type: op } = plan;
    let op = match op.required("plan op")? {
        plan::OpType::CompressedOperation(operation) => decompress_operation(operation)?,
        op => op,
    };
    match op {
        plan::OpType::Command(_) => Ok(false),
        plan::OpType::Root(relation) => {
            let plan: spec::Plan = relation.try_into()?;
            match plan {
                spec::Plan::Command(_) => Ok(false),
                spec::Plan::Query(query) => Ok(is_streaming_query_plan(&query)),
            }
        }
        plan::OpType::CompressedOperation(_) => {
            Err(SparkError::internal("nested compressed operation"))
        }
    }
}

fn collect_input_files_query_plan(
    plan: &spec::QueryPlan,
    files: &mut BTreeSet<String>,
) -> SparkResult<()> {
    collect_input_files_query_node(&plan.node, files)
}

fn collect_input_files_query_node(
    node: &spec::QueryNode,
    files: &mut BTreeSet<String>,
) -> SparkResult<()> {
    match node {
        spec::QueryNode::Read { read_type, .. } => {
            if let spec::ReadType::DataSource(source) = read_type {
                for path in &source.paths {
                    collect_input_files_path(path, files)?;
                }
                for (key, value) in &source.options {
                    if key.eq_ignore_ascii_case("path") {
                        collect_input_files_path(value, files)?;
                    }
                }
            }
        }
        // leaf nodes with no query plan inputs
        spec::QueryNode::LocalRelation { .. }
        | spec::QueryNode::CachedLocalRelation { .. }
        | spec::QueryNode::CachedRemoteRelation { .. }
        | spec::QueryNode::Range(_)
        | spec::QueryNode::Empty { .. }
        | spec::QueryNode::Values(_)
        | spec::QueryNode::CommonInlineUserDefinedTableFunction(_) => {}
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
        | spec::QueryNode::TableSample { input, .. } => {
            collect_input_files_query_plan(input, files)?;
        }
        // single optional input - None input means no source
        spec::QueryNode::Project { input, .. } | spec::QueryNode::LateralView { input, .. } => {
            if let Some(input) = input {
                collect_input_files_query_plan(input, files)?;
            }
        }
        // nested struct with single input
        spec::QueryNode::Aggregate(agg) => collect_input_files_query_plan(&agg.input, files)?,
        spec::QueryNode::Sample(s) => collect_input_files_query_plan(&s.input, files)?,
        spec::QueryNode::Deduplicate(d) => collect_input_files_query_plan(&d.input, files)?,
        spec::QueryNode::Pivot(p) => collect_input_files_query_plan(&p.input, files)?,
        spec::QueryNode::Unpivot(u) => collect_input_files_query_plan(&u.input, files)?,
        spec::QueryNode::Parse(p) => collect_input_files_query_plan(&p.input, files)?,
        spec::QueryNode::WithWatermark(w) => collect_input_files_query_plan(&w.input, files)?,
        spec::QueryNode::ApplyInPandasWithState(a) => {
            collect_input_files_query_plan(&a.input, files)?;
        }
        // multiple inputs
        spec::QueryNode::Join(j) => {
            collect_input_files_query_plan(&j.left, files)?;
            collect_input_files_query_plan(&j.right, files)?;
        }
        spec::QueryNode::SetOperation(s) => {
            collect_input_files_query_plan(&s.left, files)?;
            collect_input_files_query_plan(&s.right, files)?;
        }
        spec::QueryNode::CoGroupMap(c) => {
            collect_input_files_query_plan(&c.input, files)?;
            collect_input_files_query_plan(&c.other, files)?;
        }
        spec::QueryNode::GroupMap(g) => {
            collect_input_files_query_plan(&g.input, files)?;
            if let Some(input) = &g.initial_input {
                collect_input_files_query_plan(input, files)?;
            }
        }
        spec::QueryNode::WithCtes { input, ctes, .. } => {
            collect_input_files_query_plan(input, files)?;
            for (_name, plan) in ctes {
                collect_input_files_query_plan(plan, files)?;
            }
        }
        spec::QueryNode::WithRelations { root, references } => {
            collect_input_files_query_plan(root, files)?;
            for plan in references {
                collect_input_files_query_plan(plan, files)?;
            }
        }
        spec::QueryNode::LateralJoin { left, right, .. } => {
            collect_input_files_query_plan(left, files)?;
            collect_input_files_query_plan(right, files)?;
        }
    }
    Ok(())
}

fn collect_input_files_path(path: &str, files: &mut BTreeSet<String>) -> SparkResult<()> {
    let Some(path) = local_input_path(path) else {
        files.insert(path.to_string());
        return Ok(());
    };
    collect_local_input_files(&path, files)
}

fn local_input_path(path: &str) -> Option<PathBuf> {
    if let Some(path) = path.strip_prefix("file://") {
        Some(PathBuf::from(path))
    } else if path.contains("://") {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

fn collect_local_input_files(path: &Path, files: &mut BTreeSet<String>) -> SparkResult<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.is_file() {
        if is_data_file(path) {
            files.insert(local_file_uri(path));
        }
    } else if metadata.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if is_data_file(&path) {
                collect_local_input_files(&path, files)?;
            }
        }
    }
    Ok(())
}

fn is_data_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_none_or(|name| !name.starts_with('_') && !name.starts_with('.'))
}

fn local_file_uri(path: &Path) -> String {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    format!("file://{}", path.display())
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
