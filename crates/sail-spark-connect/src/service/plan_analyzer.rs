use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, InvariantLevel};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::function_rewrite::ApplyFunctionRewrites;
use datafusion::prelude::SessionContext;
use prost::Message;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::schema::rename_schema_with_metadata;
use sail_plan::explain::{ExplainOptions, explain_string};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::plan::NamedPlan;

use crate::config::get_pyspark_version;
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
use crate::spark::connect::plan;

async fn analyze_schema(ctx: &SessionContext, plan: sc::Plan) -> SparkResult<sc::DataType> {
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan {
        mut plan,
        fields,
        field_metadata,
    } = resolver
        .resolve_named_plan(spec::Plan::Query(plan.try_into()?))
        .await?;
    let session_state = ctx.state();
    let analyzer = session_state.analyzer();
    let config = session_state.config_options();
    // Apply the same analysis rules as execution so coercion and UDF resolution
    // determine the final schema. Do not validate executable invariants here:
    // DataFusion recursively checks subqueries both explicitly and through
    // apply_with_subqueries, making that validation exponential in nesting depth.
    // Execution still performs its normal validation before optimization.
    plan.check_invariants(InvariantLevel::Always)?;
    if !analyzer.function_rewrites.is_empty() {
        plan = ApplyFunctionRewrites::new(analyzer.function_rewrites.clone())
            .analyze(plan, config.as_ref())?;
    }
    for rule in &analyzer.rules {
        plan = rule
            .analyze(plan, config.as_ref())
            .map_err(|error| error.context(rule.name()))?;
    }
    let schema = if let Some(fields) = fields {
        rename_schema_with_metadata(plan.schema().inner(), &fields, &field_metadata)?
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
    ctx: &SessionContext,
    request: InputFilesRequest,
) -> SparkResult<InputFilesResponse> {
    let InputFilesRequest { plan } = request;
    let plan = plan.required("plan")?;
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan { plan, .. } = resolver
        .resolve_named_plan(spec::Plan::Query(plan.try_into()?))
        .await?;
    let files = sail_data_source::listing::input_files::input_files(ctx, plan).await?;
    Ok(InputFilesResponse { files })
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

/// Returns the resolved logical plan as text, without the aliases of user-facing names and
/// with the generated field IDs (`#<n>`) and relation IDs (`#relation:<uuid>`) numbered in
/// the order they first appear. So plans that differ only in aliases or in the IDs of their
/// resolution have the same canonical form, as with the canonicalized plans of Spark.
async fn canonical_plan(ctx: &SessionContext, plan: sc::Plan) -> SparkResult<String> {
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan { plan, .. } = resolver
        .resolve_named_plan(spec::Plan::Query(plan.try_into()?))
        .await?;
    let plan = plan
        .transform_up(|node| {
            node.map_expressions(|expr| {
                expr.transform_up(|expr| match expr {
                    Expr::Alias(alias) if !alias.name.starts_with('#') => {
                        Ok(Transformed::yes(*alias.expr))
                    }
                    expr => Ok(Transformed::no(expr)),
                })
            })
        })?
        .data;
    Ok(renumber_generated_ids(
        &plan.display_indent_schema().to_string(),
    ))
}

fn renumber_generated_ids(text: &str) -> String {
    const RELATION: &str = "#relation:";
    let mut fields = HashMap::new();
    let mut relations = HashMap::new();
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('#') {
        output.push_str(&rest[..start]);
        rest = &rest[start..];
        if let Some(after) = rest.strip_prefix(RELATION) {
            let end = after
                .find(|c: char| !(c.is_ascii_hexdigit() || c == '-'))
                .unwrap_or(after.len());
            let next = relations.len();
            let id = *relations.entry(&after[..end]).or_insert(next);
            output.push_str(&format!("{RELATION}{id}"));
            rest = &after[end..];
        } else {
            let digits = rest[1..]
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len() - 1);
            if digits == 0 {
                output.push('#');
            } else {
                let next = fields.len();
                let id = *fields.entry(&rest[1..=digits]).or_insert(next);
                output.push_str(&format!("#{id}"));
            }
            rest = &rest[1 + digits..];
        }
    }
    output.push_str(rest);
    output
}

pub(crate) async fn handle_analyze_same_semantics(
    ctx: &SessionContext,
    request: SameSemanticsRequest,
) -> SparkResult<SameSemanticsResponse> {
    let SameSemanticsRequest {
        target_plan,
        other_plan,
    } = request;
    let target = canonical_plan(ctx, target_plan.required("target plan")?).await?;
    let other = canonical_plan(ctx, other_plan.required("other plan")?).await?;
    Ok(SameSemanticsResponse {
        result: target == other,
    })
}

pub(crate) async fn handle_analyze_semantic_hash(
    ctx: &SessionContext,
    request: SemanticHashRequest,
) -> SparkResult<SemanticHashResponse> {
    let SemanticHashRequest { plan } = request;
    let plan = canonical_plan(ctx, plan.required("plan")?).await?;
    // The default hasher has fixed keys, so the hash is the same across sessions.
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    Ok(SemanticHashResponse {
        result: hasher.finish() as i32,
    })
}

// The data of a persisted plan is not materialized. Only the storage level is kept,
// with the plan identified by its serialized form, so that it is reported as in Spark.

pub(crate) async fn handle_analyze_persist(
    ctx: &SessionContext,
    request: PersistRequest,
) -> SparkResult<PersistResponse> {
    let PersistRequest {
        relation,
        storage_level,
    } = request;
    let relation = relation.required("relation")?;
    let storage_level = match storage_level {
        Some(level) => level.try_into()?,
        None => "MEMORY_AND_DISK".parse()?,
    };
    ctx.extension::<CatalogManager>()?
        .cache_plan(relation.encode_to_vec(), storage_level)?;
    Ok(PersistResponse {})
}

pub(crate) async fn handle_analyze_unpersist(
    ctx: &SessionContext,
    request: UnpersistRequest,
) -> SparkResult<UnpersistResponse> {
    let UnpersistRequest {
        relation,
        blocking: _,
    } = request;
    let relation = relation.required("relation")?;
    ctx.extension::<CatalogManager>()?
        .uncache_plan(&relation.encode_to_vec())?;
    Ok(UnpersistResponse {})
}

pub(crate) async fn handle_analyze_get_storage_level(
    ctx: &SessionContext,
    request: GetStorageLevelRequest,
) -> SparkResult<GetStorageLevelResponse> {
    let GetStorageLevelRequest { relation } = request;
    let relation = relation.required("relation")?;
    let storage_level = match ctx
        .extension::<CatalogManager>()?
        .get_cached_plan_storage_level(&relation.encode_to_vec())?
    {
        Some(level) => level,
        None => "NONE".parse()?,
    };
    Ok(GetStorageLevelResponse {
        storage_level: Some(storage_level.try_into()?),
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
        spec::QueryNode::NamedWindows { input, windows: _ } => is_streaming_query_plan(input),
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

#[cfg(test)]
mod tests {
    use super::renumber_generated_ids;

    #[test]
    fn test_renumber_generated_ids() {
        let a = "Projection: #12 AS #15 [#15:Int64]\n  TableScan: #relation:3f2b6c1e-0a4d-4e8f-9b7a-2c5d8e1f4a6b projection=[#12]";
        let b = "Projection: #4 AS #9 [#9:Int64]\n  TableScan: #relation:9d1e7a2b-5c3f-4b6e-8a0d-1f2e3c4b5a69 projection=[#4]";
        let expected = "Projection: #0 AS #1 [#1:Int64]\n  TableScan: #relation:0 projection=[#0]";
        assert_eq!(renumber_generated_ids(a), expected);
        assert_eq!(renumber_generated_ids(b), expected);
    }

    #[test]
    fn test_renumber_generated_ids_keeps_other_text() {
        assert_eq!(renumber_generated_ids("a # b #"), "a # b #");
        assert_eq!(renumber_generated_ids("#7 + #7 * #8"), "#0 + #0 * #1");
    }
}
