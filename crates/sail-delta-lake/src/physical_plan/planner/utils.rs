// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{
    Column as LogicalColumn, DataFusionError, Result, ScalarValue, ToDFSchema,
};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{Case, Cast, ScalarFunction};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_functions_nested::extract::array_element_udf;
use datafusion_functions_nested::map_extract::map_extract_udf;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use url::Url;

use super::context::PlannerContext;
use super::log_scan::{LogScanOptions, build_delta_log_datasource_scans_with_options};
use super::log_segment::{LogSegmentResolveOptions, resolve_log_segment_files};
use crate::datasource::{
    COMMIT_TIMESTAMP_COLUMN, COMMIT_VERSION_COLUMN, PATH_COLUMN, simplify_expr,
};
use crate::options::DeltaLogReplayStrategy;
use crate::physical_plan::{
    COL_LOG_IS_REMOVE, COL_LOG_VERSION, COL_REPLAY_PATH, DeltaCommitExec, DeltaLogReplayExec,
    DeltaWriterExec, DeltaWriterExecOptions, create_projection, create_repartition, create_sort,
};
use crate::schema::PhysicalPartitionColumn;
use crate::spec::fields::{
    DV_FIELD_OFFSET, DV_FIELD_PATH_OR_INLINE_DV, DV_FIELD_STORAGE_TYPE, FIELD_NAME_DELETION_VECTOR,
    FIELD_NAME_MODIFICATION_TIME, FIELD_NAME_PATH, FIELD_NAME_SIZE, FIELD_NAME_STATS,
};
use crate::table::DeltaSnapshot;

/// Options that control what the log replay pipeline materializes as payload columns.
///
/// This is intentionally kept small: it is primarily used to avoid scanning/transporting
/// `stats_json` unless downstream pruning (data skipping) actually needs it.
#[derive(Debug, Clone)]
pub struct LogReplayOptions {
    /// Whether to include `stats_json` in the replay output (as a Utf8 column).
    pub include_stats_json: bool,
    /// Whether to carry Add-action metadata fields needed to faithfully re-emit an Add action.
    pub include_extended_add_metadata: bool,
    /// Optional inclusive log version range for commit JSON files.
    pub commit_version_range: Option<(i64, i64)>,
    /// Optional metadata-stage filter applied after log replay.
    pub log_filter: Option<LogReplayFilter>,
    /// Optional predicate pushed down to checkpoint parquet scan.
    pub parquet_predicate: Option<Arc<dyn PhysicalExpr>>,
}

#[derive(Debug, Clone)]
pub struct LogReplayFilter {
    pub predicate: Arc<dyn PhysicalExpr>,
    pub table_schema: SchemaRef,
}

fn utf8_literal(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
}

fn struct_field_expr(struct_expr: Expr, field_name: &str) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::get_field(),
        vec![struct_expr, utf8_literal(field_name)],
    ))
}

fn concat_utf8(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::string::concat(),
        args,
    ))
}

fn first_matching_field<'a>(fields: &Fields, names: &'a [&str]) -> Option<&'a str> {
    names
        .iter()
        .copied()
        .find(|name| fields.iter().any(|field| field.name() == *name))
}

fn action_replay_key_expr(
    action_expr: Expr,
    action_is_not_null: Expr,
    action_fields: &Fields,
) -> Result<Expr> {
    let path = Expr::Cast(Cast::new(
        Box::new(struct_field_expr(action_expr.clone(), FIELD_NAME_PATH)),
        DataType::Utf8,
    ));
    let path_bytes = Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::string::octet_length(),
        vec![path.clone()],
    ));
    let path_bytes = Expr::Cast(Cast::new(Box::new(path_bytes), DataType::Utf8));

    let dv_identity = if let Some(dv_field_name) = first_matching_field(
        action_fields,
        &[FIELD_NAME_DELETION_VECTOR, "deletion_vector"],
    ) {
        let dv_field = action_fields
            .iter()
            .find(|field| field.name() == dv_field_name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "deletion vector field '{dv_field_name}' disappeared from action schema"
                ))
            })?;
        let DataType::Struct(dv_fields) = dv_field.data_type() else {
            return Err(DataFusionError::Plan(format!(
                "log replay expects '{dv_field_name}' to be Struct, got {}",
                dv_field.data_type()
            )));
        };
        let storage_type_field =
            first_matching_field(dv_fields, &[DV_FIELD_STORAGE_TYPE, "storage_type"]).ok_or_else(
                || DataFusionError::Plan("deletion vector is missing storageType".to_string()),
            )?;
        let path_or_inline_field = first_matching_field(
            dv_fields,
            &[DV_FIELD_PATH_OR_INLINE_DV, "path_or_inline_dv"],
        )
        .ok_or_else(|| {
            DataFusionError::Plan("deletion vector is missing pathOrInlineDv".to_string())
        })?;

        let dv_expr = struct_field_expr(action_expr.clone(), dv_field_name);
        let storage_type = Expr::Cast(Cast::new(
            Box::new(struct_field_expr(dv_expr.clone(), storage_type_field)),
            DataType::Utf8,
        ));
        let path_or_inline = Expr::Cast(Cast::new(
            Box::new(struct_field_expr(dv_expr.clone(), path_or_inline_field)),
            DataType::Utf8,
        ));
        let offset_suffix =
            if let Some(offset_field) = first_matching_field(dv_fields, &[DV_FIELD_OFFSET]) {
                let offset = struct_field_expr(dv_expr.clone(), offset_field);
                Expr::Case(Case::new(
                    None,
                    vec![(
                        Box::new(offset.clone().is_not_null()),
                        Box::new(concat_utf8(vec![
                            utf8_literal("@"),
                            Expr::Cast(Cast::new(Box::new(offset), DataType::Utf8)),
                        ])),
                    )],
                    Some(Box::new(utf8_literal(""))),
                ))
            } else {
                utf8_literal("")
            };
        let unique_id = concat_utf8(vec![storage_type, path_or_inline, offset_suffix]);
        Expr::Case(Case::new(
            None,
            vec![(
                Box::new(action_is_not_null.clone().and(dv_expr.is_not_null())),
                Box::new(concat_utf8(vec![utf8_literal("1:"), unique_id])),
            )],
            Some(Box::new(utf8_literal("0"))),
        ))
    } else {
        utf8_literal("0")
    };

    // Length-prefix the path so the composite string remains injective even when a path contains
    // delimiter-like text. The resulting Utf8 value is opaque to Sort/Hash replay operators.
    let key = concat_utf8(vec![
        path_bytes,
        utf8_literal(":"),
        path,
        utf8_literal(":"),
        dv_identity,
    ]);
    Ok(Expr::Case(Case::new(
        None,
        vec![(Box::new(action_is_not_null), Box::new(key))],
        None,
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayPipelineMode {
    Sort,
    Hash,
    HashCommits,
}

fn select_replay_pipeline_mode(
    strategy: DeltaLogReplayStrategy,
    has_checkpoint: bool,
) -> ReplayPipelineMode {
    match strategy {
        DeltaLogReplayStrategy::Sort => ReplayPipelineMode::Sort,
        DeltaLogReplayStrategy::Hash if has_checkpoint => ReplayPipelineMode::Hash,
        DeltaLogReplayStrategy::Hash => ReplayPipelineMode::HashCommits,
        // Commit file count does not bound the number of actions or hash-table memory. Keep the
        // default path spill-friendly; hash replay remains available through the explicit Hash
        // strategy.
        DeltaLogReplayStrategy::Auto => ReplayPipelineMode::Sort,
    }
}

impl Default for LogReplayOptions {
    fn default() -> Self {
        Self {
            // Preserve current behavior: always project stats.
            include_stats_json: true,
            include_extended_add_metadata: false,
            commit_version_range: None,
            log_filter: None,
            parquet_predicate: None,
        }
    }
}

fn replay_output_schema(
    partition_columns: &[PhysicalPartitionColumn],
    include_stats_json: bool,
    include_extended_add_metadata: bool,
) -> SchemaRef {
    let mut fields = vec![
        Field::new(PATH_COLUMN, DataType::Utf8, true),
        Field::new("size_bytes", DataType::Int64, true),
        Field::new("modification_time", DataType::Int64, true),
        Field::new(COMMIT_VERSION_COLUMN, DataType::Int64, true),
        Field::new(COMMIT_TIMESTAMP_COLUMN, DataType::Int64, true),
    ];
    for column in partition_columns {
        fields.push(Field::new(
            column.logical_name.clone(),
            DataType::Utf8,
            true,
        ));
    }
    if include_stats_json {
        fields.push(Field::new("stats_json", DataType::Utf8, true));
    }
    if include_extended_add_metadata {
        let map_entries = DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Utf8, true)),
            ]
            .into(),
        );
        fields.push(Field::new(
            "tags",
            DataType::Map(Arc::new(Field::new("entries", map_entries, false)), false),
            true,
        ));
        fields.push(Field::new("baseRowId", DataType::Int64, true));
        fields.push(Field::new("defaultRowCommitVersion", DataType::Int64, true));
        fields.push(Field::new("clusteringProvider", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

pub fn build_standard_write_layers(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: &PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
    original_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let plan = create_projection(Arc::clone(&input), ctx.partition_columns().to_vec())?;
    let plan = create_repartition(plan, ctx.partition_columns().to_vec(), target_partitions)?;
    let plan = create_sort(plan, ctx.partition_columns().to_vec(), sort_order)?;

    let writer_schema = plan.schema();
    let write_context = ctx.prepare_write_context(&writer_schema, sink_mode, None)?;
    let writer = Arc::new(DeltaWriterExec::new(
        plan,
        ctx.table_url().clone(),
        DeltaWriterExecOptions::from(ctx.options().clone())
            .with_generation_expressions(ctx.generation_expressions().clone())
            .with_identity_columns(ctx.identity_columns().clone()),
        ctx.metadata_configuration().clone(),
        ctx.partition_columns().to_vec(),
        sink_mode.clone(),
        ctx.table_exists(),
        writer_schema,
        write_context.clone(),
        ctx.lakehouse_table().cloned(),
    )?);

    // DeltaCommitExec is single-partition; gather writer partitions first.
    let writer: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(writer));

    Ok(Arc::new(DeltaCommitExec::new(
        writer,
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        original_schema,
        sink_mode.clone(),
        ctx.options().user_metadata.clone(),
        write_context.commit_context.clone(),
        ctx.lakehouse_table().cloned(),
    )))
}

pub fn align_schemas_for_union(
    new_data_plan: Arc<dyn ExecutionPlan>,
    old_data_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let new_schema = new_data_plan.schema();
    let old_schema = old_data_plan.schema();

    if new_schema.fields().len() != old_schema.fields().len() {
        return Err(DataFusionError::Plan(
            "Schema mismatch between new and old data - schema evolution not yet implemented"
                .to_string(),
        ));
    }

    let mut new_projections = Vec::new();
    let mut old_projections = Vec::new();

    for (i, field) in new_schema.fields().iter().enumerate() {
        new_projections.push((
            Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));

        if let Some((old_idx, _)) = old_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, old_field)| old_field.name() == field.name())
        {
            old_projections.push((
                Arc::new(Column::new(field.name(), old_idx)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Field '{}' not found in old data schema",
                field.name()
            )));
        }
    }

    let aligned_new = Arc::new(ProjectionExec::try_new(new_projections, new_data_plan)?);
    let aligned_old = Arc::new(ProjectionExec::try_new(old_projections, old_data_plan)?);

    Ok((aligned_new, aligned_old))
}

/// Build the standard log replay pipeline:
/// `Union(DataSourceExec)` -> `Projection(payload + replay_keys)` -> `Repartition(Hash replay_path)`
/// -> `[optional Sort(replay_path, log_version desc, preserve_partitioning)]`
/// -> `DeltaLogReplayExec`.
pub async fn build_log_replay_pipeline(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
) -> Result<Arc<dyn ExecutionPlan>> {
    build_log_replay_pipeline_with_options(ctx, snapshot, LogReplayOptions::default()).await
}

/// Same as [`build_log_replay_pipeline`], but allows controlling projected payload columns.
pub async fn build_log_replay_pipeline_with_options(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
    options: LogReplayOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let version = snapshot.version();
    let log_segment_files = resolve_log_segment_files(
        ctx,
        version,
        LogSegmentResolveOptions {
            commit_version_range: options.commit_version_range,
        },
        snapshot.load_config().catalog_managed_commits.as_ref(),
    )
    .await?;
    build_log_replay_pipeline_with_files(
        ctx,
        ctx.table_url().clone(),
        version,
        snapshot.physical_partition_columns(),
        log_segment_files.checkpoint_files,
        log_segment_files.commit_files,
        log_segment_files.sidecar_files,
        options,
    )
    .await
}

async fn build_log_replay_pipeline_with_files(
    ctx: &PlannerContext<'_>,
    table_url: Url,
    version: i64,
    partition_columns: Vec<PhysicalPartitionColumn>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    sidecar_files: Vec<String>,
    options: LogReplayOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let log_scan_options = LogScanOptions {
        projection: Some(vec!["add".to_string(), "remove".to_string()]),
        parquet_predicate: options.parquet_predicate,
    };
    let (checkpoint_scan_opt, commit_scan_opt, checkpoint_files, commit_files) =
        build_delta_log_datasource_scans_with_options(
            ctx,
            checkpoint_files,
            commit_files,
            sidecar_files,
            log_scan_options,
        )
        .await?;

    // Projection#1: build a compact log scan schema for streaming replay.
    //
    // - replay_path is the Delta logical-file identity (path plus optional deletion-vector ID)
    // - is_remove  = remove_struct IS NOT NULL AND add_struct IS NULL
    // - __sail_delta_log_version is passed through from the scan as a partition column
    // - payload columns are extracted up-front so the sort/replay does not carry wide structs
    let input_schema = checkpoint_scan_opt
        .as_ref()
        .map(|p| p.schema())
        .or_else(|| commit_scan_opt.as_ref().map(|p| p.schema()))
        .ok_or_else(|| {
            DataFusionError::Plan(
                "no _delta_log scans available to build replay pipeline".to_string(),
            )
        })?;
    let log_version_idx = input_schema.index_of(COL_LOG_VERSION)?;
    let df_schema = input_schema.clone().to_dfschema()?;
    let simplify = |expr: Expr| simplify_expr(ctx.session(), &df_schema, expr);

    if input_schema.field_with_name("add").is_err() {
        // Some tables/log ranges contain only metadata/protocol/remove actions.
        // Without any `add` payload there are no data files to replay.
        let replay: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(replay_output_schema(
                &partition_columns,
                options.include_stats_json,
                options.include_extended_add_metadata,
            )),
        );

        let replay: Arc<dyn ExecutionPlan> = if let Some(filter) = options.log_filter {
            let adapter_factory = Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
            let adapter = adapter_factory
                .create(filter.table_schema, replay.schema())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let adapted = adapter
                .rewrite(filter.predicate)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Arc::new(FilterExec::try_new(adapted, replay)?)
        } else {
            replay
        };

        return Ok(replay);
    }

    let col_expr = |name: &str| Expr::Column(LogicalColumn::new_unqualified(name));
    let lit_str = |s: &str| Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
    let lit_i64 = |v: i64| Expr::Literal(ScalarValue::Int64(Some(v)), None);
    let lit_bool = |v: bool| Expr::Literal(ScalarValue::Boolean(Some(v)), None);
    let lit_utf8_null = || Expr::Literal(ScalarValue::Utf8(None), None);
    let get_field_expr = |struct_expr: Expr, field_name: &str| {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions::core::get_field(),
            vec![struct_expr, lit_str(field_name)],
        ))
    };
    let guard_with = |cond: Expr, then_expr: Expr| {
        Expr::Case(Case::new(
            None,
            vec![(Box::new(cond), Box::new(then_expr))],
            None,
        ))
    };

    // `add` is required for replay payload extraction.
    let add_col_expr = col_expr("add");
    let has_remove_column = input_schema.field_with_name("remove").is_ok();

    let add_is_not_null = add_col_expr.clone().is_not_null();
    let remove_col_expr = has_remove_column.then(|| col_expr("remove"));
    let remove_is_not_null = remove_col_expr
        .as_ref()
        .map(|e| e.clone().is_not_null())
        .unwrap_or_else(|| lit_bool(false));

    let add_field = input_schema.field_with_name("add")?;
    let add_struct_fields = match add_field.data_type() {
        DataType::Struct(fields) => fields,
        other => {
            return Err(DataFusionError::Plan(format!(
                "log replay expects 'add' to be Struct, got {other}"
            )));
        }
    };
    let remove_struct_fields = if has_remove_column {
        let remove_field = input_schema.field_with_name("remove")?;
        match remove_field.data_type() {
            DataType::Struct(fields) => Some(fields),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "log replay expects 'remove' to be Struct, got {other}"
                )));
            }
        }
    } else {
        None
    };

    let add_key = action_replay_key_expr(
        add_col_expr.clone(),
        add_is_not_null.clone(),
        add_struct_fields,
    )?;
    let remove_key = match (remove_col_expr.as_ref(), remove_struct_fields) {
        (Some(remove), Some(fields)) => {
            action_replay_key_expr(remove.clone(), remove_is_not_null.clone(), fields)?
        }
        _ => lit_utf8_null(),
    };

    let replay_path = simplify(Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::coalesce(),
        vec![add_key, remove_key],
    )))?;

    // Mark tombstones using the struct's own validity.
    let is_remove = simplify(
        remove_is_not_null
            .clone()
            .and(Expr::Not(Box::new(add_is_not_null.clone()))),
    )?;

    // Extract a stable "metadata table" schema from `add` up-front so replay can stream
    // over narrow payload columns.
    let has_add_field = |name: &str| add_struct_fields.iter().any(|f| f.name() == name);
    let mod_time_field = if has_add_field(FIELD_NAME_MODIFICATION_TIME) {
        FIELD_NAME_MODIFICATION_TIME
    } else {
        "modification_time"
    };
    let part_values_field = if has_add_field("partitionValues") {
        "partitionValues"
    } else {
        "partition_values"
    };
    let stats_field = if has_add_field(FIELD_NAME_STATS) {
        FIELD_NAME_STATS
    } else {
        "stats_json"
    };
    let add_field_name = |names: &[&'static str]| -> Option<&'static str> {
        names.iter().copied().find(|name| has_add_field(name))
    };

    let get_add_field = |field_name: &str| get_field_expr(add_col_expr.clone(), field_name);
    let guard_add = |e: Expr| guard_with(add_is_not_null.clone(), e);

    let path_expr = simplify(Expr::Cast(Cast::new(
        Box::new(guard_add(get_add_field(FIELD_NAME_PATH))),
        DataType::Utf8,
    )))?;

    let size_expr_i64 = Expr::Cast(Cast::new(
        Box::new(guard_add(get_add_field(FIELD_NAME_SIZE))),
        DataType::Int64,
    ));
    let size_expr = simplify(Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::coalesce(),
        vec![size_expr_i64, lit_i64(0)],
    )))?;

    let mod_time_expr_i64 = Expr::Cast(Cast::new(
        Box::new(guard_add(get_add_field(mod_time_field))),
        DataType::Int64,
    ));
    let mod_time_expr = simplify(Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::coalesce(),
        vec![mod_time_expr_i64, lit_i64(0)],
    )))?;

    let stats_expr = if options.include_stats_json {
        Some(simplify(Expr::Cast(Cast::new(
            Box::new(guard_add(get_add_field(stats_field))),
            DataType::Utf8,
        )))?)
    } else {
        None
    };

    let part_values = guard_add(get_add_field(part_values_field));
    let part_expr_for = |physical: &str| -> Result<Arc<dyn PhysicalExpr>> {
        let extract_elem = |key: &str| {
            let extracted = Expr::ScalarFunction(ScalarFunction::new_udf(
                map_extract_udf(),
                vec![part_values.clone(), lit_str(key)],
            ));
            Expr::ScalarFunction(ScalarFunction::new_udf(
                array_element_udf(),
                vec![extracted, lit_i64(1)],
            ))
        };
        simplify(Expr::Cast(Cast::new(
            Box::new(extract_elem(physical)),
            DataType::Utf8,
        )))
    };

    let mut final_proj: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(6 + partition_columns.len() + 1);

    // Payload columns (the replay output schema).
    final_proj.push((path_expr, PATH_COLUMN.to_string()));
    final_proj.push((size_expr, "size_bytes".to_string()));
    final_proj.push((Arc::clone(&mod_time_expr), "modification_time".to_string()));
    let unknown_commit_metadata = simplify(Expr::Literal(ScalarValue::Int64(None), None))?;
    final_proj.push((
        Arc::clone(&unknown_commit_metadata),
        COMMIT_VERSION_COLUMN.to_string(),
    ));
    final_proj.push((unknown_commit_metadata, COMMIT_TIMESTAMP_COLUMN.to_string()));
    for column in &partition_columns {
        final_proj.push((
            part_expr_for(&column.physical_name)?,
            column.logical_name.clone(),
        ));
    }
    if let Some(stats_expr) = stats_expr {
        final_proj.push((stats_expr, "stats_json".to_string()));
    }

    if options.include_extended_add_metadata {
        if let Some(field) = add_field_name(&["tags"]) {
            final_proj.push((
                simplify(guard_add(get_add_field(field)))?,
                "tags".to_string(),
            ));
        }
        if let Some(field) = add_field_name(&["baseRowId", "base_row_id"]) {
            final_proj.push((
                simplify(Expr::Cast(Cast::new(
                    Box::new(guard_add(get_add_field(field))),
                    DataType::Int64,
                )))?,
                "baseRowId".to_string(),
            ));
        }
        if let Some(field) =
            add_field_name(&["defaultRowCommitVersion", "default_row_commit_version"])
        {
            final_proj.push((
                simplify(Expr::Cast(Cast::new(
                    Box::new(guard_add(get_add_field(field))),
                    DataType::Int64,
                )))?,
                "defaultRowCommitVersion".to_string(),
            ));
        }
        if let Some(field) = add_field_name(&["clusteringProvider", "clustering_provider"]) {
            final_proj.push((
                simplify(Expr::Cast(Cast::new(
                    Box::new(guard_add(get_add_field(field))),
                    DataType::Utf8,
                )))?,
                "clusteringProvider".to_string(),
            ));
        }
    }

    // Include the deletion vector struct so DeltaScanByAddsExec can apply per-file DV filtering.
    let dv_field_name = if has_add_field("deletionVector") {
        Some("deletionVector")
    } else if has_add_field("deletion_vector") {
        Some("deletion_vector")
    } else {
        None
    };
    if let Some(dv_field) = dv_field_name {
        let dv_expr = simplify(guard_add(get_add_field(dv_field)))?;
        final_proj.push((dv_expr, "deletionVector".to_string()));
    }

    // Replay key columns (consumed by replay; stripped from replay output schema).
    final_proj.push((Arc::clone(&replay_path), COL_REPLAY_PATH.to_string()));
    final_proj.push((is_remove, COL_LOG_IS_REMOVE.to_string()));
    final_proj.push((
        Arc::new(Column::new(COL_LOG_VERSION, log_version_idx)) as Arc<dyn PhysicalExpr>,
        COL_LOG_VERSION.to_string(),
    ));

    let log_partitions = ctx.session().config().target_partitions().max(1);

    let empty_scan = |schema: SchemaRef| -> Arc<dyn ExecutionPlan> {
        Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema))
    };

    let build_branch = |scan: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        // Keep the raw-row hash repartition through distribution enforcement. The explicit node
        // is rewritten to an executable repartition after ProjectionExec maps its expression to
        // COL_REPLAY_PATH.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            scan,
            Partitioning::Hash(vec![Arc::clone(&replay_path)], log_partitions),
        ));
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(final_proj.clone(), plan)?);
        Ok(plan)
    };

    let replay_strategy = ctx.options().delta_log_replay_strategy;
    let has_checkpoint = !checkpoint_files.is_empty();
    let replay_mode = select_replay_pipeline_mode(replay_strategy, has_checkpoint);

    let replay: Arc<dyn ExecutionPlan> = match replay_mode {
        ReplayPipelineMode::Sort => {
            let mut scans = Vec::with_capacity(2);
            if let Some(checkpoint_scan) = checkpoint_scan_opt {
                scans.push(checkpoint_scan);
            }
            if let Some(commit_scan) = commit_scan_opt {
                scans.push(commit_scan);
            }
            let scan = match scans.len() {
                0 => empty_scan(Arc::clone(&input_schema)),
                1 => scans.remove(0),
                _ => UnionExec::try_new(scans)?,
            };
            let input = build_branch(scan)?;
            Arc::new(DeltaLogReplayExec::new(
                input,
                table_url,
                version,
                checkpoint_files,
                commit_files,
            ))
        }
        ReplayPipelineMode::Hash => {
            let checkpoint_scan =
                checkpoint_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));
            let commit_scan =
                commit_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));
            let checkpoint = build_branch(checkpoint_scan)?;
            let commits = build_branch(commit_scan)?;
            Arc::new(DeltaLogReplayExec::try_new_hash(
                checkpoint,
                commits,
                table_url,
                version,
                checkpoint_files,
                commit_files,
            )?)
        }
        ReplayPipelineMode::HashCommits => {
            let commit_scan =
                commit_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));
            let commits = build_branch(commit_scan)?;
            Arc::new(DeltaLogReplayExec::new_hash_commits(
                commits,
                table_url,
                version,
                checkpoint_files,
                commit_files,
            ))
        }
    };

    let replay: Arc<dyn ExecutionPlan> = if let Some(filter) = options.log_filter {
        let adapter_factory = Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
        let adapter = adapter_factory
            .create(filter.table_schema, replay.schema())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let adapted = adapter
            .rewrite(filter.predicate)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Arc::new(FilterExec::try_new(adapted, replay)?)
    } else {
        replay
    };

    // Replay now outputs the extracted payload columns directly (replay keys are stripped).
    Ok(replay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_replay_pipeline_mode_for_strategy_and_checkpoint_presence() {
        let cases = [
            (
                DeltaLogReplayStrategy::Sort,
                false,
                ReplayPipelineMode::Sort,
            ),
            (DeltaLogReplayStrategy::Sort, true, ReplayPipelineMode::Sort),
            (
                DeltaLogReplayStrategy::Hash,
                false,
                ReplayPipelineMode::HashCommits,
            ),
            (DeltaLogReplayStrategy::Hash, true, ReplayPipelineMode::Hash),
            (
                DeltaLogReplayStrategy::Auto,
                false,
                ReplayPipelineMode::Sort,
            ),
            (DeltaLogReplayStrategy::Auto, true, ReplayPipelineMode::Sort),
        ];

        for (strategy, has_checkpoint, expected) in cases {
            assert_eq!(
                select_replay_pipeline_mode(strategy, has_checkpoint),
                expected
            );
        }
    }
}
