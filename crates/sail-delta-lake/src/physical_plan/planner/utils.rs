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

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{
    Column as LogicalColumn, DataFusionError, Result, ScalarValue, ToDFSchema,
};
use datafusion::logical_expr::expr::{Case, Cast, ScalarFunction};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_functions_nested::extract::array_element_udf;
use datafusion_functions_nested::map_extract::map_extract_udf;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::context::PlannerContext;
use super::log_scan::{build_delta_log_datasource_scans_with_options, LogScanOptions};
use crate::datasource::{
    simplify_expr, COMMIT_TIMESTAMP_COLUMN, COMMIT_VERSION_COLUMN, PATH_COLUMN,
};
use crate::options::DeltaLogReplayStrategyOption;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaLogReplayExec,
    DeltaPhysicalExprAdapterFactory, DeltaWriterExec, COL_LOG_IS_REMOVE, COL_LOG_VERSION,
    COL_REPLAY_PATH,
};

/// Options that control what the log replay pipeline materializes as payload columns.
///
/// This is intentionally kept small: it is primarily used to avoid scanning/transporting
/// `stats_json` unless downstream pruning (data skipping) actually needs it.
#[derive(Debug, Clone)]
pub struct LogReplayOptions {
    /// Whether to include `stats_json` in the replay output (as a Utf8 column).
    pub include_stats_json: bool,
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

impl Default for LogReplayOptions {
    fn default() -> Self {
        Self {
            // Preserve current behavior: always project stats.
            include_stats_json: true,
            commit_version_range: None,
            log_filter: None,
            parquet_predicate: None,
        }
    }
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
    let writer = Arc::new(DeltaWriterExec::new(
        plan,
        ctx.table_url().clone(),
        ctx.options().clone(),
        ctx.partition_columns().to_vec(),
        sink_mode.clone(),
        ctx.table_exists(),
        writer_schema,
        None,
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
    table_url: Url,
    version: i64,
    partition_columns: Vec<String>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_columns = partition_columns
        .into_iter()
        .map(|col| (col.clone(), col))
        .collect::<Vec<_>>();
    build_log_replay_pipeline_with_options(
        ctx,
        table_url,
        version,
        partition_columns,
        checkpoint_files,
        commit_files,
        LogReplayOptions::default(),
    )
    .await
}

/// Same as [`build_log_replay_pipeline`], but allows controlling projected payload columns.
pub async fn build_log_replay_pipeline_with_options(
    ctx: &PlannerContext<'_>,
    table_url: Url,
    version: i64,
    partition_columns: Vec<(String, String)>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    options: LogReplayOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let log_scan_options = LogScanOptions {
        projection: Some(vec!["add".to_string(), "remove".to_string()]),
        commit_version_range: options.commit_version_range,
        parquet_predicate: options.parquet_predicate,
    };
    let (checkpoint_scan_opt, commit_scan_opt, checkpoint_files, commit_files) =
        build_delta_log_datasource_scans_with_options(
            ctx,
            checkpoint_files,
            commit_files,
            log_scan_options,
        )
        .await?;

    // Projection#1: build a compact log scan schema for streaming replay.
    //
    // - replay_path = coalesce(get_field(add, 'path'), get_field(remove, 'path'))
    // - is_remove  = remove_struct IS NOT NULL
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

    // NOTE: `get_field(struct, 'child')` does not apply the parent struct's
    // null buffer to the returned child array. We must guard child extraction with the
    // struct's validity to avoid spurious values.
    let add_path = guard_with(
        add_is_not_null.clone(),
        get_field_expr(add_col_expr.clone(), "path"),
    );
    let remove_path = remove_col_expr
        .as_ref()
        .map(|e| {
            guard_with(
                remove_is_not_null.clone(),
                get_field_expr(e.clone(), "path"),
            )
        })
        .unwrap_or_else(lit_utf8_null);

    let replay_path = simplify(Expr::ScalarFunction(ScalarFunction::new_udf(
        datafusion::functions::core::coalesce(),
        vec![add_path, remove_path.clone()],
    )))?;

    // Mark tombstones using the struct's own validity.
    let is_remove = simplify(remove_is_not_null.clone())?;

    // Extract a stable "metadata table" schema from `add` up-front so replay can stream
    // over narrow payload columns.
    let add_field = input_schema.field_with_name("add")?;
    let add_struct_fields = match add_field.data_type() {
        DataType::Struct(fields) => fields,
        other => {
            return Err(DataFusionError::Plan(format!(
                "log replay expects 'add' to be Struct, got {other}"
            )))
        }
    };
    let has_add_field = |name: &str| add_struct_fields.iter().any(|f| f.name() == name);
    let mod_time_field = if has_add_field("modificationTime") {
        "modificationTime"
    } else {
        "modification_time"
    };
    let part_values_field = if has_add_field("partitionValues") {
        "partitionValues"
    } else {
        "partition_values"
    };
    let stats_field = if has_add_field("stats") {
        "stats"
    } else {
        "stats_json"
    };

    let get_add_field = |field_name: &str| get_field_expr(add_col_expr.clone(), field_name);
    let guard_add = |e: Expr| guard_with(add_is_not_null.clone(), e);

    let path_expr = simplify(Expr::Cast(Cast::new(
        Box::new(guard_add(get_add_field("path"))),
        DataType::Utf8,
    )))?;

    let size_expr_i64 = Expr::Cast(Cast::new(
        Box::new(guard_add(get_add_field("size"))),
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
    let part_expr_for = |logical: &str, physical: &str| -> Result<Arc<dyn PhysicalExpr>> {
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
        let physical_elem = extract_elem(physical);
        let elem = if physical == logical {
            physical_elem
        } else {
            let logical_elem = extract_elem(logical);
            Expr::ScalarFunction(ScalarFunction::new_udf(
                datafusion::functions::core::coalesce(),
                vec![physical_elem, logical_elem],
            ))
        };
        simplify(Expr::Cast(Cast::new(Box::new(elem), DataType::Utf8)))
    };

    let mut final_proj: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(6 + partition_columns.len() + 1);

    // Payload columns (the replay output schema).
    final_proj.push((path_expr, PATH_COLUMN.to_string()));
    final_proj.push((size_expr, "size_bytes".to_string()));
    final_proj.push((Arc::clone(&mod_time_expr), "modification_time".to_string()));
    final_proj.push((
        Arc::new(Column::new(COL_LOG_VERSION, log_version_idx)) as Arc<dyn PhysicalExpr>,
        COMMIT_VERSION_COLUMN.to_string(),
    ));
    final_proj.push((
        Arc::clone(&mod_time_expr),
        COMMIT_TIMESTAMP_COLUMN.to_string(),
    ));
    for (logical, physical) in &partition_columns {
        final_proj.push((part_expr_for(logical, physical)?, logical.clone()));
    }
    if let Some(stats_expr) = stats_expr {
        final_proj.push((stats_expr, "stats_json".to_string()));
    }

    // Replay key columns (consumed by replay; stripped from replay output schema).
    final_proj.push((replay_path, COL_REPLAY_PATH.to_string()));
    final_proj.push((is_remove, COL_LOG_IS_REMOVE.to_string()));
    final_proj.push((
        Arc::new(Column::new(COL_LOG_VERSION, log_version_idx)) as Arc<dyn PhysicalExpr>,
        COL_LOG_VERSION.to_string(),
    ));

    let log_partitions = ctx.session().config().target_partitions().max(1);

    let replay_partition_cols = partition_columns
        .iter()
        .map(|(logical, _)| logical.clone())
        .collect::<Vec<_>>();

    let empty_scan = |schema: SchemaRef| -> Arc<dyn ExecutionPlan> {
        Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema))
    };

    let build_branch = |scan: Arc<dyn ExecutionPlan>,
                        sort: bool|
     -> Result<Arc<dyn ExecutionPlan>> {
        // Preserve existing behavior: fan out to target partitions early for stable EXPLAIN and
        // better parallelism. (This is a shuffle, but not a pipeline breaker like SortExec.)
        let plan: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            scan,
            Partitioning::RoundRobinBatch(log_partitions),
        )?);

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(final_proj.clone(), plan)?);

        // Hash partition by replay_path so all actions for the same path are co-located.
        let replay_path_idx = plan.schema().index_of(COL_REPLAY_PATH)?;
        let replay_expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> =
            Arc::new(PhysicalColumn::new(COL_REPLAY_PATH, replay_path_idx));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            plan,
            Partitioning::Hash(vec![replay_expr], log_partitions),
        )?);

        if !sort {
            return Ok(plan);
        }

        // Ensure per-partition ordering on (replay_path, log_version desc, is_remove asc)
        // for sort-based replay mode.
        let replay_path_idx = plan.schema().index_of(COL_REPLAY_PATH)?;
        let log_version_idx = plan.schema().index_of(COL_LOG_VERSION)?;
        let is_remove_idx = plan.schema().index_of(COL_LOG_IS_REMOVE)?;
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new(COL_REPLAY_PATH, replay_path_idx)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new(COL_LOG_VERSION, log_version_idx)),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            // Add beats Remove within the same path/version (DV update pattern).
            PhysicalSortExpr {
                expr: Arc::new(Column::new(COL_LOG_IS_REMOVE, is_remove_idx)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ])
        .ok_or_else(|| {
            DataFusionError::Internal("failed to create replay ordering requirement".to_string())
        })?;
        Ok(Arc::new(
            SortExec::new(ordering, plan).with_preserve_partitioning(true),
        ))
    };

    let replay_strategy = ctx.options().delta_log_replay_strategy;
    let replay_hash_threshold = ctx.options().delta_log_replay_hash_threshold.max(1);
    let has_checkpoint = !checkpoint_files.is_empty();
    let hash_no_sort = match replay_strategy {
        DeltaLogReplayStrategyOption::Sort => false,
        DeltaLogReplayStrategyOption::HashNoSort => has_checkpoint,
        DeltaLogReplayStrategyOption::Auto => {
            has_checkpoint && commit_files.len() <= replay_hash_threshold
        }
    };

    let replay: Arc<dyn ExecutionPlan> = if has_checkpoint {
        // Hash replay: stream checkpoint, build small commit-side map, then emit commit-only adds.
        let checkpoint_scan =
            checkpoint_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));
        let commit_scan = commit_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));

        let checkpoint_branch = build_branch(checkpoint_scan, false)?;
        let commit_branch = build_branch(commit_scan, !hash_no_sort)?;

        Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_branch,
            commit_branch,
            table_url,
            version,
            replay_partition_cols,
            checkpoint_files,
            commit_files,
        ))
    } else {
        // Sort replay (spill-friendly): for commit-only scenarios, avoid building a potentially
        // large in-memory map.
        let commit_scan = commit_scan_opt.unwrap_or_else(|| empty_scan(Arc::clone(&input_schema)));
        let commit_branch = build_branch(commit_scan, true)?;

        Arc::new(DeltaLogReplayExec::new(
            commit_branch,
            table_url,
            version,
            replay_partition_cols,
            checkpoint_files,
            commit_files,
        ))
    };

    let replay: Arc<dyn ExecutionPlan> = if let Some(filter) = options.log_filter {
        let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});
        let adapter = adapter_factory.create(filter.table_schema, replay.schema());
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
