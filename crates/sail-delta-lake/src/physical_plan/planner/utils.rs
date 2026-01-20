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
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_functions_nested::extract::array_element_udf;
use datafusion_functions_nested::map_extract::map_extract_udf;
use datafusion_physical_expr::expressions::{Column as PhysicalColumn, IsNotNullExpr};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::context::PlannerContext;
use super::log_scan::build_delta_log_datasource_union;
use crate::datasource::PATH_COLUMN;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaLogReplayExec,
    DeltaWriterExec, COL_LOG_IS_REMOVE, COL_REPLAY_PATH,
};

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
/// `Union(DataSourceExec)` -> `Projection(replay_path)` -> `Repartition(Hash replay_path)` ->
/// `Sort(replay_path, preserve_partitioning)` -> `DeltaLogReplayExec`.
pub async fn build_log_replay_pipeline(
    ctx: &PlannerContext<'_>,
    table_url: Url,
    version: i64,
    partition_columns: Vec<String>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (raw_scan, checkpoint_files, commit_files) =
        build_delta_log_datasource_union(ctx, checkpoint_files, commit_files).await?;

    // Projection#1: build a minimal log scan schema for streaming replay.
    //
    // - keep only `add` payload
    // - replay_path = coalesce(get_field(add, 'path'), get_field(remove, 'path'))
    // - is_remove  = is_not_null(get_field(remove, 'path'))
    let input_schema = raw_scan.schema();
    let add_idx = input_schema.index_of("add")?;
    let remove_idx = input_schema.index_of("remove")?;

    let config_options = Arc::new(ctx.session().config_options().clone());
    let path_lit: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Utf8(Some("path".to_string()))));
    let add_col_in: Arc<dyn PhysicalExpr> = Arc::new(Column::new("add", add_idx));
    let remove_col_in: Arc<dyn PhysicalExpr> = Arc::new(Column::new("remove", remove_idx));

    let add_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::get_field(),
        vec![Arc::clone(&add_col_in), Arc::clone(&path_lit)],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);
    let remove_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::get_field(),
        vec![Arc::clone(&remove_col_in), Arc::clone(&path_lit)],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);
    let replay_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::coalesce(),
        vec![add_path, remove_path.clone()],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);
    // NOTE: `get_field(struct, 'child')` does not apply the parent struct's
    // null buffer to the returned child array. Using `is_not_null(get_field(remove,'path'))`
    // can therefore produce spurious TRUE values on rows where `remove` is NULL.
    //
    // Instead, mark tombstones using the struct's own validity.
    let is_remove: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(remove_col_in));

    let projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (
            Arc::new(Column::new("add", add_idx)) as Arc<dyn PhysicalExpr>,
            "add".to_string(),
        ),
        (replay_path, COL_REPLAY_PATH.to_string()),
        (is_remove, COL_LOG_IS_REMOVE.to_string()),
    ];

    let log_scan: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(projection_exprs, raw_scan)?);

    let log_partitions = ctx.session().config().target_partitions().max(1);
    let replay_path_idx = log_scan.schema().index_of(COL_REPLAY_PATH)?;

    // Hash partition by replay_path so all actions for the same path are co-located.
    let replay_expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> =
        Arc::new(PhysicalColumn::new(COL_REPLAY_PATH, replay_path_idx));
    let log_scan: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        log_scan,
        Partitioning::Hash(vec![replay_expr], log_partitions),
    )?);

    // Ensure per-partition ordering on replay_path so DeltaLogReplayExec can stream without
    // materializing the full active set in memory. SortExec can spill.
    let ordering = LexOrdering::new(vec![PhysicalSortExpr {
        expr: Arc::new(Column::new(COL_REPLAY_PATH, replay_path_idx)),
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    }])
    .ok_or_else(|| {
        DataFusionError::Internal("failed to create replay_path ordering requirement".to_string())
    })?;
    let log_scan: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering, log_scan).with_preserve_partitioning(true));

    let replay: Arc<dyn ExecutionPlan> = Arc::new(DeltaLogReplayExec::new(
        log_scan,
        table_url,
        version,
        partition_columns.clone(),
        checkpoint_files,
        commit_files,
    ));

    // Projection#2: extract a stable "metadata table" schema from `add`.
    let replay_schema = replay.schema();
    let add_idx = replay_schema.index_of("add")?;
    let add_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("add", add_idx));

    let add_field = replay_schema.field_with_name("add")?;
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

    let lit_str = |s: &str| -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Utf8(Some(s.to_string()))))
    };
    let lit_i64 =
        |v: i64| -> Arc<dyn PhysicalExpr> { Arc::new(Literal::new(ScalarValue::Int64(Some(v)))) };

    let get_add_field = |field_name: &str| -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            datafusion::functions::core::get_field(),
            vec![Arc::clone(&add_col), lit_str(field_name)],
            replay_schema.as_ref(),
            Arc::clone(&config_options),
        )?))
    };

    let path_expr: Arc<dyn PhysicalExpr> =
        Arc::new(CastExpr::new(get_add_field("path")?, DataType::Utf8, None));

    let size_expr_i64: Arc<dyn PhysicalExpr> =
        Arc::new(CastExpr::new(get_add_field("size")?, DataType::Int64, None));
    let size_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::coalesce(),
        vec![size_expr_i64, lit_i64(0)],
        replay_schema.as_ref(),
        Arc::clone(&config_options),
    )?);

    let mod_time_expr_i64: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        get_add_field(mod_time_field)?,
        DataType::Int64,
        None,
    ));
    let mod_time_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::coalesce(),
        vec![mod_time_expr_i64, lit_i64(0)],
        replay_schema.as_ref(),
        Arc::clone(&config_options),
    )?);

    let stats_expr: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        get_add_field(stats_field)?,
        DataType::Utf8,
        None,
    ));

    let part_values = get_add_field(part_values_field)?;
    let part_expr_for = |key: &str| -> Result<Arc<dyn PhysicalExpr>> {
        let extracted: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
            map_extract_udf(),
            vec![Arc::clone(&part_values), lit_str(key)],
            replay_schema.as_ref(),
            Arc::clone(&config_options),
        )?);
        let elem: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
            array_element_udf(),
            vec![extracted, lit_i64(1)],
            replay_schema.as_ref(),
            Arc::clone(&config_options),
        )?);
        Ok(Arc::new(CastExpr::new(elem, DataType::Utf8, None)))
    };

    let mut meta_proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(4 + partition_columns.len());
    meta_proj_exprs.push((path_expr, PATH_COLUMN.to_string()));
    meta_proj_exprs.push((size_expr, "size_bytes".to_string()));
    meta_proj_exprs.push((mod_time_expr, "modification_time".to_string()));
    for col in &partition_columns {
        meta_proj_exprs.push((part_expr_for(col)?, col.clone()));
    }
    meta_proj_exprs.push((stats_expr, "stats_json".to_string()));

    Ok(Arc::new(ProjectionExec::try_new(meta_proj_exprs, replay)?))
}
