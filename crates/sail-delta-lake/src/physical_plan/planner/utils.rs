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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::context::PlannerContext;
use super::log_scan::build_delta_log_datasource_union;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaLogReplayExec,
    DeltaWriterExec, COL_REPLAY_PATH,
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

    // Append a derived `replay_path` column using standard expressions:
    // replay_path = coalesce(get_field(add, 'path'), get_field(remove, 'path'))
    let input_schema = raw_scan.schema();
    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(input_schema.fields().len() + 1);
    for (i, field) in input_schema.fields().iter().enumerate() {
        projection_exprs.push((
            Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }

    let add_idx = input_schema.index_of("add")?;
    let remove_idx = input_schema.index_of("remove")?;

    let path_lit: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Utf8(Some("path".to_string()))));
    let add_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("add", add_idx));
    let remove_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("remove", remove_idx));

    let config_options = Arc::new(ctx.session().config_options().clone());

    let add_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::get_field(),
        vec![add_col, Arc::clone(&path_lit)],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);
    let remove_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::get_field(),
        vec![remove_col, Arc::clone(&path_lit)],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);

    let replay_path: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::core::coalesce(),
        vec![add_path, remove_path],
        input_schema.as_ref(),
        Arc::clone(&config_options),
    )?);
    projection_exprs.push((replay_path, COL_REPLAY_PATH.to_string()));

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
    .expect("non-degenerate ordering");
    let log_scan: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering, log_scan).with_preserve_partitioning(true));

    Ok(Arc::new(DeltaLogReplayExec::new(
        log_scan,
        table_url,
        version,
        partition_columns,
        checkpoint_files,
        commit_files,
    )))
}
