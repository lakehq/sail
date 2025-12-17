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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use super::utils::{
    adapt_predicate_to_schema, align_schemas_for_union, build_standard_write_layers,
    build_touched_file_plan,
};
use crate::datasource::schema::DataFusionMixins;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaFileLookupExec,
    DeltaRemoveActionsExec, DeltaScanByAddsExec, DeltaWriterExec,
};

pub async fn build_write_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match sink_mode.clone() {
        PhysicalSinkMode::OverwriteIf { condition } => {
            build_overwrite_if_plan(ctx, input, condition, sort_order).await
        }
        _ => build_standard_plan(ctx, input, sink_mode, sort_order),
    }
}

fn build_standard_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    build_standard_write_layers(ctx, input, &sink_mode, sort_order, input_schema)
}

async fn build_overwrite_if_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    condition: Arc<dyn PhysicalExpr>,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let version = snapshot_state.version();
    let table_schema = snapshot_state
        .snapshot()
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let old_data_plan = build_old_data_plan(
        ctx,
        condition.clone(),
        version,
        table_schema.clone(),
        &snapshot_state,
        table.log_store(),
    )
    .await?;

    let new_plan = create_projection(Arc::clone(&input), ctx.partition_columns().to_vec())
        .and_then(|plan| create_repartition(plan, ctx.partition_columns().to_vec()))
        .and_then(|plan| create_sort(plan, ctx.partition_columns().to_vec(), sort_order))?;

    let (aligned_new, aligned_old) = align_schemas_for_union(new_plan, old_data_plan)?;
    let union_plan = UnionExec::try_new(vec![aligned_new, aligned_old])?;

    let input_schema = input.schema();
    let writer = Arc::new(DeltaWriterExec::new(
        Arc::clone(&union_plan),
        ctx.table_url().clone(),
        ctx.options().clone(),
        ctx.partition_columns().to_vec(),
        PhysicalSinkMode::OverwriteIf {
            condition: condition.clone(),
        },
        ctx.table_exists(),
        union_plan.schema(),
        Some(condition.clone()),
        None,
    ));

    let touched_files =
        build_touched_file_plan(ctx, &snapshot_state, table.log_store(), condition.clone()).await?;
    let lookup_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaFileLookupExec::new(
        touched_files,
        ctx.table_url().clone(),
        version,
    ));
    let remove_plan = Arc::new(DeltaRemoveActionsExec::new(lookup_plan));

    let union_actions = UnionExec::try_new(vec![writer, remove_plan])?;

    Ok(Arc::new(DeltaCommitExec::new(
        union_actions,
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        input_schema,
        PhysicalSinkMode::OverwriteIf { condition },
    )))
}

async fn build_old_data_plan(
    ctx: &PlannerContext<'_>,
    condition: Arc<dyn PhysicalExpr>,
    version: i64,
    table_schema: SchemaRef,
    snapshot_state: &crate::table::DeltaTableState,
    log_store: crate::storage::LogStoreRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let touched_files =
        build_touched_file_plan(ctx, snapshot_state, log_store, condition.clone()).await?;
    let lookup_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaFileLookupExec::new(
        touched_files,
        ctx.table_url().clone(),
        version,
    ));

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&lookup_plan),
        ctx.table_url().clone(),
        table_schema.clone(),
    ));

    // Rewrite predicate against the actual scan output schema so `Column` indices line up.
    let adapted_condition = adapt_predicate_to_schema(table_schema, scan_exec.schema(), condition)?;

    let negated_condition = Arc::new(NotExpr::new(adapted_condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    Ok(filter_exec)
}
