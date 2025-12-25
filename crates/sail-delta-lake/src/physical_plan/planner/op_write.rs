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
use sail_common_datafusion::physical_expr::PhysicalExprWithSource;

use super::context::PlannerContext;
use super::utils::{align_schemas_for_union, build_standard_write_layers};
use crate::datasource::schema::DataFusionMixins;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaFindFilesExec,
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
    condition: PhysicalExprWithSource,
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

    let old_data_plan =
        build_old_data_plan(ctx, condition.expr.clone(), version, table_schema.clone()).await?;

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
        None,
    ));

    let find_files_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaFindFilesExec::new(
        ctx.table_url().clone(),
        Some(condition.expr.clone()),
        ctx.table_schema_for_cond(),
        version,
    ));
    let remove_plan = Arc::new(DeltaRemoveActionsExec::new(find_files_plan));

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
) -> Result<Arc<dyn ExecutionPlan>> {
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaFindFilesExec::new(
        ctx.table_url().clone(),
        Some(condition.clone()),
        Some(table_schema.clone()),
        version,
    ));

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        table_schema,
    ));

    let negated_condition = Arc::new(NotExpr::new(condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    Ok(filter_exec)
}
