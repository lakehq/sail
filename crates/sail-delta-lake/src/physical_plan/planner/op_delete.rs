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

use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use super::utils::{adapt_predicate_to_schema, build_touched_file_plan};
use crate::datasource::schema::DataFusionMixins;
use crate::kernel::DeltaOperation;
use crate::physical_plan::{
    DeltaCommitExec, DeltaFileLookupExec, DeltaRemoveActionsExec, DeltaScanByAddsExec,
    DeltaWriterExec,
};

pub async fn build_delete_plan(
    ctx: &PlannerContext<'_>,
    condition: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = snapshot_state.version();

    let table_schema = snapshot_state
        .snapshot()
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();

    let touched_files =
        build_touched_file_plan(ctx, snapshot_state, table.log_store(), condition.clone()).await?;

    // Convert path stream -> Add(JSON) stream (visible in EXPLAIN as a separate node).
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

    // Rewrite the predicate against the actual scan schema so `Column` indices line up.
    let adapted_condition =
        adapt_predicate_to_schema(table_schema.clone(), scan_exec.schema(), condition.clone())?;

    let negated_condition = Arc::new(NotExpr::new(adapted_condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    let operation_override = Some(DeltaOperation::Delete {
        predicate: Some(format!("{}", fmt_sql(condition.as_ref()))),
    });
    let writer_exec = Arc::new(DeltaWriterExec::new(
        filter_exec,
        ctx.table_url().clone(),
        ctx.options().clone(),
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        ctx.table_exists(),
        table_schema.clone(),
        None,
        operation_override,
    ));

    let remove_exec = Arc::new(DeltaRemoveActionsExec::new(lookup_plan));
    let union_exec = UnionExec::try_new(vec![writer_exec, remove_exec])?;

    Ok(Arc::new(DeltaCommitExec::new(
        union_exec,
        ctx.table_url().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        PhysicalSinkMode::Append,
    )))
}
