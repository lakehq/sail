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
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_expr::{ Expr, col, lit, when};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_common::ToDFSchema;
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use sail_common_datafusion::datasource::{OPERATION_COLUMN, RowLevelOperationType};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;

use crate::physical_plan::planner::commit::assemble_commit_plan;
use crate::physical_plan::{DeltaDiscoveryExec, DeltaScanByAddsExec, DeltaWriterExecOptions, prepare_delta_write_context};
use crate::spec::DeltaOperation;

use super::context::PlannerContext;
use super::metadata_predicate::build_metadata_filter;
use super::utils::{LogReplayOptions, build_log_replay_pipeline_with_options};

pub async fn build_update_plan(
    ctx: &PlannerContext<'_>,
    condition: Option<ExprWithSource>,
    assignments: Vec<(String, Expr)>
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = snapshot_state.version();

    let table_schema = snapshot_state
        .input_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();
    let table_df_schema = table_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let log_replay_options = LogReplayOptions {
        include_stats_json: true,
        ..Default::default()
    };

    // File discovery: metadata pipeline for writer
    let meta_scan_w: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options.clone())
        .await?;
    let meta_scan_w: Arc<dyn ExecutionPlan> = if let Some(cond) = &condition {
        build_metadata_filter(
            ctx.session(),
            meta_scan_w,
            snapshot_state,
            cond.expr.clone()
        )?
    } else {
        meta_scan_w
    };
    let find_files_writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan_w,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.clone(),
        false
    )?);

    // File discovery: metadata pipeline for remover
    let meta_scan_r: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options)
        .await?;
    let meta_scan_r: Arc<dyn ExecutionPlan> = if let Some(cond) = &condition {
        build_metadata_filter(
            ctx.session(),
            meta_scan_r,
            snapshot_state,
            cond.expr.clone()
        )?
    } else {
        meta_scan_r
    };
    let find_files_remove: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan_r,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.clone(),
        false
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    // TODO(adaptive-partitioning): Keep this aligned with `scan_planner.rs`.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_writer: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_writer,
        Partitioning::RoundRobinBatch(target_partitions)
    )?);
    let find_files_remove: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_remove,
        Partitioning::RoundRobinBatch(target_partitions)
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        find_files_writer,
        ctx.table_url().clone(),
        version,
        table_schema.clone(),
        table_schema.clone(),
        crate::datasource::DeltaScanConfig::default(),
        None,
        None,
        None,
        ctx.lakehouse_table().cloned(),
        snapshot_state.load_config().catalog_managed_commits.clone(),
    ));
    let scan_schema = scan_exec.schema();
    let scan_df_schema = scan_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Adapt physical expressions from table schema to scan schema. PhysicalExpr Column indices are schema-dependent,
    // and DeltaScanByAddsExec may reorder/augment the schema compared to the original table schema.
    let adapter_factory = Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
    let adapter = adapter_factory
        .create(table_schema.clone(), scan_schema.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Build the projection expressions:
    // - Assigned columns (with condition): CASE WHEN condition THEN new_value ELSE old_value END
    // - Assigned columns (no condition): new_value
    // - Unassigned columns: Passed through unchanged
    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(scan_schema.fields().len());
    for field in scan_schema.fields() {
        let col_name = field.name().clone();
        if let Some((_, new_value)) = assignments.iter().find(|(c, _)| c == &col_name) {
            let physical_expr = if let Some(cond) = &condition {
                let case_expr = when(cond.expr.clone(), new_value.clone())
                    .otherwise(col(col_name.clone()))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                ctx
                    .session()
                    .create_physical_expr(case_expr, &table_df_schema)?
            } else {
                ctx
                    .session()
                    .create_physical_expr(new_value.clone(), &table_df_schema)?
            };
            let adapted_expr = adapter
                .rewrite(physical_expr)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            projection_exprs.push((adapted_expr, col_name));
        } else {
            let col_expr = col(col_name.clone());
            let physical_expr = ctx
                .session()
                .create_physical_expr(col_expr, &scan_df_schema)?;
            projection_exprs.push((physical_expr, col_name))
        }
    }

    // Add OPERATION_COLUMN for per-row tracking to determine whether a row is updated or copied
    let op_tag_expr = if let Some(cond) = &condition{
        let tag_expr = when(cond.expr.clone(), lit(RowLevelOperationType::Update.as_i32()))
        .otherwise(lit(RowLevelOperationType::Copy.as_i32()))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let physical_expr = ctx
            .session()
            .create_physical_expr(tag_expr, &table_df_schema)?;
        adapter
            .rewrite(physical_expr)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    } else {
        ctx
            .session()
            .create_physical_expr(lit(RowLevelOperationType::Update.as_i32()), &scan_df_schema)?
    };
    projection_exprs.push((op_tag_expr, OPERATION_COLUMN.to_string()));

    let projection_exec: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(projection_exprs, scan_exec)?);

    let operation = Some(DeltaOperation::Update {
         predicate: condition.and_then(|c| c.source)
    });
    let writer_options = DeltaWriterExecOptions::from(ctx.options().clone());
    let write_context = prepare_delta_write_context(
        ctx.table_url(),
        Some(snapshot_state.as_ref()),
        &writer_options,
        ctx.metadata_configuration(),
        &partition_columns,
        &sail_common_datafusion::datasource::PhysicalSinkMode::Append,
        ctx.table_exists(),
        &projection_exec.schema(),
        operation
    )?;

    assemble_commit_plan(
        projection_exec,
        Some(find_files_remove),
        Some(snapshot_state.physical_partition_columns()),
        ctx.table_url().clone(),
        writer_options,
        ctx.metadata_configuration().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        ctx.options().user_metadata.clone(),
        write_context,
        ctx.lakehouse_table().cloned()
    )
}
