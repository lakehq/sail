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

use datafusion::common::{DataFusionError, Result, ToDFSchema};
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::commit::assemble_commit_plan;
use super::context::PlannerContext;
use super::metadata_predicate::{build_metadata_filter, predicate_requires_stats};
use super::utils::{build_log_replay_pipeline_with_options, LogReplayOptions};
use crate::kernel::DeltaOperation;
use crate::physical_plan::{
    DeltaDiscoveryExec, DeltaPhysicalExprAdapterFactory, DeltaScanByAddsExec,
    DeltaWriterExecOptions,
};

pub async fn build_delete_plan(
    ctx: &PlannerContext<'_>,
    condition: ExprWithSource,
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
    let condition_expr = condition.expr.clone();
    let physical_condition = ctx
        .session()
        .create_physical_expr(condition_expr.clone(), &table_df_schema)?;

    // Partition-only predicates can delete entire files without scanning data. In that case,
    // build a visible metadata pipeline over a log-derived meta table.
    let partition_only = !predicate_requires_stats(&condition_expr, &partition_columns);
    let log_replay_options = LogReplayOptions {
        include_stats_json: !partition_only,
        ..Default::default()
    };

    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options).await?;
    let meta_scan: Arc<dyn ExecutionPlan> =
        build_metadata_filter(ctx.session(), meta_scan, snapshot_state, condition_expr)?;

    // Always wrap with DeltaDiscoveryExec so EXPLAIN shows the metadata pipeline.
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.clone(),
        partition_only,
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    // TODO(adaptive-partitioning): Keep this aligned with `scan_planner.rs`.
    // Plan: switch from fixed `target_partitions` + round-robin to size-driven partition count
    // first, then size-aware distribution to avoid oversharding and worker skew.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        version,
        table_schema.clone(),
        table_schema.clone(),
        crate::datasource::DeltaScanConfig::default(),
        None,
        None,
        None,
    ));

    // Adapt the predicate to the scan schema. PhysicalExpr Column indices are schema-dependent,
    // and DeltaScanByAddsExec may reorder/augment the schema compared to the original table schema.
    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory
        .create(table_schema.clone(), scan_exec.schema())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let adapted_condition = adapter
        .rewrite(physical_condition.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let negated_condition = Arc::new(NotExpr::new(adapted_condition));
    let filter_exec: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    let operation = Some(DeltaOperation::Delete {
        predicate: condition.source,
    });

    assemble_commit_plan(
        filter_exec,
        Some(find_files_exec),
        ctx.table_url().clone(),
        DeltaWriterExecOptions::from(ctx.options().clone()),
        ctx.metadata_configuration().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        operation,
    )
}

/// Merge-on-Read DELETE: write deletion vectors instead of rewriting files.
///
/// The plan:
/// 1. Discover affected files via the metadata pipeline (same as CoW)
/// 2. For each affected file, scan to find matching row indices
/// 3. Write DV files with bitmaps of the matching rows
/// 4. Emit Remove(old_add) + Add(path, dv=new_dv) commit actions
pub async fn build_delete_plan_mor(
    ctx: &PlannerContext<'_>,
    condition: ExprWithSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = snapshot_state.version();

    // Verify that deletion vectors are enabled on this table
    snapshot_state
        .verify_deletion_vectors()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table_schema = snapshot_state
        .input_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();
    let table_df_schema = table_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let condition_expr = condition.expr.clone();
    let physical_condition = ctx
        .session()
        .create_physical_expr(condition_expr.clone(), &table_df_schema)?;

    // Partition-only predicates can delete entire files without scanning data.
    // For MoR with partition-only predicates, we still fall back to file removal
    // since there's no benefit in writing DVs for entire files.
    let partition_only = !predicate_requires_stats(&condition_expr, &partition_columns);
    if partition_only {
        // For partition-only predicates, use the CoW path as it's more efficient
        // (just remove whole files).
        return build_delete_plan(ctx, condition).await;
    }

    let log_replay_options = LogReplayOptions {
        include_stats_json: true,
        ..Default::default()
    };

    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options).await?;
    let meta_scan: Arc<dyn ExecutionPlan> =
        build_metadata_filter(ctx.session(), meta_scan, snapshot_state, condition_expr)?;

    // Wrap with DeltaDiscoveryExec for metadata pipeline visibility.
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.clone(),
        false, // not partition_only for MoR
    )?);

    // Spread Add actions across partitions so DeletionVectorWriterExec can process files
    // in parallel — same pattern as the CoW DELETE path with DeltaScanByAddsExec.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let operation = Some(DeltaOperation::Delete {
        predicate: condition.source,
    });

    // DeletionVectorWriterExec handles: scan each file → find matching rows → write DVs → emit actions
    let dv_writer: Arc<dyn ExecutionPlan> =
        Arc::new(crate::physical_plan::DeletionVectorWriterExec::new(
            find_files_exec,
            ctx.table_url().clone(),
            physical_condition,
            table_schema.clone(),
            version,
            operation,
        )?);

    // Wrap in CoalescePartitions → DeltaCommitExec for final commit
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(dv_writer),
    );

    Ok(Arc::new(crate::physical_plan::DeltaCommitExec::new(
        coalesced,
        ctx.table_url().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        sail_common_datafusion::datasource::PhysicalSinkMode::Append,
    )))
}
