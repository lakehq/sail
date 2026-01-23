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
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::physical_expr::PhysicalExprWithSource;

use super::context::PlannerContext;
use super::utils::{build_log_replay_pipeline_with_options, LogReplayFilter, LogReplayOptions};
use crate::datasource::schema::DataFusionMixins;
use crate::datasource::PredicateProperties;
use crate::kernel::DeltaOperation;
use crate::physical_plan::{
    DeltaCommitExec, DeltaDiscoveryExec, DeltaRemoveActionsExec, DeltaScanByAddsExec,
    DeltaWriterExec,
};

pub async fn build_delete_plan(
    ctx: &PlannerContext<'_>,
    condition: PhysicalExprWithSource,
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

    // Partition-only predicates can delete entire files without scanning data. In that case,
    // build a visible metadata pipeline over a log-derived meta table.
    let mut expr_props = PredicateProperties::new(partition_columns.clone());
    expr_props
        .analyze_predicate(&condition.expr)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let kernel_snapshot = snapshot_state.snapshot().snapshot().inner.clone();
    let log_segment = kernel_snapshot.log_segment();
    let checkpoint_files = log_segment
        .checkpoint_parts
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();
    let commit_files = log_segment
        .ascending_commit_files
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();

    // Build a visible metadata pipeline over the Delta log.
    let mut log_replay_options = LogReplayOptions::default();
    if expr_props.partition_only {
        log_replay_options.log_filter = Some(LogReplayFilter {
            predicate: condition.expr.clone(),
            table_schema: table_schema.clone(),
        });
    }

    let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline_with_options(
        ctx,
        ctx.table_url().clone(),
        version,
        partition_columns.clone(),
        checkpoint_files,
        commit_files,
        log_replay_options,
    )
    .await?;

    // Always wrap with DeltaDiscoveryExec so EXPLAIN shows the metadata pipeline.
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        Some(condition.expr.clone()),
        Some(table_schema.clone()),
        version,
        partition_columns.clone(),
        expr_props.partition_only,
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        table_schema.clone(),
    ));

    // Adapt the predicate to the scan schema. PhysicalExpr Column indices are schema-dependent,
    // and DeltaScanByAddsExec may reorder/augment the schema compared to the original table schema.
    let adapter_factory = Arc::new(crate::physical_plan::DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory.create(table_schema.clone(), scan_exec.schema());
    let adapted_condition = adapter
        .rewrite(condition.expr.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let negated_condition = Arc::new(NotExpr::new(adapted_condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    let operation_override = Some(DeltaOperation::Delete {
        predicate: condition.source,
    });
    let writer_exec = Arc::new(DeltaWriterExec::new(
        filter_exec,
        ctx.table_url().clone(),
        ctx.options().clone(),
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        ctx.table_exists(),
        table_schema.clone(),
        operation_override,
    )?);

    let remove_exec = Arc::new(DeltaRemoveActionsExec::new(find_files_exec)?);
    let union_exec = UnionExec::try_new(vec![writer_exec, remove_exec])?;

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(union_exec)),
        ctx.table_url().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        PhysicalSinkMode::Append,
    )))
}
