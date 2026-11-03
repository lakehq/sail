use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use super::utils::{LogReplayOptions, build_log_replay_pipeline_with_options};
use crate::physical_plan::{
    DeltaCommitContext, DeltaCommitExec, DeltaRemoveActionsExec, ExecCommitMeta, encode_actions,
};
use crate::spec::DeltaOperation;
use crate::transaction::PROTOCOL;

/// TRUNCATE removes active Add actions without reading data or producing CDF files.
pub(super) async fn build_truncate_plan(
    ctx: &PlannerContext<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    // Validate even empty tables, for which the commit node intentionally skips a commit.
    PROTOCOL
        .can_write_to_protocol(snapshot.protocol())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    PROTOCOL
        .check_append_only(snapshot)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let metadata = build_log_replay_pipeline_with_options(
        ctx,
        snapshot,
        LogReplayOptions {
            include_stats_json: true,
            include_extended_add_metadata: true,
            ..Default::default()
        },
    )
    .await?;
    let removals: Arc<dyn ExecutionPlan> = Arc::new(DeltaRemoveActionsExec::try_new(
        Arc::new(CoalescePartitionsExec::new(metadata)),
        Some(snapshot.physical_partition_columns()),
    )?);
    // Keep explicit command intent in the existing action stream (and its codec).
    let batch = encode_actions(
        vec![],
        Some(ExecCommitMeta {
            operation: Some(DeltaOperation::Truncate {}),
            ..Default::default()
        }),
    )?;
    let intent = MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
    let input = UnionExec::try_new(vec![intent, removals])?;
    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(input)),
        ctx.table_url().clone(),
        snapshot.metadata().partition_columns().clone(),
        true,
        snapshot
            .input_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
        PhysicalSinkMode::Append,
        ctx.options().user_metadata.clone(),
        DeltaCommitContext::from_snapshot(snapshot),
        ctx.lakehouse_table().cloned(),
    )))
}
