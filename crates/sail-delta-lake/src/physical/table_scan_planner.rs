use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::internal_err;
use sail_common_datafusion::datasource::{
    create_sort_order, PhysicalSinkInfo, PhysicalSinkMode, SinkMode,
};

use crate::logical::table_source::DeltaTableSource;
use crate::physical::scan_planner::plan_delta_scan;
use crate::{DeltaTableFormat, DeltaWriteNode};

/// Physical planner for logical Delta table scans.
/// Plans `DeltaTableSource` table scans directly without an intermediate extension node.
pub struct DeltaTablePhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for DeltaTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DeltaWriteNode>() else {
            return Ok(None);
        };
        let [logical_input] = logical_inputs else {
            return internal_err!("DeltaWriteNode requires exactly one logical input");
        };
        let [physical_input] = physical_inputs else {
            return internal_err!("DeltaWriteNode requires exactly one physical input");
        };

        let sort_order = create_sort_order(session_state, node.sort_order().to_vec(), logical_input.schema())?;
        let mode = to_physical_sink_mode(node.mode());

        let info = PhysicalSinkInfo {
            input: physical_input.clone(),
            mode,
            partition_by: node.partition_by().to_vec(),
            bucket_by: node.bucket_by().cloned(),
            sort_order,
            options: node.options().to_vec(),
            logical_schema: Some(logical_input.schema().clone()),
        };

        let plan = DeltaTableFormat::create_physical_writer(session_state, info).await?;
        Ok(Some(plan))
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.as_any().downcast_ref::<DeltaTableSource>() else {
            return Ok(None);
        };

        let snapshot = source.snapshot();
        let log_store = source.log_store();
        let config = source.config();
        let filters = unnormalize_cols(scan.filters.clone());
        let projection = scan.projection.clone();
        let files = if !snapshot.load_config().require_files || snapshot.adds().is_empty() {
            None
        } else if snapshot.adds().iter().any(|a| a.deletion_vector.is_some()) {
            // When deletion vectors are present, fall through to the
            // DeltaScanByAddsExec path which applies per-file DV filtering.
            None
        } else {
            Some(Arc::new(snapshot.adds().to_vec()))
        };
        let plan = plan_delta_scan(
            session_state,
            snapshot,
            log_store,
            config,
            files,
            projection.as_ref(),
            &filters,
            scan.fetch,
        )
        .await?;

        Ok(Some(plan))
    }
}

fn to_physical_sink_mode(mode: &SinkMode) -> PhysicalSinkMode {
    match mode.clone() {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => PhysicalSinkMode::OverwriteIf {
            source: condition.source.clone(),
            condition: Some(condition),
        },
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    }
}
