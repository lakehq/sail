use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::logical::DeltaTableScanNode;
use crate::physical::scan_planner::plan_delta_scan;

/// Physical planner for logical Delta table scans (`DeltaTableSource` rewritten into an extension
/// node). This avoids relying on `TableProvider::scan` during logical planning.
pub struct DeltaTablePhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for DeltaTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DeltaTableScanNode>() else {
            return Ok(None);
        };

        let handle = node.handle().inner();
        let filters = unnormalize_cols(node.filters().clone());
        let projection = node.projection().map(|p| p.to_vec());
        let files =
            if !handle.snapshot.load_config().require_files || handle.snapshot.adds().is_empty() {
                None
            } else if handle
                .snapshot
                .adds()
                .iter()
                .any(|a| a.deletion_vector.is_some())
            {
                // When deletion vectors are present, fall through to the
                // DeltaScanByAddsExec path which applies per-file DV filtering.
                None
            } else {
                Some(Arc::new(handle.snapshot.adds().to_vec()))
            };
        let plan = plan_delta_scan(
            session_state,
            &handle.snapshot,
            &handle.log_store,
            &handle.config,
            files,
            projection.as_ref(),
            &filters,
            node.fetch(),
        )
        .await?;

        Ok(Some(plan))
    }
}
