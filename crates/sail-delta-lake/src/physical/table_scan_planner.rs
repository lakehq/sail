use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::logical::table_source::DeltaTableSource;
use crate::physical::scan_planner::plan_delta_scan;

/// Physical planner for logical Delta table scans.
/// Plans `DeltaTableSource` table scans directly without an intermediate extension node.
pub struct DeltaTablePhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for DeltaTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        _node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
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
