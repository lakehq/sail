use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_common_datafusion::datasource::is_lakehouse_format;
use sail_logical_plan::merge::{MergeCardinalityCheckNode, RowLevelWriteNode};
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
use sail_physical_plan::row_level_write::create_row_level_write_physical_plan;

use crate::logical::table_source::DeltaTableSource;
use crate::physical::scan_planner::plan_delta_scan;
use crate::table_format::{plan_delta_write, DeltaWriteNode};

/// Physical planner for logical Delta table scans.
/// Plans `DeltaTableSource` table scans directly without an intermediate extension node.
pub struct DeltaPhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for DeltaPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<DeltaWriteNode>() {
            let [logical_input] = logical_inputs else {
                return datafusion_common::internal_err!(
                    "DeltaWriteNode requires exactly one logical input"
                );
            };
            let [physical_input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "DeltaWriteNode requires exactly one physical input"
                );
            };
            return plan_delta_write(session_state, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }

        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !is_lakehouse_format(node.target_format()) {
                return Ok(None);
            }

            let plan = create_row_level_write_physical_plan(session_state, planner, node).await?;
            return Ok(Some(plan));
        }

        if let Some(node) = node.as_any().downcast_ref::<MergeCardinalityCheckNode>() {
            let [input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "MergeCardinalityCheckNode requires exactly one physical input"
                );
            };
            let exec = MergeCardinalityCheckExec::new(
                input.clone(),
                node.target_row_id_col(),
                node.target_present_col(),
                node.source_present_col(),
            )?;
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
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
