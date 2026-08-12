use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_logical_plan::merge::MergeCardinalityCheckNode;
use sail_logical_plan::row_level::RowLevelWriteNode;
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;

use crate::lake_source::{DeltaWriteNode, plan_delta_write};
use crate::logical::table_source::{DeltaFileSelection, DeltaTableSource};
use crate::physical::scan_planner::{DeltaFileSource, plan_delta_scan};
use crate::physical_plan::planner::create_row_level_write_physical_plan;

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
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
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
            return plan_delta_write(session, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }

        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !node.target_format().eq_ignore_ascii_case("delta") {
                return Ok(None);
            }

            let plan = create_row_level_write_physical_plan(session, planner, node).await?;
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
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
            return Ok(None);
        };

        let snapshot = source.snapshot();
        let log_store = source.log_store();
        let config = source.config();
        let filters = unnormalize_cols(scan.filters.clone());
        let projection = scan.projection.clone();
        let file_source = match (
            snapshot.load_config().require_files,
            source.file_selection(),
        ) {
            (true, DeltaFileSelection::Snapshot) => DeltaFileSource::Eager(snapshot.shared_adds()),
            (true, DeltaFileSelection::Selected(indices)) => {
                let files = indices
                    .iter()
                    .map(|&index| {
                        snapshot.adds().get(index).cloned().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(format!(
                                "Delta file selection index {index} is out of range"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                DeltaFileSource::Eager(Arc::new(files))
            }
            (false, DeltaFileSelection::Snapshot) => DeltaFileSource::Replay,
            (false, DeltaFileSelection::Selected(_)) => {
                return datafusion_common::internal_err!(
                    "Delta file selection cannot be planned from a metadata-only snapshot"
                );
            }
        };
        let plan = plan_delta_scan(
            session,
            snapshot,
            log_store,
            config,
            file_source,
            projection.as_ref(),
            &filters,
            scan.fetch,
        )
        .await?;

        Ok(Some(plan))
    }
}
