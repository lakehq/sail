use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::internal_err;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_logical_plan::merge::{MergeCardinalityCheckNode, RowLevelWriteNode};
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;
use sail_physical_plan::row_level_write::create_row_level_write_physical_plan;

mod optimizer;

pub use optimizer::{lakehouse_optimizer_rules, ExpandRowLevelOp};
use sail_common_datafusion::datasource::is_lakehouse_format;

#[derive(Debug, Default)]
pub struct DeltaExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DeltaExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !is_lakehouse_format(node.target_format()) {
                return Ok(None);
            }

            let plan = create_row_level_write_physical_plan(session_state, planner, node).await?;
            return Ok(Some(plan));
        }

        if let Some(node) = node.as_any().downcast_ref::<MergeCardinalityCheckNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
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
}
