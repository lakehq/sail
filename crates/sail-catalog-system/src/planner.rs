use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::logical_plan::SystemTableNode;
use crate::physical_plan::SystemTableExec;

pub struct SystemTablePhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for SystemTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<SystemTableNode>() {
            Ok(Some(Arc::new(SystemTableExec::try_new(
                node.table(),
                node.projection().map(|p| p.to_vec()),
                node.filters()
                    .iter()
                    .map(|e| planner.create_physical_expr(e, node.original_schema(), session_state))
                    .collect::<Result<Vec<_>>>()?,
                node.fetch(),
            )?)))
        } else {
            Ok(None)
        }
    }
}
