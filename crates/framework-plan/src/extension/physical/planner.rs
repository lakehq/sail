use crate::extension::logical::{RangeNode, ShowStringNode};
use crate::extension::physical::range::RangeExec;
use crate::extension::physical::show_string::ShowStringExec;
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use std::sync::Arc;

pub(crate) struct ExtensionPhysicalPlanner {}

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let node = node;
        let plan: Arc<dyn ExecutionPlan> =
            if let Some(node) = node.as_any().downcast_ref::<RangeNode>() {
                Arc::new(RangeExec::new(
                    node.range.clone(),
                    node.num_partitions,
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
                if physical_inputs.len() != 1 {
                    return internal_err!("ShowStringExec should have one child");
                }
                Arc::new(ShowStringExec::new(
                    physical_inputs[0].clone(),
                    node.limit,
                    node.format.clone(),
                ))
            } else {
                return internal_err!("Unsupported logical extension node: {:?}", node);
            };
        Ok(Some(plan))
    }
}
