use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{create_physical_sort_exprs, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};

use crate::extension::logical::{
    MapPartitionsNode, RangeNode, SchemaPivotNode, ShowStringNode, SortWithinPartitionsNode,
};
use crate::extension::physical::map_partitions::MapPartitionsExec;
use crate::extension::physical::range::RangeExec;
use crate::extension::physical::schema_pivot::SchemaPivotExec;
use crate::extension::physical::show_string::ShowStringExec;
use crate::utils::ItemTaker;

pub(crate) struct ExtensionPhysicalPlanner {}

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let physical_inputs = physical_inputs.to_vec();
        let plan: Arc<dyn ExecutionPlan> =
            if let Some(node) = node.as_any().downcast_ref::<RangeNode>() {
                Arc::new(RangeExec::new(
                    node.range().clone(),
                    node.num_partitions(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
                Arc::new(ShowStringExec::new(
                    physical_inputs.one()?,
                    node.names().to_vec(),
                    node.limit(),
                    node.format().clone(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
                Arc::new(MapPartitionsExec::new(
                    physical_inputs.one()?,
                    node.udf().clone(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
                let expr = create_physical_sort_exprs(
                    node.sort_expr(),
                    node.schema(),
                    session_state.execution_props(),
                )?;
                let sort = SortExec::new(expr, physical_inputs.one()?)
                    .with_fetch(node.fetch())
                    .with_preserve_partitioning(true);
                Arc::new(sort)
            } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
                Arc::new(SchemaPivotExec::new(
                    physical_inputs.one()?,
                    node.names().to_vec(),
                    node.schema().inner().clone(),
                ))
            } else {
                return internal_err!("Unsupported logical extension node: {:?}", node);
            };
        Ok(Some(plan))
    }
}
