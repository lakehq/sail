use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::LogicalPlan;
use sail_logical_plan::partition_filter::PartitionFilterNode;

#[derive(Debug)]
pub struct PartitionAwareFilterPushDown;

impl OptimizerRule for PartitionAwareFilterPushDown {
    fn name(&self) -> &str {
        "push_down_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        PushDownFilter::new().apply_order()
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // DataFusion's extension pushdown guard only checks column references.
        // Nullary stateful functions must also stay above the boundary, and
        // their conjuncts must not be reordered or split by the built-in rule.
        if let LogicalPlan::Filter(filter) = &plan
            && filter.input.exists(|input| {
                Ok(matches!(input, LogicalPlan::Extension(extension)
                    if extension.node.as_any().is::<PartitionFilterNode>()))
            })?
        {
            return Ok(Transformed::no(plan));
        }
        PushDownFilter::new().rewrite(plan, config)
    }
}
