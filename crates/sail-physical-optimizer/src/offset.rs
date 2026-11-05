use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// DataFusion's sort enforcement treats an offset without fetch as an order-preserving
/// unary node. Changing its input ordering changes the rows it discards. Hide that
/// propagation path during enforcement, while keeping the original ordering required.
#[derive(Debug, Default)]
pub struct OffsetSafeRequirements;

impl PhysicalOptimizerRule for OffsetSafeRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|plan| {
                if plan
                    .downcast_ref::<GlobalLimitExec>()
                    .is_some_and(|limit| limit.skip() > 0 && limit.fetch().is_none())
                {
                    Ok(Transformed::yes(
                        Arc::new(OffsetBoundary(plan)) as Arc<dyn ExecutionPlan>
                    ))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data;
        let plan = EnsureRequirements::new().optimize(plan, config)?;
        Ok(plan
            .transform_up(|plan| {
                if let Some(boundary) = plan.downcast_ref::<OffsetBoundary>() {
                    Ok(Transformed::yes(Arc::clone(&boundary.0)))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data)
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

/// Temporary optimizer node, removed before execution and serialization.
#[derive(Debug)]
struct OffsetBoundary(Arc<dyn ExecutionPlan>);

impl DisplayAs for OffsetBoundary {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OffsetBoundary")
    }
}

impl ExecutionPlan for OffsetBoundary {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            self.0
                .properties()
                .output_ordering()
                .cloned()
                .map(Into::into),
        ]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("OffsetBoundary requires one input");
        }
        Ok(Arc::new(Self(children.remove(0))))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
