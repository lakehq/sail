use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// A sort above LIMIT must sort the selected rows, without changing their selection.
/// DataFusion 55's requirement enforcement can push that sort below an unordered
/// limit, also propagating `fetch` without `skip`. Conditional UNION projection
/// pushdown exposes this shape, so protect limits during that optimizer pass.
#[derive(Debug, Default)]
pub struct LimitSafeRequirements;

impl PhysicalOptimizerRule for LimitSafeRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|plan| {
                if plan.is::<GlobalLimitExec>() || plan.is::<LocalLimitExec>() {
                    Ok(Transformed::yes(Arc::new(LimitSortBoundary(plan)) as _))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data;
        let plan = EnsureRequirements::new().optimize(plan, config)?;
        plan.transform_up(|plan| {
            if let Some(boundary) = plan.downcast_ref::<LimitSortBoundary>() {
                Ok(Transformed::yes(Arc::clone(&boundary.0)))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Temporary optimizer node: retain known output ordering, but do not advertise
/// that new sort requirements may be moved to the input. Removed before execution.
#[derive(Debug)]
struct LimitSortBoundary(Arc<dyn ExecutionPlan>);

impl DisplayAs for LimitSortBoundary {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LimitSortBoundary")
    }
}

impl ExecutionPlan for LimitSortBoundary {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // The boundary changes no rows. Keep the limit's cardinality visible so
        // requirement enforcement does not repartition a known-small input.
        Ok(Arc::clone(&input_stats[0]))
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children.try_into().map_err(|children: Vec<_>| {
            datafusion::common::internal_datafusion_err!(
                "LimitSortBoundary expects one child, got {}",
                children.len()
            )
        })?;
        Ok(Arc::new(Self(input)))
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        internal_err!("LimitSortBoundary must be removed before execution")
    }
}
