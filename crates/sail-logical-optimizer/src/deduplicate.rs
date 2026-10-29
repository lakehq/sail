use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::{Distinct, LogicalPlan, LogicalPlanBuilder};

/// The resolver uses DISTINCT ON to carry recovered DataFrame attributes without
/// changing the keys. When every output is a key, ordinary DISTINCT avoids
/// redundant first_value aggregates and enables metadata-only grouping.
#[derive(Debug, Default)]
pub struct SimplifyDeduplicate(ReplaceDistinctWithAggregate);

impl OptimizerRule for SimplifyDeduplicate {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Distinct(Distinct::On(distinct)) = &plan
            && distinct.sort_expr.is_none()
            && distinct.on_expr == distinct.select_expr
        {
            let input = LogicalPlanBuilder::from(distinct.input.as_ref().clone())
                .project(distinct.select_expr.clone())?
                .distinct()?
                .build()?;
            return self.0.rewrite(input, config);
        }
        self.0.rewrite(plan, config)
    }
}
