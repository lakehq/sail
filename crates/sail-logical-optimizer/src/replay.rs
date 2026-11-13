use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan;
use sail_logical_plan::replay::ReplayNode;

/// Column-based extension policies cannot block predicates with no column refs.
#[derive(Debug)]
pub(crate) struct ReplayAwarePushDownFilter;

impl OptimizerRule for ReplayAwarePushDownFilter {
    fn name(&self) -> &str {
        "push_down_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(filter) = &plan {
            let mut input = filter.input.as_ref();
            // DF recursively merges adjacent filters within rewrite(), bypassing
            // this wrapper. Guard the entire adjacent chain, not only its leaf.
            while let LogicalPlan::Filter(filter) = input {
                input = filter.input.as_ref();
            }
            if let LogicalPlan::Extension(extension) = input
                && (extension.node.as_any().is::<ReplayNode>()
                    || extension
                        .node
                        .as_any()
                        .is::<sail_logical_plan::merge_evaluate::EvaluateOnceNode>())
            {
                return Ok(Transformed::no(plan));
            }
        }
        // The optimizer still visits the replay's producer and optimizes it.
        PushDownFilter::new().rewrite(plan, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::optimizer::OptimizerContext;
    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::{Extension, LogicalPlanBuilder, col, lit};
    use sail_common_datafusion::replay::ReplayId;
    use std::sync::Arc;

    #[test]
    fn replay_filter_barrier_keeps_source_optimization_visible() -> Result<()> {
        let input = LogicalPlanBuilder::values(vec![vec![lit(1_i64)]])?
            .project(vec![col("column1").alias("v")])?
            .filter(col("v").gt(lit(0_i64)))?
            .build()?;
        let id = ReplayId {
            scope: "00000000-0000-0000-0000-000000000001"
                .parse()
                .map_err(|e| datafusion_common::internal_datafusion_err!("{e}"))?,
            ordinal: 0,
        };
        let replay = LogicalPlan::Extension(Extension {
            node: Arc::new(ReplayNode::new(id, Arc::new(input))),
        });
        let plan = LogicalPlanBuilder::from(replay)
            .filter(lit(false))?
            .filter(lit(true))?
            .build()?;
        let optimized = plan
            .transform_down(|plan| {
                ReplayAwarePushDownFilter.rewrite(plan, &OptimizerContext::new())
            })?
            .data;
        let LogicalPlan::Filter(outer) = optimized else {
            panic!("expected outer filter");
        };
        let LogicalPlan::Filter(inner) = outer.input.as_ref() else {
            panic!("expected inner filter");
        };
        let LogicalPlan::Extension(extension) = inner.input.as_ref() else {
            panic!("expected replay boundary");
        };
        let replay = extension
            .node
            .as_any()
            .downcast_ref::<ReplayNode>()
            .ok_or_else(|| datafusion_common::internal_datafusion_err!("expected replay"))?;
        // The source's filter moved through its projection even though consumer
        // filters remained above the replay boundary.
        let LogicalPlan::Projection(projection) = replay.input().as_ref() else {
            panic!("source projection must be retained");
        };
        assert!(matches!(projection.input.as_ref(), LogicalPlan::Filter(_)));
        Ok(())
    }
}
