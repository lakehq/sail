use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use sail_physical_plan::barrier::BarrierExec;

/// Rewrites hash joins to `CollectLeft` when either join side carries
/// a broadcast-hint marker (`BarrierExec` with no preconditions).
#[derive(Debug, Default)]
pub struct RewriteBroadcastHintHashJoin;

impl RewriteBroadcastHintHashJoin {
    pub fn new() -> Self {
        Self
    }
}

fn has_broadcast_hint(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(barrier) = plan.as_any().downcast_ref::<BarrierExec>() {
        if barrier.preconditions().is_empty() {
            return true;
        }
    }
    plan.children().into_iter().any(has_broadcast_hint)
}

impl PhysicalOptimizerRule for RewriteBroadcastHintHashJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            let Some(join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let left_hinted = has_broadcast_hint(&join.left);
            let right_hinted = has_broadcast_hint(&join.right);
            if !left_hinted && !right_hinted {
                return Ok(Transformed::no(node));
            }

            let mode = PartitionMode::CollectLeft;
            if join.mode == mode {
                return Ok(Transformed::no(node));
            }

            let new_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
                join.left.clone(),
                join.right.clone(),
                join.on.clone(),
                join.filter.clone(),
                &join.join_type,
                join.projection.as_ref().map(|x| x.to_vec()),
                mode,
                join.null_equality,
                join.null_aware,
            )?);
            Ok(Transformed::yes(new_join))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteBroadcastHintHashJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
