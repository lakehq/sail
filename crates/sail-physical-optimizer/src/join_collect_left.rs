use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Adds a `CoalescePartitionsExec` on the build (left) side of `CollectLeft`
/// `HashJoinExec` nodes when that side is not already single-partition.
///
/// `CollectLeft` mode requires the build side to be `SinglePartition`, but
/// DataFusion's `EnforceDistribution` optimizer may fail to enforce this
/// (e.g., when a `FinalPartitioned` aggregate feeds into the build side).
/// This rule inserts the missing coalesce to satisfy the requirement while
/// preserving `CollectLeft` semantics (which keeps probe-side ordering
/// intact, unlike converting to `Partitioned` mode).
pub struct RewriteCollectLeftHashJoin {}

impl RewriteCollectLeftHashJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RewriteCollectLeftHashJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for RewriteCollectLeftHashJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|plan| {
            let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            if join.mode != PartitionMode::CollectLeft {
                return Ok(Transformed::no(plan));
            }

            let left_partitions = join.left.output_partitioning().partition_count();

            // Only rewrite when the build side violates the SinglePartition requirement.
            if left_partitions <= 1 {
                return Ok(Transformed::no(plan));
            }

            // Wrap the build side in CoalescePartitionsExec to satisfy
            // the SinglePartition requirement for CollectLeft mode.
            let coalesced_left: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(Arc::clone(&join.left)));

            Ok(Transformed::yes(Arc::new(HashJoinExec::try_new(
                coalesced_left,
                Arc::clone(&join.right),
                join.on.clone(),
                join.filter.clone(),
                &join.join_type,
                join.projection.clone(),
                PartitionMode::CollectLeft,
                join.null_equality,
            )?)))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteCollectLeftHashJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for RewriteCollectLeftHashJoin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
