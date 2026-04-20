use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_physical_expr::Partitioning;
use sail_physical_plan::barrier::BarrierExec;

/// A physical optimizer rule that wraps all precondition children of a [`BarrierExec`]
/// with `RepartitionExec` (round-robin) or `CoalescePartitionsExec` to match the partition
/// count of the actual plan.
///
/// By wrapping preconditions this way, the actual plan will not start until all partitions of
/// the preconditions are completed, even if we only call `execute()` for one partition.
/// Such wrapping can be skipped if the precondition and the actual plan both have only one
/// partition, since a single precondition partition is sufficient to block the actual plan.
pub struct EnforceBarrierPartitioning {}

impl EnforceBarrierPartitioning {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for EnforceBarrierPartitioning {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for EnforceBarrierPartitioning {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node: Arc<dyn ExecutionPlan>| {
            let Some(barrier) = node.as_any().downcast_ref::<BarrierExec>() else {
                return Ok(Transformed::no(node));
            };

            let plan = barrier.plan();
            let target_partitions = plan.output_partitioning().partition_count();

            let preconditions: Vec<Arc<dyn ExecutionPlan>> = barrier
                .preconditions()
                .iter()
                .map(|precondition| {
                    let precondition_partitions =
                        precondition.output_partitioning().partition_count();
                    // Skip wrapping if both the precondition and the actual plan have only one
                    // partition, since a single precondition partition is sufficient.
                    if precondition_partitions == 1 && target_partitions == 1 {
                        return Ok(precondition.clone());
                    }
                    if target_partitions == 1 {
                        // Coalesce to a single partition.
                        Ok(Arc::new(CoalescePartitionsExec::new(precondition.clone()))
                            as Arc<dyn ExecutionPlan>)
                    } else {
                        // Fan out to the target partition count using round-robin.
                        Ok(Arc::new(RepartitionExec::try_new(
                            precondition.clone(),
                            Partitioning::RoundRobinBatch(target_partitions),
                        )?) as Arc<dyn ExecutionPlan>)
                    }
                })
                .collect::<Result<_>>()?;

            let barrier = BarrierExec::new(preconditions, plan.clone());
            Ok(Transformed::yes(Arc::new(barrier) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "EnforceBarrierPartitioning"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for EnforceBarrierPartitioning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
