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

/// A physical optimizer rule that ensures all precondition children of a [`BarrierExec`]
/// have the same partition count as the actual (last) child.
///
/// When a [`CatalogCommandExec`](sail_physical_plan::catalog_command::CatalogCommandExec)
/// precondition produces a single partition but the actual write plan has multiple partitions,
/// this rule wraps each mismatched precondition with either:
/// - A `RepartitionExec` (round-robin) to fan out from 1 → N partitions, or
/// - A `CoalescePartitionsExec` to gather from M → 1 partitions.
///
/// No wrapping is applied when both the precondition and the actual plan have exactly one
/// partition, since exhausting the single precondition partition is already sufficient.
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
            let preconditions = barrier.preconditions();

            // Check whether any precondition needs adjustment.
            let needs_adjustment = preconditions
                .iter()
                .any(|pre| pre.output_partitioning().partition_count() != target_partitions);

            if !needs_adjustment {
                return Ok(Transformed::no(node));
            }

            // Wrap mismatched preconditions.
            let new_preconditions: Vec<Arc<dyn ExecutionPlan>> = preconditions
                .iter()
                .map(|pre| {
                    let pre_partitions = pre.output_partitioning().partition_count();
                    if pre_partitions == target_partitions {
                        return Ok(pre.clone());
                    }
                    if target_partitions == 1 {
                        // Coalesce to a single partition.
                        Ok(Arc::new(CoalescePartitionsExec::new(pre.clone()))
                            as Arc<dyn ExecutionPlan>)
                    } else {
                        // Fan out to the target partition count using round-robin.
                        Ok(Arc::new(RepartitionExec::try_new(
                            pre.clone(),
                            Partitioning::RoundRobinBatch(target_partitions),
                        )?) as Arc<dyn ExecutionPlan>)
                    }
                })
                .collect::<Result<_>>()?;

            let new_barrier = BarrierExec::new(new_preconditions, plan.clone());
            Ok(Transformed::yes(
                Arc::new(new_barrier) as Arc<dyn ExecutionPlan>
            ))
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
