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
use sail_physical_plan::repartition::{
    ExplicitRepartitionExec, ExplicitRepartitionKind, NarrowCoalesceExec, RoundRobinRepartitionExec,
};

pub struct RewriteExplicitRepartition {}

/// Rewrites the explicit repartition node as [`RepartitionExec`].
impl RewriteExplicitRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RewriteExplicitRepartition {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for RewriteExplicitRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|plan| {
            if let Some(node) = plan.as_any().downcast_ref::<ExplicitRepartitionExec>() {
                let partitioning = node.properties().output_partitioning().clone();
                match node.kind() {
                    ExplicitRepartitionKind::RoundRobin => Ok(Transformed::yes(Arc::new(
                        RoundRobinRepartitionExec::try_new(
                            node.input().clone(),
                            partitioning.partition_count(),
                        )?,
                    ))),
                    ExplicitRepartitionKind::Hash => Ok(Transformed::yes(Arc::new(
                        RepartitionExec::try_new(node.input().clone(), partitioning)?,
                    ))),
                    ExplicitRepartitionKind::Coalesce => {
                        let target_partitions = partitioning.partition_count();
                        let input_partitions = node.input().output_partitioning().partition_count();
                        if target_partitions >= input_partitions {
                            Ok(Transformed::yes(node.input().clone()))
                        } else if matches!(partitioning, Partitioning::UnknownPartitioning(1)) {
                            Ok(Transformed::yes(Arc::new(CoalescePartitionsExec::new(
                                node.input().clone(),
                            ))))
                        } else {
                            Ok(Transformed::yes(Arc::new(NarrowCoalesceExec::try_new(
                                node.input().clone(),
                                target_partitions,
                            )?)))
                        }
                    }
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteExplicitRepartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for RewriteExplicitRepartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
