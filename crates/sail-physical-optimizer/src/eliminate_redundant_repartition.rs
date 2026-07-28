use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_physical_expr::Partitioning;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

/// `EliminateRedundantRepartition` rule removes redundant repartition nodes from the physical
/// plan, thereby optimizing repartition performance across the plan.
///
/// The rule targets two redundant patterns:
///
/// Pattern 1: `RepartitionExec(RoundRobinBatch)` → `ExplicitRepartitionExec`
///
/// This rule removes a `RepartitionExec` with `Partitioning::RoundRobinBatch` inserted by the
/// `EnforceDistribution` rule that sits directly on top of an `ExplicitRepartitionExec`.
///
/// `EnforceDistribution` inserts a `RepartitionExec` with `Partitioning::RoundRobinBatch` only
/// when the parent node's required input distribution is `Distribution::UnspecifiedDistribution`,
/// purely to increase parallelism up to `target_partitions`. Removing this round-robin
/// repartition does not violate the parent node's distribution requirement, since it is
/// unspecified. It only means the resulting number of partitions is bounded by
/// `ExplicitRepartitionExec`'s own partitioning rather than being raised to
/// `target_partitions`.
///
/// Note: `RepartitionExec` with `Partitioning::Hash(..)` or
/// `Partitioning::UnknownPartitioning` is left untouched, since eliminating it could violate
/// the parent node's distribution requirement.
///
/// To avoid eliminating any unintended `RepartitionExec`, this rule should be applied
/// immediately after the `EnforceDistribution` rule.
///
/// Pattern 2: `ExplicitRepartitionExec` → `ExplicitRepartitionExec`
///
/// This rule collapses nested `ExplicitRepartitionExec` nodes into one, using the outer node's
/// partitioning scheme, since the outer repartition redistributes the data regardless of how
/// the inner one arranged it.
///
/// Exception: an outer `UnknownPartitioning` over an inner `RoundRobin`/`Hash` is preserved.
/// When the outer partition count is less than the inner partition count,
/// `UnknownPartitioning` gets rewritten to `CoalesceExec`/`CoalescePartitionsExec` by the
/// `RewriteExplicitRepartition` rule. Coalesce never shuffles; it only merges partitions down
/// to the outer count. Collapsing here would silently remove the shuffle and turn it into a
/// no-shuffle operation.
///

pub struct EliminateRedundantRepartition {}

impl EliminateRedundantRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for EliminateRedundantRepartition {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for EliminateRedundantRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node: Arc<dyn ExecutionPlan>| {
            // Pattern 1: RepartitionExec(RoundRobinBatch) → ExplicitRepartitionExec
            if let Some(repartition) = node.downcast_ref::<RepartitionExec>() {
                if matches!(
                    repartition.properties().output_partitioning(),
                    Partitioning::RoundRobinBatch(_)
                ) && repartition
                    .input()
                    .downcast_ref::<ExplicitRepartitionExec>()
                    .is_some()
                {
                    return Ok(Transformed::yes(repartition.input().clone()));
                }
                return Ok(Transformed::no(node));
            }

            // Pattern 2: ExplicitRepartitionExec → ExplicitRepartitionExec
            if let Some(outer) = node.downcast_ref::<ExplicitRepartitionExec>() {
                if let Some(inner) = outer.input().downcast_ref::<ExplicitRepartitionExec>() {
                    let outer_p = outer.properties().output_partitioning();
                    let inner_p = inner.properties().output_partitioning();

                    if !matches!(
                        (outer_p, inner_p),
                        (
                            Partitioning::UnknownPartitioning(_),
                            Partitioning::RoundRobinBatch(_),
                        ) | (
                            Partitioning::UnknownPartitioning(_),
                            Partitioning::Hash(_, _)
                        )
                    ) {
                        return Ok(Transformed::yes(Arc::new(ExplicitRepartitionExec::new(
                            inner.input().clone(),
                            outer_p.clone(),
                        ))));
                    }
                }
                return Ok(Transformed::no(node));
            }

            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "EliminateRedundantRepartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for EliminateRedundantRepartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion_physical_expr::Partitioning;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;

    use super::EliminateRedundantRepartition;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        EliminateRedundantRepartition::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    fn test_eliminates_rr_repartition_above_rr_explicit() {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::RoundRobinBatch(3),
        ));
        let redundant: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(explicit, Partitioning::RoundRobinBatch(10)).unwrap(),
        );
        let result = optimize(redundant);

        assert!(result.downcast_ref::<ExplicitRepartitionExec>().is_some());
        assert_eq!(result.output_partitioning().partition_count(), 3);
    }

    #[test]
    fn test_eliminates_rr_repartition_above_hash_explicit() {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::Hash(vec![], 3),
        ));
        let redundant: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(explicit, Partitioning::RoundRobinBatch(10)).unwrap(),
        );
        let result = optimize(redundant);

        assert!(result.downcast_ref::<ExplicitRepartitionExec>().is_some());
        assert_eq!(result.output_partitioning().partition_count(), 3);
    }

    #[test]
    fn test_eliminates_rr_repartition_above_unknown_explicit() {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::UnknownPartitioning(3),
        ));
        let redundant: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(explicit, Partitioning::RoundRobinBatch(10)).unwrap(),
        );
        let result = optimize(redundant);

        assert!(result.downcast_ref::<ExplicitRepartitionExec>().is_some());
        assert_eq!(result.output_partitioning().partition_count(), 3);
    }

    #[test]
    fn test_no_change_when_repartition_is_hash() {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::RoundRobinBatch(3),
        ));
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(explicit, Partitioning::Hash(vec![], 10)).unwrap());
        let result = optimize(plan);

        assert!(result.downcast_ref::<RepartitionExec>().is_some());
    }

    #[test]
    fn test_no_change_when_repartition_is_unknown() {
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::RoundRobinBatch(3),
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(explicit, Partitioning::UnknownPartitioning(10)).unwrap(),
        );
        let result = optimize(plan);

        assert!(result.downcast_ref::<RepartitionExec>().is_some());
    }

    #[test]
    fn test_no_change_when_child_is_not_explicit_repartition() {
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(3)).unwrap(),
        );
        let result = optimize(repartition);

        assert!(result.downcast_ref::<RepartitionExec>().is_some());
    }
}
