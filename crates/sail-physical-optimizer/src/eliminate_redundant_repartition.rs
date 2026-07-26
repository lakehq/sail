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

/// `EliminateRedundantRepartition` optimizer rule removes a `RepartitionExec`
/// with `Partitioning::RoundRobinBatch` that sits directly on top of an
/// `ExplicitRepartitionExec`.
///
/// `RepartitionExec` with `Partitioning::Hash(..)` or `Partitioning::UnknownPartitioning`
/// is left untouched, since eliminating it could violate the parent node's
/// distribution requirement.
///
/// This rule should be applied after the `EnforceDistribution`
/// rule and before the `RewriteExplicitRepartition` rule.
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
            let Some(repartition) = node.downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };

            let child = repartition.input();
            let Some(_explicit) = child.downcast_ref::<ExplicitRepartitionExec>() else {
                return Ok(Transformed::no(node));
            };

            // Eliminating a RepartitionExec with a RoundRobinBatch scheme that sits directly
            // on top of an ExplicitRepartitionExec does not violate the parent node's
            // distribution requirements.
            if matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_)) {
                return Ok(Transformed::yes(child.clone()));
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
