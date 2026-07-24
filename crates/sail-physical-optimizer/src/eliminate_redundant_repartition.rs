use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

/// EliminateRedundantRepartition optimizer rule removes a RepartitionExec
/// that is directly on top of an ExplicitRepartitionExec, since the input has
/// already been explicitly repartitioned and the additional repartition is
/// redundant.
///
/// This rule should be applied after the EnforceDistribution
/// rule and before the RewriteExplicitRepartition rule.
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
            if node.downcast_ref::<RepartitionExec>().is_none() {
                return Ok(Transformed::no(node));
            };

            let [child] = node.children()[..] else {
                return Ok(Transformed::no(node));
            };

            if child.downcast_ref::<ExplicitRepartitionExec>().is_some() {
                Ok(Transformed::yes(child.clone()))
            } else {
                Ok(Transformed::no(node))
            }
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

    #[test]
    fn test_eliminate_redundant_repartition_above_explicit_repartition() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let explicit: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::RoundRobinBatch(3),
        ));
        let redundant: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(explicit, Partitioning::RoundRobinBatch(10)).unwrap(),
        );

        let rule = EliminateRedundantRepartition::new();
        let result = rule.optimize(redundant, &ConfigOptions::default()).unwrap();

        assert!(result.downcast_ref::<ExplicitRepartitionExec>().is_some());
        assert_eq!(result.output_partitioning().partition_count(), 3);
    }

    #[test]
    fn test_no_change_when_child_is_not_explicit_repartition() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let repartition: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(3)).unwrap());

        let rule = EliminateRedundantRepartition::new();
        let result = rule
            .optimize(repartition, &ConfigOptions::default())
            .unwrap();

        assert!(result.downcast_ref::<RepartitionExec>().is_some());
    }
}
