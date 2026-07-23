use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use datafusion::{common::tree_node::TreeNode, physical_optimizer::PhysicalOptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::error::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::config::ConfigOptions;

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
    ) -> Result<Arc<dyn ExecutionPlan>>
    {
        let result = plan.transform_up(|node| {
            if node.downcast_ref::<RepartitionExec>().is_none() {
                return Ok(Transformed::no(node))
            };

            let child = node.children()[0];
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

