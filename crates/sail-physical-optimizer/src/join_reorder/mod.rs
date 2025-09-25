use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Default)]
pub struct JoinReorder {}

impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for JoinReorder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinReorder")
    }
}
