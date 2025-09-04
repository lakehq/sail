use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

pub struct JoinReorder {}

/// The [`JoinReorder`] optimizer rule implement.
#[allow(dead_code)]
impl JoinReorder {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
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
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
