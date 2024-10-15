use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

pub struct JobDefinition {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

impl JobDefinition {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}
