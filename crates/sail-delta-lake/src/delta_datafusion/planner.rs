// Duplicate of crates/sail-plan/src/extension/mod.rs, may not be needed.
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::delta_datafusion::DataFusionResult;

/// Deltaplanner
#[derive(Debug)]
pub struct DeltaPlanner<T: ExtensionPlanner> {
    /// custom extension planner
    pub extension_planner: T,
}

#[async_trait]
impl<T: ExtensionPlanner + Send + Sync + 'static + Clone + std::fmt::Debug> QueryPlanner
    for DeltaPlanner<T>
{
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![Arc::new(self.extension_planner.clone())],
        )));
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
