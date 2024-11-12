use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

pub mod function;
pub mod logical;
pub mod physical;
pub(crate) mod source;

#[derive(Debug)]
pub(crate) struct ExtensionQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            physical::ExtensionPhysicalPlanner {},
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
