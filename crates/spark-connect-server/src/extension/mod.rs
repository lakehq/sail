use std::sync::Arc;

use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use tonic::async_trait;

pub(crate) mod logical;
pub(crate) mod physical;

struct ExtensionQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            physical::RangePlanner {},
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
pub(crate) fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(ExtensionQueryPlanner {})
}
