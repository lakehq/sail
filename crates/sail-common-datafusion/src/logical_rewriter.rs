use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan;

/// A trait for rewriting logical plans after logical optimization.
/// This is needed so that the rewritten plan does not confuse the multi-pass
/// logical optimization process.
#[async_trait]
pub trait LogicalRewriter: Send + Sync {
    fn name(&self) -> &str;

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>>;
}
