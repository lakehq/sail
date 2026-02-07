use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;

/// A trait for rewriting logical plans after logical optimization.
/// This is needed so that the rewritten plan does not confuse the multi-pass
/// logical optimization process.
pub trait LogicalRewriter {
    fn name(&self) -> &str;

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>>;
}
