use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan;

#[derive(Debug, Default)]
pub struct ResolveLambdaVariables;

impl AnalyzerRule for ResolveLambdaVariables {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan.resolve_lambda_variables()?.data)
    }

    fn name(&self) -> &str {
        "resolve_lambda_variables"
    }
}

/// Re-resolves lambda variable fields after the other optimizer rules have run.
///
/// Rules such as constant folding can change the type or nullability of a
/// higher-order function's value arguments (e.g. an array literal whose element
/// field becomes non-nullable once folded into a literal), leaving the lambda
/// variable fields resolved during logical planning stale. The physical planner
/// compares those fields against the planning schema with strict equality, so
/// they are refreshed here.
impl OptimizerRule for ResolveLambdaVariables {
    fn name(&self) -> &str {
        "resolve_lambda_variables"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.resolve_lambda_variables()
    }
}
