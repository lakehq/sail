use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;

#[derive(Debug, Default)]
pub(crate) struct ResolveLambdaVariables;

impl AnalyzerRule for ResolveLambdaVariables {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan.resolve_lambda_variables()?.data)
    }

    fn name(&self) -> &str {
        "resolve_lambda_variables"
    }
}
