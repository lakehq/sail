use async_recursion::async_recursion;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;

use crate::error::PlanResult;
use crate::explain::{explain_string, ExplainOptions};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion]
    pub(super) async fn resolve_command_explain(
        &self,
        input: spec::Plan,
        mode: spec::ExplainMode,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let _ = state;
        let options = ExplainOptions::from_mode(mode);
        let explain = explain_string(self.ctx, self.config.clone(), input, options).await?;
        let plan =
            LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Utf8(Some(explain.output)))]])?
                .project(vec![col("column1").alias("plan")])?
                .build()?;
        Ok(plan)
    }
}
