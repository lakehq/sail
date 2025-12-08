use async_recursion::async_recursion;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;

use crate::error::PlanResult;
use crate::explain::{explain_string_from_logical_plan, ExplainOptions};
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
        let (plan, fields) = match input {
            spec::Plan::Query(query) => {
                let plan = self.resolve_query_plan(query, state).await?;
                let fields = Some(Self::get_field_names(plan.schema(), state)?);
                (plan, fields)
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, state).await?;
                (plan, None)
            }
        };
        let options = ExplainOptions::from_mode(mode);
        let explain = explain_string_from_logical_plan(self.ctx, plan, fields, options).await?;
        let plan =
            LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Utf8(Some(explain.output)))]])?
                .project(vec![col("column1").alias("plan")])?
                .build()?;
        Ok(plan)
    }
}
