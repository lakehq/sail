use async_recursion::async_recursion;
use datafusion_common::ScalarValue;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
use sail_common::spec;

use crate::error::PlanResult;
use crate::explain::{
    ExplainOptions, distributed_explain_string_from_logical_plan, explain_string_from_logical_plan,
};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    #[async_recursion]
    pub(super) async fn resolve_command_explain(
        &self,
        input: spec::Plan,
        request: spec::ExplainRequest,
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
        let explain = match request {
            spec::ExplainRequest::Spark { mode } => {
                let options = ExplainOptions::from_mode(mode);
                explain_string_from_logical_plan(self.ctx, plan, fields, options).await?
            }
            spec::ExplainRequest::Sail {
                kind: spec::SailExplainKind::Distributed,
                format,
                analyze,
                verbose,
            } => {
                distributed_explain_string_from_logical_plan(
                    self.ctx, plan, fields, format, analyze, verbose,
                )
                .await?
            }
        };
        let plan =
            LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Utf8(Some(explain.output)))]])?
                .project(vec![col("column1").alias("plan")])?
                .build()?;
        Ok(plan)
    }
}
