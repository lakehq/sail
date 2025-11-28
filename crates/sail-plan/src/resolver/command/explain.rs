use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::ToDFSchema;
use datafusion_expr::{Explain, ExplainFormat, LogicalPlan};
use sail_common::spec;

use crate::error::PlanResult;
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
        let input = match input {
            spec::Plan::Query(query) => self.resolve_query_plan(query, state).await?,
            spec::Plan::Command(command) => self.resolve_command_plan(command, state).await?,
        };
        let stringified_plans: Vec<StringifiedPlan> =
            vec![input.to_stringified(PlanType::InitialLogicalPlan)];
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;
        Ok(LogicalPlan::Explain(Explain {
            verbose: matches!(mode, spec::ExplainMode::Verbose),
            explain_format: ExplainFormat::Indent,
            plan: Arc::new(input),
            stringified_plans,
            schema,
            logical_optimization_succeeded: true,
        }))
    }
}
