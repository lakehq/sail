use datafusion_expr::LogicalPlan;
use sail_common::spec::{self, Identifier};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_named_windows(
        &self,
        input: spec::QueryPlan,
        window: Vec<(Identifier, spec::Window)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        for (name, w) in window {
            if state.get_window(name.as_ref()).is_some() {
                return Err(PlanError::AnalysisError(format!(
                    "Name {} is used more than once in WINDOW clause",
                    name.as_ref()
                )));
            } else {
                state.insert_window(name.into(), w);
            }
        }
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        Ok(input)
    }
}
