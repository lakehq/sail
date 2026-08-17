use std::collections::HashMap;

use datafusion_expr::LogicalPlan;
use sail_common::spec::{self, Identifier};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_named_windows(
        &self,
        input: spec::QueryPlan,
        window: Vec<(Identifier, spec::Window)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut windows: HashMap<String, spec::Window> = HashMap::new();
        for (name, w) in window {
            if windows.contains_key(name.as_ref()) {
                return Err(PlanError::analysis(format!(
                    "Name {} is used more than once in WINDOW clause",
                    name.as_ref()
                )));
            } else {
                windows.insert(name.into(), w);
            }
        }

        let old_windows = state.set_windows(windows);
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let _ = state.set_windows(old_windows);
        Ok(input)
    }
}
