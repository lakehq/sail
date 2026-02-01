use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a WithRelations node by storing referenced plans and resolving the root.
    pub(super) async fn resolve_query_with_relations(
        &self,
        root: spec::QueryPlan,
        references: Vec<spec::QueryPlan>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_subquery_ref_scope();
        let state = scope.state();

        for ref_plan in references {
            let plan_id = ref_plan
                .plan_id
                .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
            state.insert_subquery_ref(plan_id, ref_plan);
        }

        self.resolve_query_plan(root, state).await
    }
}
