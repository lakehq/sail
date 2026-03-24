use std::collections::HashSet;
use std::sync::Arc;

use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_with_ctes(
        &self,
        input: spec::QueryPlan,
        recursive: bool,
        ctes: Vec<(spec::Identifier, spec::QueryPlan)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Deduplicate CTEs - keep the last occurrence of each name (shadowing behavior)
        // This matches Spark's behavior where later CTEs can shadow earlier ones
        let mut seen_names: HashSet<spec::Identifier> = HashSet::new();
        let ctes: Vec<_> = ctes
            .into_iter()
            .rev() // Reverse to keep last occurrence
            .filter(|(name, _)| seen_names.insert(name.clone()))
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Reverse back to original order
            .collect();
        let mut scope = state.enter_cte_scope();
        let state = scope.state();
        for (name, query) in ctes.into_iter() {
            let reference = self.resolve_table_reference(&spec::ObjectName::bare(name.clone()))?;
            let plan = if recursive {
                self.resolve_recursive_query_plan(query, state).await?
            } else {
                self.resolve_query_plan(query, state).await?
            };
            let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(plan),
                reference.clone(),
            )?);
            state.insert_cte(reference, plan);
        }
        self.resolve_query_plan(input, state).await
    }
}
