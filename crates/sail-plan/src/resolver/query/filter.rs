use std::sync::Arc;

use datafusion_expr::{Filter, LogicalPlan};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_filter(
        &self,
        input: spec::QueryPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let schema = input.schema();
        let predicate = self.resolve_expression(condition, schema, state).await?;
        let filter = Filter::try_new(predicate, Arc::new(input))?;
        Ok(LogicalPlan::Filter(filter))
    }
}
