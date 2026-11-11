use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

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
        let output_schema = Arc::clone(input.schema());
        let (predicate, input) = self
            .resolve_expressions_with_missing_inputs(vec![condition], input, state)
            .await?;
        let (input, predicate) = self.rewrite_partition_expressions(input, predicate, state)?;
        let filter = self.rewrite_seeded_random_filter(input, predicate.one()?, false, state)?;
        Self::restore_missing_input_output(filter, output_schema)
    }
}
