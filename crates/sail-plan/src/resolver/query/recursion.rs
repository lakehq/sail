use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_recursive_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        match plan {
            spec::QueryPlan {
                node:
                    spec::QueryNode::SetOperation(spec::SetOperation {
                        left: _,
                        right: _,
                        set_op_type: _,
                        is_all: _,
                        by_name: _,
                        allow_missing_columns: _,
                    }),
                ..
            } => Err(PlanError::todo("Recursive CTEs")),
            other => self.resolve_query_plan(other, state).await,
        }
    }
}
