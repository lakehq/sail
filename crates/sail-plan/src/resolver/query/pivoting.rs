use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_pivot(
        &self,
        _pivot: spec::Pivot,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("pivot"))
    }

    pub(super) async fn resolve_query_unpivot(
        &self,
        _unpivot: spec::Unpivot,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("unpivot"))
    }
}
