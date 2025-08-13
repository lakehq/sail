use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark streaming query API.
    pub(super) async fn resolve_command_write_stream(
        &self,
        _write_stream: spec::WriteStream,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("write stream"))
    }
}
