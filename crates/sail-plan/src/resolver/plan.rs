use datafusion::logical_expr::LogicalPlan;
use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

#[derive(Debug)]
pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing fields for query plan,
    /// or `None` for a non-query plan (e.g. a DDL statement).
    pub fields: Option<Vec<String>>,
}

impl PlanResolver<'_> {
    /// Resolves a plan into a named plan.
    pub async fn resolve_named_plan(&self, plan: spec::Plan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        self.resolve_named_plan_with_state(plan, &mut state).await
    }

    /// Resolve a prepared plan and collect placeholders before command resolution
    /// stores expressions in catalog objects or row-level operation metadata.
    pub async fn resolve_prepared_plan(
        &self,
        plan: spec::Plan,
    ) -> PlanResult<(NamedPlan, Vec<String>)> {
        let mut state = PlanResolverState::new();
        let plan = self.resolve_named_plan_with_state(plan, &mut state).await?;
        Ok((plan, state.placeholders.into_iter().collect()))
    }

    /// Bind parameters during resolution so command expressions are substituted
    /// before they leave the logical plan's expression tree.
    pub async fn resolve_named_plan_with_parameters(
        &self,
        plan: spec::Plan,
        parameters: Vec<ScalarValue>,
    ) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        state.bound_parameters = Some(parameters);
        self.resolve_named_plan_with_state(plan, &mut state).await
    }

    async fn resolve_named_plan_with_state(
        &self,
        plan: spec::Plan,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedPlan> {
        match plan {
            spec::Plan::Query(query) => {
                let plan = self.resolve_query_plan(query, state).await?;
                let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
                let fields = Some(Self::get_field_names(plan.schema(), state)?);
                Ok(NamedPlan { plan, fields })
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, state).await?;
                let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
                Ok(NamedPlan { plan, fields: None })
            }
        }
    }
}
