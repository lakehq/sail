use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn resolve_query_common_inline_udtf(
        &self,
        udtf: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = udtf;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udtf(function, state)?;
        let input = self.resolve_empty_query_plan()?;
        let arguments = self
            .resolve_named_expressions(arguments, input.schema(), state)
            .await?;
        self.resolve_python_udtf_plan(
            function,
            &function_name,
            input,
            arguments,
            None,
            None,
            deterministic,
            state,
        )
    }
}
