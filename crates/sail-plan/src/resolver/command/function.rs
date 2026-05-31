use datafusion_expr::LogicalPlan;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) fn resolve_catalog_register_function(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct: _,
            arguments: _,
            function,
        } = function;

        let function_name: String = function_name.into();
        let function_name = function_name.to_ascii_lowercase();
        let function = self.resolve_python_udf(function, state)?;
        let udf = PySparkUnresolvedUDF::new(
            function_name,
            function.python_version,
            function.eval_type,
            function.command,
            Some(function.output_type),
            deterministic,
        );

        let manager = self.ctx.extension::<CatalogManager>()?;
        let udf = manager.track_function(datafusion_expr::ScalarUDF::from(udf))?;
        let command = CatalogCommand::RegisterFunction { udf };
        self.resolve_catalog_command(command)
    }

    pub(super) fn resolve_catalog_register_table_function(
        &self,
        function: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments: _,
            function,
        } = function;
        let function_name: String = function_name.into();
        let function_name = function_name.to_ascii_lowercase();
        let function = self.resolve_python_udtf(function, state)?;
        let udtf = PySparkUnresolvedUDF::new(
            function_name,
            function.python_version,
            function.eval_type,
            function.command,
            function.return_type,
            deterministic,
        );
        // PySpark UDTF is registered as a scalar UDF since it will be used as a stream UDF
        // in the `MapPartitions` plan.
        let manager = self.ctx.extension::<CatalogManager>()?;
        let udf = manager.track_function(datafusion_expr::ScalarUDF::from(udtf))?;
        let command = CatalogCommand::RegisterFunction { udf };
        self.resolve_catalog_command(command)
    }
}
