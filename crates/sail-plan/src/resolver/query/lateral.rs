use std::sync::Arc;

use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Expr, LogicalPlan, Projection};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::get_outer_built_in_generator_functions;
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_query_lateral_view(
        &self,
        input: Option<spec::QueryPlan>,
        function: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        named_arguments: Vec<(spec::Identifier, spec::Expr)>,
        table_alias: Option<spec::ObjectName>,
        column_aliases: Option<Vec<spec::Identifier>>,
        outer: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Ok(function_name) = <Vec<String>>::from(function).one() else {
            return Err(PlanError::unsupported(
                "qualified lateral view function name",
            ));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        if let Ok(f) = self.ctx.udf(&canonical_function_name) {
            if f.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }
        let input = match input {
            Some(x) => self.resolve_query_plan(x, state).await?,
            None => self.resolve_query_empty(true)?,
        };
        let schema = input.schema().clone();

        if let Ok(f) = self.ctx.udf(&canonical_function_name) {
            if let Some(f) = f.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>() {
                if !f.eval_type().is_table_function() {
                    return Err(PlanError::invalid(format!(
                        "not a table function for UDTF lateral view: {function_name}"
                    )));
                }
                let udtf = PythonUdtf {
                    python_version: f.python_version().to_string(),
                    eval_type: f.eval_type(),
                    command: f.command().to_vec(),
                    return_type: f.output_type().clone(),
                };
                let arguments = self
                    .resolve_named_expressions(arguments, input.schema(), state)
                    .await?;
                let output_names =
                    column_aliases.map(|aliases| aliases.into_iter().map(|x| x.into()).collect());
                let output_qualifier = table_alias
                    .map(|alias| self.resolve_table_reference(&alias))
                    .transpose()?;
                return self.resolve_python_udtf_plan(
                    udtf,
                    &function_name,
                    input,
                    arguments,
                    output_names,
                    output_qualifier,
                    f.deterministic(),
                    state,
                );
            }
        }

        let function_name = if outer {
            get_outer_built_in_generator_functions(&function_name).to_string()
        } else {
            function_name
        };
        let expression = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
            function_name: spec::ObjectName::bare(function_name),
            arguments,
            named_arguments,
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        });
        let expression = if let Some(aliases) = column_aliases {
            spec::Expr::Alias {
                expr: Box::new(expression),
                name: aliases,
                metadata: None,
            }
        } else {
            expression
        };
        let expr = self
            .resolve_named_expression(expression, &schema, state)
            .await?;
        let (input, expr) = self.rewrite_wildcard(input, vec![expr], state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        let expr = if let Some(table_alias) = table_alias {
            let table_reference = self.resolve_table_reference(&table_alias)?;
            expr.into_iter()
                .map(|x| {
                    let name = x.schema_name().to_string();
                    x.alias_qualified(Some(table_reference.clone()), name)
                })
                .collect()
        } else {
            expr
        };
        let projections = schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .chain(expr.into_iter())
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(Projection::try_new(
            projections,
            Arc::new(input),
        )?))
    }
}
