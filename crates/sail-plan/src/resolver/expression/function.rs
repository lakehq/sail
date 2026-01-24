use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{expr, EmptyRelation, Expr, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{AggFunctionInput, FunctionContextInput, ScalarFunctionInput};
use crate::function::{get_built_in_aggregate_function, get_built_in_function};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_function(
        &self,
        function: spec::UnresolvedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        let spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal: _,
            ignore_nulls,
            filter,
            order_by,
        } = function;

        let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
            return Err(PlanError::unsupported("qualified function name"));
        };
        if !named_arguments.is_empty() {
            return Err(PlanError::todo("named function arguments"));
        }
        let canonical_function_name = function_name.to_ascii_lowercase();
        if let Ok(udf) = self.ctx.udf(&canonical_function_name) {
            if udf.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }

        let (argument_display_names, arguments) = if canonical_function_name == "struct" {
            self.resolve_struct_expressions_and_names(arguments, schema, state)
                .await?
        } else {
            self.resolve_expressions_and_names(arguments, schema, state)
                .await?
        };

        // FIXME: `is_user_defined_function` is always false,
        //   so we need to check UDFs before built-in functions.
        let func = if let Ok(udf) = self.ctx.udf(&canonical_function_name) {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            if let Some(f) = udf.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>() {
                let function = PythonUdf {
                    python_version: f.python_version().to_string(),
                    eval_type: f.eval_type(),
                    command: f.command().to_vec(),
                    output_type: f.output_type().clone(),
                };
                self.resolve_python_udf_expr(
                    function,
                    &function_name,
                    arguments,
                    &argument_display_names,
                    schema,
                    f.deterministic(),
                    is_distinct,
                    state,
                )?
            } else {
                expr::Expr::ScalarFunction(ScalarFunction {
                    func: udf,
                    args: arguments,
                })
            }
        } else if let Ok(func) = get_built_in_function(&canonical_function_name) {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            let input = ScalarFunctionInput {
                arguments,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            func(input)?
        } else if let Ok(func) = get_built_in_aggregate_function(&canonical_function_name) {
            let filter = match filter {
                Some(x) => Some(Box::new(self.resolve_expression(*x, schema, state).await?)),
                None => None,
            };
            let order_by = match order_by {
                Some(x) => self.resolve_sort_orders(x, true, schema, state).await?,
                None => vec![],
            };
            let input = AggFunctionInput {
                arguments,
                distinct: is_distinct,
                ignore_nulls,
                filter,
                order_by,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            func(input)?
        } else {
            return Err(PlanError::unsupported(format!(
                "unknown function: {function_name}",
            )));
        };

        let service = self.ctx.extension::<PlanService>()?;
        let name = service.plan_formatter().function_to_string(
            &function_name,
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], func))
    }

    pub(super) async fn resolve_expression_call_function(
        &self,
        function_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let function = spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        };
        self.resolve_expression_function(function, schema, state)
            .await
    }

    async fn resolve_struct_expressions_and_names(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        fn column_display_name(
            column: &datafusion_common::Column,
            state: &PlanResolverState,
        ) -> PlanResult<Option<String>> {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                return Ok(None);
            }
            Ok(Some(info.name().to_string()))
        }

        let mut names: Vec<String> = vec![];
        let mut exprs: Vec<expr::Expr> = vec![];

        for expression in expressions {
            let NamedExpr { name, expr, .. } = self
                .resolve_named_expression(expression, schema, state)
                .await?;

            match expr {
                // Expand wildcard inside `struct(...)` only, to match Spark behavior:
                // - struct(*) expands to all visible columns
                // - struct(alias.*) expands to all visible columns from that qualifier
                #[allow(deprecated)]
                Expr::Wildcard { qualifier, options } => {
                    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    });

                    #[allow(deprecated)]
                    let expanded = match qualifier {
                        Some(q) => expand_qualified_wildcard(&q, schema, Some(&options))?,
                        None => expand_wildcard(schema, &plan, Some(&options))?,
                    };

                    for e in expanded {
                        let Expr::Column(column) = e else {
                            return Err(PlanError::internal(format!(
                                "column expected for expanded wildcard expression in struct, got: {e:?}"
                            )));
                        };
                        if let Some(display_name) = column_display_name(&column, state)? {
                            names.push(display_name);
                            exprs.push(Expr::Column(column));
                        }
                    }
                }

                // Nested-field wildcard expansion can produce a MultiExpr.
                // Flatten it so `struct(a.*)` can become `struct(a.x, a.y, ...)`.
                Expr::ScalarFunction(ScalarFunction { func, args })
                    if func.inner().as_any().is::<MultiExpr>() =>
                {
                    if name.len() == args.len() {
                        for (n, arg) in name.into_iter().zip(args) {
                            let field_name = n;
                            names.push(field_name.clone());
                            exprs.push(Expr::Alias(expr::Alias::new(
                                arg,
                                None::<datafusion_common::TableReference>,
                                field_name,
                            )));
                        }
                    } else {
                        return Err(PlanError::internal(format!(
                            "MultiExpr in struct argument must provide one name per expression, got {} names for {} expressions",
                            name.len(),
                            args.len()
                        )));
                    }
                }

                other => {
                    names.push(name.one()?);
                    exprs.push(other);
                }
            }
        }

        Ok((names, exprs))
    }
}
