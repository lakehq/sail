use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{expr, EmptyRelation, Expr, LogicalPlan};
use sail_catalog::manager::CatalogManager;
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
        // Extract any NamedArgument entries embedded in arguments (Spark Connect inline path).
        // PySpark encodes kwargs as NamedArgumentExpression inside the arguments[] repeated field,
        // which are converted to spec::Expr::NamedArgument. extract_kwargs peels those out before
        // resolve_expressions_and_names, which would otherwise reject them as standalone expressions.
        let (mut arguments, mut kwarg_names) = Self::extract_kwargs(arguments);
        // Also merge named_arguments from spec::UnresolvedFunction (SQL analyzer path).
        // PySpark sends registered-UDF kwargs here rather than as embedded NamedArgument entries.
        for (key, value) in named_arguments {
            kwarg_names.push(Some(key.into()));
            arguments.push(value);
        }

        // Validate named arguments: reject duplicate kwarg names early so we surface
        // a proper AnalysisException instead of letting it crash in the Python worker.
        {
            let mut seen_kwarg_names = std::collections::HashSet::new();
            for name in kwarg_names.iter().flatten() {
                if !seen_kwarg_names.insert(name.as_str()) {
                    return Err(PlanError::AnalysisError(format!(
                        "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] \
                         Duplicate named argument: '{name}' is assigned more than once."
                    )));
                }
            }
        }

        let canonical_function_name = function_name.to_ascii_lowercase();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        if let Some(udf) = catalog_manager.get_function(&canonical_function_name)? {
            if udf.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }

        // For functions that accept a date-part keyword as the first argument
        // (e.g., DATEDIFF(DAY, start, end)), convert the unresolved attribute
        // to a string literal before resolution.
        let arguments = Self::convert_date_part_argument(&canonical_function_name, arguments);

        let (argument_display_names, arguments) = if canonical_function_name == "struct" {
            self.resolve_struct_expressions_and_names(arguments, schema, state)
                .await?
        } else {
            self.resolve_expressions_and_names(arguments, schema, state)
                .await?
        };

        // FIXME: `is_user_defined_function` is always false,
        //   so we need to check UDFs before built-in functions.
        let func = if let Some(udf) = catalog_manager.get_function(&canonical_function_name)? {
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
                    &kwarg_names, // pass kwargs from named_arguments
                    schema,
                    f.deterministic(),
                    is_distinct,
                    state,
                )?
            } else {
                expr::Expr::ScalarFunction(ScalarFunction {
                    func: std::sync::Arc::new(udf),
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
            // For DISTINCT aggregate functions with a wildcard argument (e.g., COUNT(DISTINCT *)),
            // expand the wildcard to visible column references here in the resolver where we have
            // access to `state` for hidden-column filtering. This ensures hidden columns (e.g.,
            // join keys) are excluded from the distinct count.
            #[expect(deprecated)]
            let arguments = if is_distinct
                && matches!(
                    arguments.as_slice(),
                    [expr::Expr::Wildcard {
                        qualifier: None,
                        options: _
                    }]
                ) {
                schema
                    .columns()
                    .into_iter()
                    .filter(|c| {
                        state
                            .get_field_info(&c.name)
                            .is_ok_and(|info| !info.is_hidden())
                    })
                    .map(expr::Expr::Column)
                    .collect()
            } else {
                arguments
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

        // When `COUNT(DISTINCT *)` is used, expand the wildcard display names
        // to individual column names so the output header matches Spark JVM behavior
        // (e.g., `count(DISTINCT a, b, c)` instead of `count(DISTINCT *)`).
        let argument_display_names =
            if is_distinct && argument_display_names.iter().any(|n| n == "*") {
                schema
                    .columns()
                    .iter()
                    .filter_map(|c| {
                        let info = state.get_field_info(&c.name).ok()?;
                        if info.is_hidden() {
                            None
                        } else {
                            Some(info.name().to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                argument_display_names
            };
        let service = self.ctx.extension::<PlanService>()?;
        let name = service.plan_formatter().function_to_string(
            &function_name,
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;

        // Extract metadata from UDF if it implements return_field_from_args
        let metadata = if let expr::Expr::ScalarFunction(ScalarFunction {
            func: udf, args, ..
        }) = &func
        {
            extract_metadata_from_udf(udf, args)?
        } else {
            vec![]
        };

        if !metadata.is_empty() {
            Ok(NamedExpr::new(vec![name], func).with_metadata(metadata))
        } else {
            Ok(NamedExpr::new(vec![name], func))
        }
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
                #[expect(deprecated)]
                Expr::Wildcard { qualifier, options } => {
                    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    });

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

    /// For functions that accept a date-part keyword as their first argument
    /// (e.g., `DATEDIFF(DAY, start, end)`, `TIMESTAMPDIFF(HOUR, start, end)`),
    /// convert the first argument from an unresolved attribute to a string literal.
    fn convert_date_part_argument(
        function_name: &str,
        mut arguments: Vec<spec::Expr>,
    ) -> Vec<spec::Expr> {
        const DATE_PART_FUNCTIONS: &[&str] = &["datediff", "date_diff", "timestampdiff"];
        if arguments.len() >= 3 && DATE_PART_FUNCTIONS.contains(&function_name) {
            if let spec::Expr::UnresolvedAttribute { ref name, .. } = arguments[0] {
                let parts: Vec<String> = name.clone().into();
                if parts.len() == 1 {
                    arguments[0] = spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some(parts.into_iter().next().unwrap_or_default()),
                    });
                }
            }
        }
        arguments
    }
}

/// Extract metadata from a UDF by calling return_field_from_args with real argument information
fn extract_metadata_from_udf(
    udf: &std::sync::Arc<datafusion_expr::ScalarUDF>,
    args: &[expr::Expr],
) -> PlanResult<Vec<(String, String)>> {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ExprSchemable, ReturnFieldArgs};

    // Extract real field types from the resolved expressions
    let empty_schema = datafusion_common::DFSchema::empty();
    let arg_fields: Vec<Arc<Field>> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let data_type = arg.get_type(&empty_schema).unwrap_or(DataType::Null);
            Arc::new(Field::new(format!("arg_{}", i), data_type, true))
        })
        .collect();

    // Extract literal scalar values from expressions
    let mut scalar_values = Vec::new();

    for arg in args {
        if let expr::Expr::Literal(scalar_value, _) = arg {
            scalar_values.push(scalar_value.clone());
        } else {
            scalar_values.push(ScalarValue::Null);
        }
    }

    let scalar_refs: Vec<Option<&ScalarValue>> = scalar_values
        .iter()
        .map(|v| if v.is_null() { None } else { Some(v) })
        .collect();

    let return_field_args = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &scalar_refs,
    };

    // Try to extract metadata, but don't fail if it doesn't work
    if let Ok(field) = udf.return_field_from_args(return_field_args) {
        Ok(field
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    } else {
        Ok(vec![])
    }
}
