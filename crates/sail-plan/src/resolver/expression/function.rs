use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard, expr_to_columns};
use datafusion_expr::{expr, EmptyRelation, Expr, ExprSchemable, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_filter_expr::SparkArrayFilterExpr;
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

        // Special handling for higher-order functions with lambdas
        // These need to intercept the lambda BEFORE normal argument resolution
        if canonical_function_name == "filter" && arguments.len() == 2 {
            if let spec::Expr::LambdaFunction {
                function: lambda_body,
                arguments: lambda_args,
            } = &arguments[1]
            {
                // Resolve only the array argument (first arg)
                let array_expr = self
                    .resolve_expression(arguments[0].clone(), schema, state)
                    .await?;

                // Get the element type from the array expression
                let array_type = array_expr.get_type(schema.as_ref())?;
                let element_type = match &array_type {
                    DataType::List(field) | DataType::LargeList(field) => field.data_type().clone(),
                    other => {
                        return Err(PlanError::invalid(format!(
                            "filter() first argument must be an array, got {:?}",
                            other
                        )))
                    }
                };

                // Extract lambda variable names - supports (element) or (element, index)
                let var_names: Vec<String> = lambda_args
                    .iter()
                    .flat_map(|v| {
                        let names: Vec<String> = v.name.clone().into();
                        names
                    })
                    .collect();

                // Register synthetic fields for the lambda variables
                let element_field_id = state.register_synthetic_field();
                let index_field_id = if var_names.len() >= 2 {
                    Some(state.register_synthetic_field())
                } else {
                    None
                };

                // Transform the lambda body to replace variable references with field_ids
                let transformed_body = transform_lambda_variables_with_index(
                    lambda_body.as_ref(),
                    &var_names,
                    &element_field_id,
                    index_field_id.as_deref(),
                )?;

                // Create schema with element column (and optionally index column)
                let mut lambda_schema_fields =
                    vec![Field::new(&element_field_id, element_type.clone(), true)];
                if let Some(ref idx_id) = index_field_id {
                    // Use Int32 to match common array element types for better type compatibility
                    lambda_schema_fields.push(Field::new(idx_id, DataType::Int32, false));
                }
                let lambda_only_schema =
                    DFSchema::try_from(Arc::new(Schema::new(lambda_schema_fields.clone())))?;

                // Combine lambda schema with outer schema for resolving external column references
                let mut combined_schema = lambda_only_schema.clone();
                combined_schema.merge(schema.as_ref());
                let combined_schema = Arc::new(combined_schema);

                // Resolve the transformed lambda body against the combined schema
                let resolved_lambda = self
                    .resolve_expression(transformed_body, &combined_schema, state)
                    .await?;

                // Find which columns from the outer schema are referenced in the lambda
                let mut referenced_columns: HashSet<Column> = HashSet::new();
                let _ = expr_to_columns(&resolved_lambda, &mut referenced_columns);

                // Determine which are external (not lambda variables)
                let lambda_column_names: HashSet<&str> = {
                    let mut names = HashSet::new();
                    names.insert(element_field_id.as_str());
                    if let Some(ref idx_id) = index_field_id {
                        names.insert(idx_id.as_str());
                    }
                    names
                };

                // Collect external columns with their full Column reference (including qualifier)
                let outer_columns_with_refs: Vec<(Column, DataType)> = referenced_columns
                    .iter()
                    .filter(|c| !lambda_column_names.contains(c.name.as_str()))
                    .filter_map(|c| {
                        // Try to find the column in the outer schema (with or without qualifier)
                        schema.index_of_column(c).ok().map(|idx| {
                            let field = schema.field(idx);
                            (c.clone(), field.data_type().clone())
                        })
                    })
                    .collect();

                // Extract just the names and types for the UDF (uses column name as schema field)
                let outer_columns: Vec<(String, DataType)> = outer_columns_with_refs
                    .iter()
                    .map(|(c, dt)| (c.name.clone(), dt.clone()))
                    .collect();

                // Build a map of qualified column names to unqualified for rewriting
                let outer_column_set: HashSet<Column> = outer_columns_with_refs
                    .iter()
                    .map(|(c, _)| c.clone())
                    .collect();

                // Rewrite the lambda expression to remove qualifiers from outer columns
                // This is necessary because the UDF's internal schema uses unqualified names
                let resolved_lambda = resolved_lambda
                    .transform(|e| {
                        match &e {
                            Expr::Column(col) if outer_column_set.contains(col) => {
                                // Replace qualified column with unqualified version
                                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                                    &col.name,
                                ))))
                            }
                            _ => Ok(Transformed::no(e)),
                        }
                    })?
                    .data;

                // Build arguments: array + outer columns (using full column expressions)
                let mut udf_args = vec![array_expr];
                for (column, _) in &outer_columns_with_refs {
                    udf_args.push(Expr::Column(column.clone()));
                }

                // Create the filter UDF with appropriate constructor
                let filter_udf = if outer_columns.is_empty() {
                    if let Some(idx_id) = index_field_id {
                        SparkArrayFilterExpr::with_index_column(
                            resolved_lambda,
                            element_type,
                            element_field_id,
                            idx_id,
                        )
                    } else {
                        SparkArrayFilterExpr::with_column_name(
                            resolved_lambda,
                            element_type,
                            element_field_id,
                        )
                    }
                } else {
                    SparkArrayFilterExpr::with_outer_columns(
                        resolved_lambda,
                        element_type,
                        element_field_id,
                        index_field_id,
                        outer_columns,
                    )
                };
                let udf = Arc::new(datafusion_expr::ScalarUDF::new_from_impl(filter_udf));

                let filter_expr = expr::Expr::ScalarFunction(ScalarFunction {
                    func: udf,
                    args: udf_args,
                });

                return Ok(NamedExpr::new(
                    vec![format!("filter({}, <lambda>)", function_name)],
                    filter_expr,
                ));
            }
        }

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

/// Transform a spec::Expr to replace lambda variable references with attribute references.
///
/// This recursively traverses the expression tree and replaces:
/// - First lambda variable (element) -> `UnresolvedAttribute(element_field_id)`
/// - Second lambda variable (index, if present) -> `UnresolvedAttribute(index_field_id)`
///
/// The field_ids are unique identifiers registered in the state that will be used
/// as column names in the lambda schema for proper attribute resolution.
fn transform_lambda_variables_with_index(
    expr: &spec::Expr,
    lambda_var_names: &[String],
    element_field_id: &str,
    index_field_id: Option<&str>,
) -> PlanResult<spec::Expr> {
    // Helper to determine which field_id to use for a variable name
    let get_field_id = |var_name: &[String]| -> Option<&str> {
        // First variable is the element
        if let Some(first_var) = lambda_var_names.first() {
            if var_name.iter().any(|v| v == first_var) {
                return Some(element_field_id);
            }
        }
        // Second variable is the index
        if let (Some(second_var), Some(idx_field)) = (lambda_var_names.get(1), index_field_id) {
            if var_name.iter().any(|v| v == second_var) {
                return Some(idx_field);
            }
        }
        None
    };

    match expr {
        // Replace lambda variable references
        spec::Expr::UnresolvedNamedLambdaVariable(v) => {
            let var_name: Vec<String> = v.name.clone().into();
            if let Some(field_id) = get_field_id(&var_name) {
                Ok(spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(field_id),
                    plan_id: None,
                    is_metadata_column: false,
                })
            } else {
                Ok(expr.clone())
            }
        }
        spec::Expr::UnresolvedAttribute {
            name,
            plan_id,
            is_metadata_column,
        } => {
            let var_name: Vec<String> = name.clone().into();
            if let Some(field_id) = get_field_id(&var_name) {
                Ok(spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(field_id),
                    plan_id: *plan_id,
                    is_metadata_column: *is_metadata_column,
                })
            } else {
                Ok(expr.clone())
            }
        }

        // Recursively transform function arguments
        spec::Expr::UnresolvedFunction(func) => {
            let transformed_args: Vec<spec::Expr> = func
                .arguments
                .iter()
                .map(|arg| {
                    transform_lambda_variables_with_index(
                        arg,
                        lambda_var_names,
                        element_field_id,
                        index_field_id,
                    )
                })
                .collect::<PlanResult<Vec<_>>>()?;

            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: func.function_name.clone(),
                arguments: transformed_args,
                named_arguments: func.named_arguments.clone(),
                is_distinct: func.is_distinct,
                is_user_defined_function: func.is_user_defined_function,
                is_internal: func.is_internal,
                ignore_nulls: func.ignore_nulls,
                filter: func.filter.clone(),
                order_by: func.order_by.clone(),
            }))
        }

        // Transform cast expressions
        spec::Expr::Cast {
            expr: inner,
            cast_to_type,
            rename,
            is_try,
        } => {
            let transformed = transform_lambda_variables_with_index(
                inner,
                lambda_var_names,
                element_field_id,
                index_field_id,
            )?;
            Ok(spec::Expr::Cast {
                expr: Box::new(transformed),
                cast_to_type: cast_to_type.clone(),
                rename: *rename,
                is_try: *is_try,
            })
        }

        // Transform alias expressions
        spec::Expr::Alias {
            expr: inner,
            name,
            metadata,
        } => {
            let transformed = transform_lambda_variables_with_index(
                inner,
                lambda_var_names,
                element_field_id,
                index_field_id,
            )?;
            Ok(spec::Expr::Alias {
                expr: Box::new(transformed),
                name: name.clone(),
                metadata: metadata.clone(),
            })
        }

        // Literals and other non-recursive expressions pass through unchanged
        spec::Expr::Literal(_) => Ok(expr.clone()),

        // For other expression types, we may need to add more cases
        // For now, return as-is
        _ => Ok(expr.clone()),
    }
}
