use datafusion_common::ScalarValue;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

enum FunctionResolution {
    Python(super::udf::PythonFunction, bool),
    HigherOrder(std::sync::Arc<datafusion_expr::HigherOrderUDF>),
    Native,
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_function(
        &mut self,
        id: ExprId,
        function: spec::UnresolvedFunction,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpr> {
        let spec::UnresolvedFunction {
            function_name,
            mut arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal: _,
            ignore_nulls,
            filter,
            mut order_by,
        } = function;
        let original_name = Vec::<String>::from(function_name).join(".");
        let name = original_name.to_ascii_lowercase();
        let registered = self.registered_function(&name)?;
        let python = registered
            .as_ref()
            .map(Self::registered_python)
            .transpose()?
            .flatten();
        let higher_order =
            crate::function::scalar::lambda::get_higher_order_function(&name, &self.context.config)
                .filter(|_| {
                    arguments
                        .iter()
                        .any(crate::resolver::expression::lambda::is_lambda)
                        || matches!(
                            name.as_str(),
                            "map_filter" | "transform_keys" | "transform_values"
                        )
                        || (matches!(name.as_str(), "aggregate" | "reduce") && arguments.len() == 3)
                });
        let resolution = python
            .map(|(definition, deterministic)| {
                FunctionResolution::Python(definition, deterministic)
            })
            .or_else(|| higher_order.map(FunctionResolution::HigherOrder))
            .unwrap_or(FunctionResolution::Native);
        match resolution {
            FunctionResolution::Python(definition, deterministic) => {
                if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                    return Err(PlanError::analysis("invalid scalar function clause"));
                }
                arguments.extend(named_arguments.into_iter().map(|(key, value)| {
                    spec::Expr::NamedArgument {
                        key: key.into(),
                        value: Box::new(value),
                    }
                }));
                self.python_arguments(
                    id,
                    &name,
                    definition,
                    deterministic,
                    is_distinct,
                    arguments,
                    schema,
                    sail_python_udf::udf::pyspark_udaf::PySparkAggregateMode::Grouped,
                )
                .await
            }
            FunctionResolution::HigherOrder(function) => {
                if !named_arguments.is_empty() {
                    return Err(PlanError::unsupported("native named function arguments"));
                }
                if matches!(name.as_str(), "aggregate" | "reduce") && arguments.len() == 3 {
                    let parameter = spec::UnresolvedNamedLambdaVariable {
                        name: spec::ObjectName::bare("__finish"),
                    };
                    arguments.push(spec::Expr::LambdaFunction {
                        function: Box::new(spec::Expr::UnresolvedNamedLambdaVariable(
                            parameter.clone(),
                        )),
                        arguments: vec![parameter],
                    });
                }
                if is_distinct || ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                    return Err(PlanError::analysis("invalid higher-order function clause"));
                }
                self.higher_order(id, name, function, arguments, schema)
                    .await
            }
            FunctionResolution::Native => {
                // Unsupported higher-order functions cannot resolve their lambda arguments.
                if registered.is_none() && matches!(name.as_str(), "map_zip_with" | "zip_with") {
                    return Err(PlanError::todo(format!("function: {name}")));
                }
                if !named_arguments.is_empty() {
                    return Err(PlanError::unsupported("native named function arguments"));
                }
                match (name.as_str(), order_by.take()) {
                    ("mode", Some(sorts)) => {
                        if !arguments.is_empty() {
                            return Err(PlanError::invalid(
                                "mode() with WITHIN GROUP does not take arguments",
                            ));
                        }
                        let sort = sorts.one()?;
                        let tie_break = if sort.direction == spec::SortDirection::Descending {
                            "highest"
                        } else {
                            "lowest"
                        };
                        arguments = vec![
                            *sort.child,
                            spec::Expr::Literal(spec::Literal::Utf8 {
                                value: Some(tie_break.to_string()),
                            }),
                        ];
                    }
                    (_, sorts) => order_by = sorts,
                }

                let mut values = Vec::with_capacity(arguments.len());
                let mut names = Vec::with_capacity(arguments.len());
                let argument_count = arguments.len();
                for (index, mut argument) in arguments.into_iter().enumerate() {
                    if index == 0
                        && argument_count == 3
                        && matches!(
                            name.as_str(),
                            "dateadd"
                                | "date_add"
                                | "timestampadd"
                                | "timestampdiff"
                                | "date_diff"
                                | "datediff"
                        )
                        && let spec::Expr::UnresolvedAttribute { name, .. } = &argument
                    {
                        argument = spec::Expr::Literal(spec::Literal::Utf8 {
                            value: Some(Vec::<String>::from(name.clone()).join(".")),
                        });
                    }
                    match argument {
                        spec::Expr::UnresolvedStar { .. } if name == "count" && !is_distinct => {
                            names.push("1".into());
                            values.push(Expr::literal(
                                self.context.ids.expression(),
                                ScalarValue::Int64(Some(1)),
                            ));
                        }
                        spec::Expr::UnresolvedRegex { col_name, plan_id } => {
                            for value in self.expand_regex(&col_name, plan_id, schema)? {
                                names.push(value.name);
                                values.push(value.expression);
                            }
                        }
                        spec::Expr::UnresolvedStar {
                            target,
                            plan_id,
                            wildcard_options,
                        } => {
                            for argument in self
                                .expand_star(target, plan_id, wildcard_options, schema)
                                .await?
                            {
                                names.push(argument.name);
                                values.push(argument.expression);
                            }
                        }
                        argument => {
                            let argument = self.resolve_named_expression(argument, schema).await?;
                            names.push(argument.name);
                            values.push(argument.expression);
                        }
                    }
                }
                let display_name = self
                    .context
                    .session
                    .extension::<PlanService>()?
                    .plan_formatter()
                    .function_to_string(
                        &original_name,
                        names.iter().map(String::as_str).collect(),
                        is_distinct,
                    )?;
                let scalar_clause = || -> PlanResult<()> {
                    if is_distinct
                        || ignore_nulls.is_some()
                        || filter.is_some()
                        || order_by.is_some()
                    {
                        return Err(PlanError::analysis("invalid scalar function clause"));
                    }
                    Ok(())
                };
                let function = crate::function::resolve_function(
                    &name,
                    registered,
                    &self.context.function_context(),
                )?;
                let expression = match function {
                    crate::function::Function::Scalar(function) => {
                        scalar_clause()?;
                        function(crate::function::common::ScalarFunctionInput {
                            arguments: values,
                            argument_display_names: &names,
                            context: self.context.function_context(),
                        })?
                    }
                    crate::function::Function::Aggregate(function) => {
                        let filter = if let Some(filter) = filter {
                            Some(Box::new(
                                self.resolve_named_expression(*filter, schema)
                                    .await?
                                    .expression,
                            ))
                        } else {
                            None
                        };
                        let mut resolved_order = vec![];
                        for sort in order_by.unwrap_or_default() {
                            resolved_order.push(self.resolve_sort_order(sort, schema).await?);
                        }
                        function.call(crate::function::aggregate::AggregateFunctionInput {
                            arguments: values,
                            context: self.context.function_context(),
                            options: crate::function::aggregate::AggregateOptions {
                                window: false,
                                distinct: is_distinct,
                                ignore_nulls,
                                filter,
                                order_by: resolved_order,
                            },
                        })?
                    }
                };
                Ok(NamedExpr {
                    name: display_name,
                    expression,
                })
            }
        }
    }
}
