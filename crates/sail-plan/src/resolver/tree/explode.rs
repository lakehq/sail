use arrow_schema::DataType;
use datafusion::functions::core;
use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

pub(in crate::resolver) fn is_generator(expression: &spec::Expr) -> bool {
    match expression {
        spec::Expr::Alias { expr, .. } => is_generator(expr),
        spec::Expr::UnresolvedFunction(f) => {
            matches!(f.function_name.parts(), [name] if crate::function::get_built_in_generator_function(&name.as_ref().to_ascii_lowercase()).is_some())
        }
        _ => false,
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn generator(
        &mut self,
        mut input: Query,
        request: spec::Expr,
    ) -> PlanResult<(Query, Vec<NamedExpression>)> {
        let (request, aliases) = match request {
            spec::Expr::Alias { expr, name, .. } => (*expr, Some(name)),
            request => (request, None),
        };
        let spec::Expr::UnresolvedFunction(function) = request else {
            return Err(PlanError::analysis("expected a generator function"));
        };
        let function_name = Vec::<String>::from(function.function_name.clone())
            .join(".")
            .to_ascii_lowercase();
        match self.registered_function(&function_name)? {
            Some(registered) => {
                let (definition, deterministic) = Self::registered_python_table(&registered)?;
                let mut arguments = function.arguments;
                arguments.extend(function.named_arguments.into_iter().map(|(key, value)| {
                    spec::Expr::NamedArgument {
                        key: key.into(),
                        value: Box::new(value),
                    }
                }));
                let width = input.schema.resolved("UDTF passthrough")?.attributes.len();
                let input = self
                    .python_table(
                        input,
                        &function_name,
                        definition,
                        deterministic,
                        arguments,
                        aliases.map(|a| a.into_iter().map(Into::into).collect()),
                    )
                    .await?;
                let generated = input.schema.resolved("UDTF result")?.attributes[width..]
                    .iter()
                    .map(|a| {
                        Ok(NamedExpression {
                            name: a.name.clone(),
                            expression: self.reference(a)?,
                        })
                    })
                    .collect::<PlanResult<_>>()?;
                Ok((input, generated))
            }
            None => {
                if function.is_distinct || function.filter.is_some() || function.order_by.is_some()
                {
                    return Err(PlanError::analysis("invalid generator function modifiers"));
                }
                let name = function_name.as_str();
                let outer = name.ends_with("_outer");
                let position = name.starts_with("posexplode");
                let inline = name.starts_with("inline")
                    || name.starts_with("variant_explode")
                    || matches!(name, "json_tuple" | "stack");
                let replacements = self.expression_replacements.len();
                let mut arguments = vec![];
                let requests =
                    function
                        .arguments
                        .into_iter()
                        .chain(function.named_arguments.into_iter().map(|(key, value)| {
                            spec::Expr::NamedArgument {
                                key: key.into(),
                                value: Box::new(value),
                            }
                        }));
                for argument in requests {
                    let argument = match argument {
                        spec::Expr::NamedArgument { key, value } => {
                            let parameter = match name {
                                "explode" | "explode_outer" | "posexplode" | "posexplode_outer" => {
                                    "collection"
                                }
                                "inline" | "inline_outer" => "input",
                                _ => {
                                    return Err(PlanError::unsupported(
                                        "native named function arguments",
                                    ));
                                }
                            };
                            if !key.eq_ignore_ascii_case(parameter) {
                                return Err(PlanError::analysis(format!(
                                    "unknown named argument for {name}: {key}"
                                )));
                            }
                            *value
                        }
                        argument => argument,
                    };
                    let mut nested = vec![];
                    nested_generators(&argument, &mut nested);
                    for request in nested {
                        let (next, mut values) = self.generator(input, request.clone()).await?;
                        input = next;
                        if values.len() != 1 {
                            return Err(PlanError::analysis(
                                "nested generator must produce one column",
                            ));
                        }
                        self.expression_replacements
                            .push((request, values.remove(0)));
                    }
                    arguments.push(
                        self.resolve_named_expression(
                            argument,
                            input.schema.resolved("generator input")?,
                        )
                        .await?
                        .expression,
                    );
                }
                self.expression_replacements.truncate(replacements);
                let map = arguments.first().is_some_and(|argument| {
            matches!(&argument.result_type, crate::analyzer::deferred::Deferred::Resolved(result_type) if matches!(result_type.data_type, DataType::Map(_, _)))
        });
                let function =
                    crate::function::get_built_in_generator_function(name).ok_or_else(|| {
                        PlanError::analysis(format!("unknown generator function: {name}"))
                    })?;
                let argument = function(crate::function::common::ScalarFunctionInput {
                    arguments,
                    argument_display_names: &[],
                    context: self.context.function_context(),
                })?;
                let (input, array) = self.append(input, argument, "generator".into())?;
                let (input, element) = self.unnest(input, &array, outer)?;
                let mut paths: Vec<(String, Vec<String>)> = vec![];
                if position {
                    paths.push(("pos".into(), vec!["pos".into()]));
                }
                let prefix = if position {
                    vec!["col".to_string()]
                } else {
                    vec![]
                };
                if map {
                    for (name, field) in [
                        ("key", spec::SAIL_MAP_KEY_FIELD_NAME),
                        ("value", spec::SAIL_MAP_VALUE_FIELD_NAME),
                    ] {
                        let mut path = prefix.clone();
                        path.push(field.into());
                        paths.push((name.into(), path));
                    }
                } else if inline {
                    let DataType::Struct(fields) =
                        &element.result_type.resolved("inline element")?.data_type
                    else {
                        return Err(PlanError::analysis("inline requires an array of structs"));
                    };
                    paths.extend(
                        fields
                            .iter()
                            .map(|f| (f.name().clone(), vec![f.name().clone()])),
                    );
                } else {
                    paths.push((
                        if name == "window" { "window" } else { "col" }.into(),
                        prefix,
                    ));
                }
                if let Some(aliases) = aliases {
                    if paths.len() != aliases.len() {
                        return Err(PlanError::analysis(
                            "generator alias count does not match output width",
                        ));
                    }
                    for ((name, _), alias) in paths.iter_mut().zip(aliases) {
                        *name = alias.into();
                    }
                }
                let mut expressions = vec![];
                for (name, path) in paths {
                    let mut expression = self.reference(&element)?;
                    for field in path {
                        let selector = self
                            .context
                            .function_context()
                            .literal(ScalarValue::Utf8(Some(field)));
                        expression = self
                            .context
                            .function_context()
                            .scalar_udf(core::get_field(), vec![expression, selector])?;
                    }
                    if inline {
                        let parent = self.reference(&element)?;
                        let is_null = self.context.function_context().is_null(parent);
                        let null = self
                            .context
                            .function_context()
                            .literal(ScalarValue::try_from(
                                &expression.result_type.resolved("inline field")?.data_type,
                            )?);
                        expression = self
                            .context
                            .function_context()
                            .case(vec![(is_null, null)], Some(expression))?;
                    }
                    expressions.push(NamedExpression { name, expression });
                }
                Ok((input, expressions))
            }
        }
    }
}

/// Find generators inside a projection without crossing subquery or lambda scopes.
pub(in crate::resolver) fn nested_generators(
    expression: &spec::Expr,
    output: &mut Vec<spec::Expr>,
) {
    if is_generator(expression) {
        output.push(expression.clone());
        return;
    }
    match expression {
        spec::Expr::UnresolvedExtractValue { child, extraction } => {
            nested_generators(child, output);
            nested_generators(extraction, output);
        }
        spec::Expr::Alias { expr, .. } | spec::Expr::Cast { expr, .. } => {
            nested_generators(expr, output)
        }
        spec::Expr::UnresolvedFunction(function) => {
            for argument in &function.arguments {
                nested_generators(argument, output);
            }
            for (_, argument) in &function.named_arguments {
                nested_generators(argument, output);
            }
        }
        _ => {}
    }
}
