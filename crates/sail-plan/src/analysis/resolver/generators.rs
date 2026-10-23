use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_common::spec;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;

use super::{NamedExpression, Resolver};
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

pub(super) fn is_generator(expression: &spec::Expr) -> bool {
    match expression {
        spec::Expr::Alias { expr, .. } => is_generator(expr),
        spec::Expr::UnresolvedFunction(f) => {
            matches!(f.function_name.parts(), [name] if matches!(name.as_ref().to_ascii_lowercase().as_str(), "explode" | "explode_outer" | "posexplode" | "posexplode_outer" | "inline" | "inline_outer" | "variant_explode" | "variant_explode_outer" | "window" | "json_tuple"))
        }
        _ => false,
    }
}

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn generator(
        &mut self,
        mut input: LogicalPlan,
        request: spec::Expr,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpression>)> {
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
        if let Some(registered) = self.registered_function(&function_name)? {
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
            return Ok((input, generated));
        }
        if !matches!(
            function_name.as_str(),
            "explode"
                | "explode_outer"
                | "posexplode"
                | "posexplode_outer"
                | "inline"
                | "inline_outer"
                | "variant_explode"
                | "variant_explode_outer"
                | "window"
                | "json_tuple"
        ) {
            return Err(PlanError::analysis(format!(
                "unknown table function: {function_name}"
            )));
        }
        if !function.named_arguments.is_empty()
            || function.is_distinct
            || function.filter.is_some()
            || function.order_by.is_some()
        {
            return Err(PlanError::analysis("invalid generator function modifiers"));
        }
        let name = function_name.as_str();
        let outer = name.ends_with("_outer");
        let position = name.starts_with("posexplode");
        let inline = name.starts_with("inline")
            || name.starts_with("variant_explode")
            || name == "json_tuple";
        let replacements = self.expression_replacements.len();
        let mut arguments = vec![];
        for argument in function.arguments {
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
                self.expression(argument, input.schema.resolved("generator input")?)
                    .await?
                    .expression,
            );
        }
        self.expression_replacements.truncate(replacements);
        let mut argument = match name {
            "window" => self.window_buckets(arguments)?,
            "variant_explode" | "variant_explode_outer" => self.scalar_udf(Arc::new(ScalarUDF::from(sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf::new())), arguments)?,
            "json_tuple" => {
                if arguments.len() < 2 {
                    return Err(PlanError::invalid(
                        "json_tuple requires at least 2 arguments",
                    ));
                }
                for field in &arguments[1..] {
                    if !matches!(
                        field.node,
                        crate::analysis::deferred::Deferred::Resolved(ExpressionNode::Literal(
                            ScalarValue::Utf8(Some(_))
                        ))
                    ) {
                        return Err(PlanError::invalid(
                            "json_tuple field names must be string literals",
                        ));
                    }
                }
                let tuple =
                    self.scalar_udf(datafusion_spark::function::json::json_tuple(), arguments)?;
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(
                        sail_function::scalar::array::spark_array::SparkArray::new(),
                    )),
                    vec![tuple],
                )?
            }
            _ => {
                use sail_common_datafusion::utils::items::ItemTaker;
                arguments.one()?
            }
        };
        let map = matches!(
            argument
                .result_type
                .resolved("generator argument")?
                .data_type,
            DataType::Map(_, _)
        );
        if inline && map {
            return Err(PlanError::analysis("inline requires an array of structs"));
        }
        if map {
            argument = self.scalar("map_entries", vec![argument])?;
        }
        if position {
            argument = self.scalar_udf(
                Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())),
                vec![argument],
            )?;
        }
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
                let selector = self.literal(ScalarValue::Utf8(Some(field)));
                expression = self.scalar("get_field", vec![expression, selector])?;
            }
            if inline {
                let parent = self.reference(&element)?;
                let is_null = self.is_null(parent);
                let null = self.literal(ScalarValue::try_from(
                    &expression.result_type.resolved("inline field")?.data_type,
                )?);
                expression = self.case(vec![(is_null, null)], Some(expression))?;
            }
            expressions.push(NamedExpression { name, expression });
        }
        Ok((input, expressions))
    }
}

/// Find generators inside a projection without crossing subquery or lambda scopes.
pub(super) fn nested_generators(expression: &spec::Expr, output: &mut Vec<spec::Expr>) {
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
        }
        _ => {}
    }
}
