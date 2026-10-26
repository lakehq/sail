use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::{DataType, FieldRef};
use datafusion_expr::{HigherOrderUDF, LambdaParametersProgress, ValueOrLambda};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

enum Slot {
    Value(NamedExpression),
    Lambda {
        request: spec::Expr,
        captured: Option<Box<NamedExpression>>,
        names: Vec<String>,
        parameters: Option<QuerySchema>,
        body: Option<Box<NamedExpression>>,
    },
}

impl Slot {
    fn field(&self) -> PlanResult<ValueOrLambda<FieldRef, Option<FieldRef>>> {
        Ok(match self {
            Self::Value(value) => ValueOrLambda::Value(
                value
                    .expression
                    .result_type
                    .resolved("higher-order argument type")?
                    .to_field(""),
            ),
            Self::Lambda { body, .. } => ValueOrLambda::Lambda(
                body.as_ref()
                    .map(|body| {
                        body.expression
                            .result_type
                            .resolved("lambda result type")
                            .map(|field| field.to_field(""))
                    })
                    .transpose()?,
            ),
        })
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn lambda_variable(
        &mut self,
        id: ExprId,
        name: &[String],
    ) -> PlanResult<Option<NamedExpression>> {
        let Some((first, fields)) = name.split_first() else {
            return Ok(None);
        };
        let Some(attribute) = self
            .lambdas
            .iter()
            .rev()
            .flat_map(|scope| scope.attributes.iter())
            .find(|attribute| self.name_key(&attribute.name) == self.name_key(first))
            .cloned()
        else {
            return Ok(None);
        };
        let mut value = NamedExpression {
            name: attribute.name.clone(),
            expression: Expr {
                id: if fields.is_empty() {
                    id
                } else {
                    self.context.ids.expression()
                },
                kind: Deferred::Resolved(ExprKind::LambdaVariable(attribute.id)),
                result_type: attribute.result_type,
            },
        };
        for (i, field) in fields.iter().enumerate() {
            let selected_id = if i + 1 == fields.len() {
                id
            } else {
                self.context.ids.expression()
            };
            let extraction = NamedExpression {
                name: field.clone(),
                expression: Expr::literal(
                    self.context.ids.expression(),
                    datafusion_common::ScalarValue::Utf8(Some(field.clone())),
                ),
            };
            value = self.extract_value(selected_id, value, extraction)?;
        }
        Ok(Some(value))
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn higher_order(
        &mut self,
        id: ExprId,
        name: String,
        mut function: Arc<HigherOrderUDF>,
        arguments: Vec<spec::Expr>,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpression> {
        let mut slots = vec![];
        let mut implicit_map_lambda = false;
        for (index, argument) in arguments.into_iter().enumerate() {
            let argument = unwrap_alias(argument);
            match argument {
                spec::Expr::LambdaFunction {
                    function,
                    arguments,
                } => {
                    let names = arguments
                        .into_iter()
                        .map(|argument| Vec::<String>::from(argument.name).one())
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut seen = HashSet::new();
                    if names.is_empty()
                        || names
                            .iter()
                            .any(|name| !seen.insert(name.to_ascii_lowercase()))
                    {
                        return Err(PlanError::analysis(
                            "lambda parameters must be nonempty and distinct",
                        ));
                    }
                    slots.push(Slot::Lambda {
                        request: *function,
                        captured: None,
                        names,
                        parameters: None,
                        body: None,
                    });
                }
                expression
                    if index == 1
                        && matches!(
                            name.as_str(),
                            "map_filter" | "transform_keys" | "transform_values"
                        ) =>
                {
                    let resolved = self
                        .resolve_named_expression(expression.clone(), schema)
                        .await?;
                    implicit_map_lambda = true;
                    slots.push(Slot::Lambda {
                        request: expression,
                        captured: Some(Box::new(resolved)),
                        names: vec!["__map_key".into(), "__map_value".into()],
                        parameters: None,
                        body: None,
                    });
                }
                expression => slots.push(Slot::Value(
                    self.resolve_named_expression(expression, schema).await?,
                )),
            }
        }
        if implicit_map_lambda
            && let Some(Slot::Value(value)) = slots.first_mut()
            && value
                .expression
                .result_type
                .resolved("map input")?
                .data_type
                == DataType::Null
        {
            let kind = sail_function::scalar::map::utils::map_type_from_key_value_types(
                &DataType::Null,
                &DataType::Null,
            );
            value.expression = self
                .context
                .function_context()
                .literal(datafusion_common::ScalarValue::try_from(&kind)?);
        }
        for (index, slot) in slots.iter().enumerate() {
            if let Slot::Lambda { names, .. } = slot {
                let arity = match (name.as_str(), index) {
                    ("aggregate" | "reduce", 2) | ("array_sort", 1) => Some(2),
                    ("aggregate" | "reduce", 3) => Some(1),
                    _ => None,
                };
                if let Some(arity) = arity
                    && names.len() != arity
                {
                    return Err(PlanError::analysis(format!(
                        "{name} lambda function argument {index} requires {arity} parameters, got {}",
                        names.len()
                    )));
                }
            }
        }
        let fields = slots
            .iter()
            .map(Slot::field)
            .collect::<PlanResult<Vec<_>>>()?;
        let targets =
            crate::analyzer::typing::higher_order::value_fields(function.as_ref(), &fields)?;
        for (slot, target) in slots.iter_mut().zip(targets) {
            if let (Slot::Value(value), ValueOrLambda::Value(target)) = (slot, target) {
                value.expression = typing::coerce(
                    value.expression.clone(),
                    target.data_type(),
                    &mut self.context.ids,
                )?;
            }
        }
        for step in 0..100 {
            let fields = slots
                .iter()
                .map(Slot::field)
                .collect::<PlanResult<Vec<_>>>()?;
            let (complete, parameters) = match function.lambda_parameters(step, &fields)? {
                LambdaParametersProgress::Complete(parameters) => {
                    (true, parameters.into_iter().map(Some).collect::<Vec<_>>())
                }
                LambdaParametersProgress::Partial(parameters) => (false, parameters),
            };
            let lambda_count = slots
                .iter()
                .filter(|slot| matches!(slot, Slot::Lambda { .. }))
                .count();
            crate::analyzer::typing::coercion::validate_arity(
                "lambda parameter inference",
                parameters.len(),
                lambda_count,
            )?;
            let mut parameters = parameters.into_iter();
            for slot in &mut slots {
                let Slot::Lambda {
                    request,
                    captured,
                    names,
                    parameters: scope,
                    body,
                } = slot
                else {
                    continue;
                };
                let Some(fields) = parameters.next().flatten() else {
                    continue;
                };
                if names.len() > fields.len() {
                    return Err(PlanError::analysis(format!(
                        "{name} supports at most {} lambda parameters, got {}",
                        fields.len(),
                        names.len()
                    )));
                }
                if matches!(
                    name.as_str(),
                    "map_filter" | "transform_keys" | "transform_values"
                ) && names.len() != 2
                {
                    return Err(PlanError::analysis(format!(
                        "{name} requires two lambda parameters"
                    )));
                }
                let parameters = QuerySchema {
                    attributes: names
                        .iter()
                        .zip(fields)
                        .map(|(name, field)| {
                            self.context
                                .function_context()
                                .attribute(name.clone(), ExprType::from_field(&field))
                        })
                        .collect(),
                };
                self.lambdas.push(parameters.clone());
                let replacements = std::mem::take(&mut self.expression_replacements);
                let discarded = self.discarded_predicate;
                self.discarded_predicate |= name == "map_filter" && null_result(request);
                let result = if let Some(captured) = captured {
                    self.context
                        .function_context()
                        .copy_expression(&captured.expression)
                        .map(|expression| NamedExpression {
                            name: captured.name.clone(),
                            expression,
                        })
                } else {
                    self.resolve_named_expression(request.clone(), schema).await
                };
                self.lambdas.pop();
                self.expression_replacements = replacements;
                self.discarded_predicate = discarded;
                let mut resolved = result?;
                // Spark discards a VOID map predicate after binding its inputs.
                // Nested predicate return checks are irrelevant in that discarded
                // tree, but attribute binding and explicit casts still validate.
                if discarded && name == "map_filter" {
                    resolved.expression = self
                        .context
                        .function_context()
                        .literal(datafusion_common::ScalarValue::Boolean(None));
                }
                if matches!(name.as_str(), "filter" | "exists" | "forall" | "map_filter") {
                    if !matches!(
                        resolved
                            .expression
                            .result_type
                            .resolved("predicate lambda result")?
                            .data_type,
                        DataType::Boolean | DataType::Null
                    ) {
                        return Err(PlanError::analysis(format!(
                            "{name} lambda must return BOOLEAN"
                        )));
                    }
                    resolved.expression = if name == "map_filter"
                        && resolved
                            .expression
                            .result_type
                            .resolved("map predicate")?
                            .data_type
                            == DataType::Null
                    {
                        self.context
                            .function_context()
                            .literal(datafusion_common::ScalarValue::Boolean(None))
                    } else {
                        typing::coerce(
                            resolved.expression,
                            &DataType::Boolean,
                            &mut self.context.ids,
                        )?
                    };
                }
                *scope = Some(parameters);
                *body = Some(Box::new(resolved));
            }
            if !complete {
                continue;
            }
            let fields = slots
                .iter()
                .map(|slot| {
                    Ok(match slot.field()? {
                        ValueOrLambda::Value(field) => ValueOrLambda::Value(field),
                        ValueOrLambda::Lambda(Some(field)) => ValueOrLambda::Lambda(field),
                        ValueOrLambda::Lambda(None) => {
                            return Err(PlanError::analysis("incomplete lambda result inference"));
                        }
                    })
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let literals = slots
                .iter()
                .map(|slot| match slot {
                    Slot::Value(value) => {
                        match value.expression.kind.resolved("higher-order argument")? {
                            ExprKind::Literal(value) => Ok(Some(value)),
                            _ => Ok(None),
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let result = crate::analyzer::typing::higher_order::resolve(
                function.as_ref(),
                &fields,
                &literals,
            )?;
            let changed = fields.iter().zip(&result.arguments).any(|(source, target)| matches!((source, target), (ValueOrLambda::Value(a), ValueOrLambda::Value(b)) if a.data_type() != b.data_type()));
            if changed {
                for (slot, target) in slots.iter_mut().zip(result.arguments) {
                    if let (Slot::Value(value), ValueOrLambda::Value(target)) = (slot, target) {
                        value.expression = typing::coerce(
                            value.expression.clone(),
                            target.data_type(),
                            &mut self.context.ids,
                        )?;
                    }
                }
                continue;
            }
            let mut names = vec![];
            let mut arguments = vec![];
            for (index, slot) in slots.into_iter().enumerate() {
                match slot {
                    Slot::Value(value) => {
                        names.push(value.name);
                        arguments.push(FunctionArg::Value(Box::new(value.expression)));
                    }
                    Slot::Lambda {
                        names: parameters_names,
                        parameters: Some(mut parameters),
                        body: Some(body),
                        ..
                    } => {
                        names.push(format!(
                            "lambdafunction({}, {})",
                            body.name,
                            parameters_names.join(", ")
                        ));
                        // Physical lambda evaluation only materializes used
                        // parameter columns. Choose the existing index-first
                        // function when an array lambda captures only its index.
                        if (matches!(name.as_str(), "filter" | "transform" | "array_sort")
                            || (matches!(name.as_str(), "aggregate" | "reduce") && index == 2))
                            && parameters.attributes.len() == 2
                        {
                            let first = parameters.attributes[0].id;
                            let second = parameters.attributes[1].id;
                            if !references(&body.expression, first)?
                                && references(&body.expression, second)?
                            {
                                parameters.attributes.remove(0);
                                function = Arc::new(if name == "filter" {
                                    HigherOrderUDF::new_from_impl(sail_function::scalar::array::spark_array_filter::SparkArrayFilter::new_index_first())
                                } else if name == "transform" {
                                    HigherOrderUDF::new_from_impl(sail_function::scalar::array::spark_array_transform::SparkArrayTransform::new_index_first())
                                } else if name == "array_sort" {
                                    HigherOrderUDF::new_from_impl(sail_function::scalar::array::spark_array_sort::SparkArraySort::new_swapped())
                                } else {
                                    HigherOrderUDF::new_from_impl(sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate::new_element_first())
                                });
                            }
                        }
                        arguments.push(FunctionArg::Lambda {
                            id: self.context.ids.expression(),
                            parameters,
                            body: Box::new(body.expression),
                        });
                    }
                    _ => {
                        return Err(PlanError::analysis(
                            "incomplete lambda at analysis boundary",
                        ));
                    }
                }
            }
            let display_name = format!("{name}({})", names.join(", "));
            return Ok(NamedExpression {
                name: display_name,
                expression: Expr {
                    id,
                    kind: Deferred::Resolved(ExprKind::HigherOrderFunction {
                        function,
                        arguments,
                    }),
                    result_type: Deferred::Resolved(ExprType::from_field(&result.result)),
                },
            });
        }
        Err(PlanError::analysis(format!(
            "lambda type inference for {name} did not converge"
        )))
    }
}

fn unwrap_alias(expression: spec::Expr) -> spec::Expr {
    match expression {
        spec::Expr::Alias { expr, .. }
            if matches!(expr.as_ref(), spec::Expr::LambdaFunction { .. }) =>
        {
            *expr
        }
        expression => expression,
    }
}

fn references(expression: &Expr, id: AttributeId) -> PlanResult<bool> {
    let node = expression.kind.resolved("lambda capture")?;
    if matches!(node, ExprKind::LambdaVariable(variable) if *variable == id) {
        return Ok(true);
    }
    if let ExprKind::HigherOrderFunction { arguments, .. } = node {
        for argument in arguments {
            if let FunctionArg::Lambda { body, .. } = argument
                && references(body, id)?
            {
                return Ok(true);
            }
        }
    }
    let mut found = false;
    expression
        .clone()
        .map_children::<crate::error::PlanError>(&mut |child| {
            found |= references(&child, id)?;
            Ok(child)
        })?;
    Ok(found)
}

pub(in crate::resolver) fn is_lambda(expression: &spec::Expr) -> bool {
    match expression {
        spec::Expr::LambdaFunction { .. } => true,
        spec::Expr::Alias { expr, .. } => is_lambda(expr),
        _ => false,
    }
}

/// Recognize a statically VOID result without skipping input validation.
fn null_result(request: &spec::Expr) -> bool {
    match request {
        spec::Expr::Literal(spec::Literal::Null) => true,
        spec::Expr::Alias { expr, .. } => null_result(expr),
        spec::Expr::Cast {
            cast_to_type: spec::DataType::Null,
            ..
        } => true,
        spec::Expr::UnresolvedFunction(function) if matches!(function.function_name.parts(), [name] if matches!(name.as_ref().to_ascii_lowercase().as_str(), "case" | "when")) =>
        {
            let arguments = &function.arguments;
            !arguments.is_empty()
                && arguments.iter().skip(1).step_by(2).all(null_result)
                && (arguments.len().is_multiple_of(2) || arguments.last().is_some_and(null_result))
        }
        _ => false,
    }
}
