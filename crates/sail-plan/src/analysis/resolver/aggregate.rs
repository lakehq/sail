use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn aggregate(
        &mut self,
        input: LogicalPlan,
        grouping: Vec<NamedExpression>,
        output: Vec<NamedExpression>,
        having: Option<LogicalExpression>,
    ) -> PlanResult<LogicalPlan> {
        self.aggregate_with_sets(input, grouping, output, having, None)
    }

    pub(super) fn aggregate_with_sets(
        &mut self,
        mut input: LogicalPlan,
        mut grouping: Vec<NamedExpression>,
        mut output: Vec<NamedExpression>,
        mut having: Option<LogicalExpression>,
        grouping_sets: Option<Vec<Vec<usize>>>,
    ) -> PlanResult<LogicalPlan> {
        self.grouping_metadata(&mut input, &mut grouping, &mut output, &mut having)?;
        let schema = input.schema.resolved("aggregate input")?;
        let mut grouping = grouping
            .into_iter()
            .map(|expression| self.projection(expression, schema))
            .collect::<PlanResult<Vec<_>>>()?;
        let grouping_id = if let Some(sets) = &grouping_sets {
            let mut counts = std::collections::HashMap::new();
            for set in sets {
                *counts.entry(set).or_insert(0usize) += 1;
            }
            let duplicates = counts.into_values().max().unwrap_or(1) - 1;
            let kind = datafusion_expr::Aggregate::grouping_id_type(grouping.len(), duplicates);
            let mut attribute =
                self.attribute("grouping_id".into(), ExpressionType::new(kind, false));
            attribute.hidden = true;
            for key in &mut grouping {
                let mut field = key.attribute.result_type.resolved("grouping type")?.clone();
                field.nullable = true;
                key.attribute.result_type = Deferred::Resolved(field);
            }
            Some(attribute)
        } else {
            None
        };
        let mut aggregates = vec![];
        let output = output
            .into_iter()
            .map(|expression| {
                Ok(NamedExpression {
                    name: expression.name,
                    expression: self.aggregate_expression(
                        expression.expression,
                        &grouping,
                        &mut aggregates,
                        grouping_id.as_ref(),
                    )?,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let having = having
            .map(|expression| {
                self.aggregate_expression(
                    expression,
                    &grouping,
                    &mut aggregates,
                    grouping_id.as_ref(),
                )
            })
            .transpose()?;
        let schema = PlanSchema {
            attributes: grouping
                .iter()
                .map(|projection| projection.attribute.clone())
                .chain(grouping_id.iter().cloned())
                .chain(
                    aggregates
                        .iter()
                        .map(|projection| projection.attribute.clone()),
                )
                .collect(),
        };
        let mut aggregate = LogicalPlan {
            node: Deferred::Resolved(PlanNode::Aggregate {
                input: Box::new(input),
                grouping,
                grouping_sets,
                grouping_id,
                aggregates,
            }),
            schema: Deferred::Resolved(schema.clone()),
        };
        if let Some(having) = having {
            let condition = typing::coerce(
                having,
                &arrow_schema::DataType::Boolean,
                &mut self.context.ids,
            )?;
            aggregate = LogicalPlan {
                node: Deferred::Resolved(PlanNode::Filter {
                    input: Box::new(aggregate),
                    condition: Box::new(condition),
                }),
                schema: Deferred::Resolved(schema),
            };
        }
        self.project(aggregate, output)
    }

    pub(super) fn aggregate_expression(
        &mut self,
        expression: LogicalExpression,
        grouping: &[Projection],
        aggregates: &mut Vec<Projection>,
        grouping_id: Option<&Attribute>,
    ) -> PlanResult<LogicalExpression> {
        for key in grouping {
            if expression.equivalent(&key.expression)? {
                return self.reference(&key.attribute);
            }
        }
        if let ExpressionNode::AggregateFunction {
            function,
            arguments,
            distinct,
            filter,
            order_by,
            null_treatment,
        } = expression.node.resolved("aggregate expression")?
            && matches!(function.name(), "grouping" | "grouping_id")
        {
            if *distinct || filter.is_some() || !order_by.is_empty() || null_treatment.is_some() {
                return Err(PlanError::invalid("invalid grouping function clause"));
            }
            return self.grouping_mask(function.name(), arguments, grouping, grouping_id);
        }
        match expression.node.resolved("aggregate expression")? {
            ExpressionNode::AggregateFunction {
                arguments,
                filter,
                order_by,
                ..
            } => {
                for argument in arguments {
                    if let Some(name) = volatile_name(argument)? {
                        return Err(PlanError::analysis(format!(
                            "Non-deterministic expression {name} should not appear in an aggregate query"
                        )));
                    }
                }
                for argument in arguments
                    .iter()
                    .chain(filter.iter().map(Box::as_ref))
                    .chain(order_by.iter().map(|sort| &sort.expression))
                {
                    if argument.any(&|node| {
                        matches!(
                            node,
                            ExpressionNode::AggregateFunction { .. }
                                | ExpressionNode::WindowFunction { .. }
                        )
                    })? {
                        return Err(PlanError::analysis(
                            "nested aggregate or window function inside aggregate",
                        ));
                    }
                }
                let attribute = self.attribute(
                    format!("aggregate_{}", aggregates.len()),
                    expression
                        .result_type
                        .resolved("aggregate result type")?
                        .clone(),
                );
                let reference = self.reference(&attribute)?;
                aggregates.push(Projection {
                    expression,
                    attribute,
                });
                Ok(reference)
            }
            ExpressionNode::HigherOrderFunction { .. } => {
                let mut expression = expression;
                let Deferred::Resolved(ExpressionNode::HigherOrderFunction { arguments, .. }) =
                    &mut expression.node
                else {
                    unreachable!()
                };
                for argument in arguments {
                    let value = match argument {
                        FunctionArgument::Value(value) => value,
                        FunctionArgument::Lambda { body, .. } => body,
                    };
                    **value = self.aggregate_expression(
                        *value.clone(),
                        grouping,
                        aggregates,
                        grouping_id,
                    )?;
                }
                self.retype(expression)
            }
            ExpressionNode::ScalarSubquery(_)
            | ExpressionNode::Exists { .. }
            | ExpressionNode::InSubquery { .. } => {
                use sail_query::rewrite::QueryRewriter;
                GroupOuter {
                    resolver: self,
                    grouping,
                    depth: 0,
                    fields: Default::default(),
                }
                .rewrite_expression(expression)
            }
            ExpressionNode::Attribute(_) => Err(PlanError::analysis(
                "expression references a column that is not grouped or aggregated",
            )),
            node if is_invalid_volatile(node) => Err(PlanError::analysis(
                "Non-deterministic expression should not appear in an aggregate query",
            )),
            _ => {
                let expression = expression.map_children(&mut |expression| {
                    self.aggregate_expression(expression, grouping, aggregates, grouping_id)
                })?;
                self.retype(expression)
            }
        }
    }
}

fn is_invalid_volatile(node: &ExpressionNode) -> bool {
    matches!(node, ExpressionNode::ScalarFunction { function, .. }
        if function.signature().volatility == datafusion_expr::Volatility::Volatile && function.as_async().is_none()
        && !function.inner().is::<sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId>()
        && !function.inner().is::<sail_function::scalar::misc::spark_partition_id::SparkPartitionId>())
}

fn volatile_name(expression: &LogicalExpression) -> PlanResult<Option<String>> {
    let mut found = None;
    expression
        .clone()
        .transform_up::<PlanError>(&mut |expression| {
            if let Deferred::Resolved(node) = &expression.node
                && is_invalid_volatile(node)
                && let ExpressionNode::ScalarFunction { function, .. } = node
            {
                found = Some(function.name().to_string());
            }
            Ok(expression)
        })?;
    Ok(found)
}

/// Correlation in a grouped projection refers to grouping outputs, rather than
/// the input slots that existed while the subquery was initially analyzed.
struct GroupOuter<'a, 'b, 'c, 'd> {
    resolver: &'a mut Resolver<'b, 'c>,
    grouping: &'d [Projection],
    depth: usize,
    fields: std::collections::HashMap<AttributeId, ExpressionType>,
}
impl sail_query::rewrite::QueryRewriter for GroupOuter<'_, '_, '_, '_> {
    type Error = PlanError;
    fn rewrite_expression(
        &mut self,
        expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        let boundary = matches!(
            expression.node,
            Deferred::Resolved(
                ExpressionNode::ScalarSubquery(_)
                    | ExpressionNode::Exists { .. }
                    | ExpressionNode::InSubquery { .. }
            )
        );
        if !boundary && self.depth > 0 && expression.any(&|n| matches!(n, ExpressionNode::OuterAttribute { depth, .. } if *depth == self.depth))?
            && !expression.any(&|n| match n { ExpressionNode::Attribute(_) | ExpressionNode::LambdaVariable(_) => true, ExpressionNode::OuterAttribute { depth, .. } => *depth != self.depth, _ => false })? {
            let local = expression.clone().transform_up::<PlanError>(&mut |mut value| {
                if let Deferred::Resolved(ExpressionNode::OuterAttribute { attribute, .. }) = value.node { value.node = Deferred::Resolved(ExpressionNode::Attribute(attribute)); }
                Ok(value)
            })?;
            for key in self.grouping {
                if local.equivalent(&key.expression)? {
                    return Ok(LogicalExpression { id: expression.id, node: Deferred::Resolved(ExpressionNode::OuterAttribute { depth: self.depth, attribute: key.attribute.id }), result_type: key.attribute.result_type.clone() });
                }
            }
        }
        if matches!(expression.node, Deferred::Resolved(ExpressionNode::OuterAttribute { depth, .. }) if depth == self.depth && self.depth > 0)
        {
            return Err(PlanError::analysis(
                "correlated expression references a column that is not grouped or aggregated",
            ));
        }
        if boundary {
            self.depth += 1;
        }
        let value = sail_query::rewrite::walk_expression(self, expression);
        if boundary {
            self.depth -= 1;
        }
        self.resolver.retype(value?)
    }
    fn rewrite_projections(&mut self, projections: Vec<Projection>) -> PlanResult<Vec<Projection>> {
        projections
            .into_iter()
            .map(|mut projection| {
                projection.expression = self.rewrite_expression(projection.expression)?;
                projection.attribute.result_type = projection.expression.result_type.clone();
                self.fields.insert(
                    projection.attribute.id,
                    projection
                        .attribute
                        .result_type
                        .resolved("grouped correlation projection")?
                        .clone(),
                );
                Ok(projection)
            })
            .collect()
    }
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        let mut plan = sail_query::rewrite::walk_plan(self, plan)?;
        if let Deferred::Resolved(schema) = &mut plan.schema {
            for attribute in &mut schema.attributes {
                if let Some(field) = self.fields.get(&attribute.id) {
                    attribute.result_type = Deferred::Resolved(field.clone());
                }
            }
        }
        Ok(plan)
    }
}
