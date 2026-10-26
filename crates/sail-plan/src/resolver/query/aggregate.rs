use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn aggregate(
        &mut self,
        input: Query,
        grouping: Vec<NamedExpression>,
        output: Vec<NamedExpression>,
        having: Option<Expr>,
    ) -> PlanResult<Query> {
        self.aggregate_with_sets(input, grouping, output, having, None)
    }

    pub(in crate::resolver) fn aggregate_with_sets(
        &mut self,
        mut input: Query,
        mut grouping: Vec<NamedExpression>,
        mut output: Vec<NamedExpression>,
        mut having: Option<Expr>,
        grouping_sets: Option<Vec<Vec<usize>>>,
    ) -> PlanResult<Query> {
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
            let mut attribute = self
                .context
                .function_context()
                .attribute("grouping_id".into(), ExprType::new(kind, false));
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
        let schema = QuerySchema {
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
        let mut aggregate = Query {
            kind: Deferred::Resolved(QueryKind::Aggregate {
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
            aggregate = Query {
                kind: Deferred::Resolved(QueryKind::Filter {
                    input: Box::new(aggregate),
                    condition: Box::new(condition),
                }),
                schema: Deferred::Resolved(schema),
            };
        }
        self.project(aggregate, output)
    }

    pub(in crate::resolver) fn aggregate_expression(
        &mut self,
        expression: Expr,
        grouping: &[Projection],
        aggregates: &mut Vec<Projection>,
        grouping_id: Option<&Attribute>,
    ) -> PlanResult<Expr> {
        for key in grouping {
            if expression.equivalent(&key.expression)? {
                return self.reference(&key.attribute);
            }
        }
        if let ExprKind::AggregateFunction {
            function,
            arguments,
            distinct,
            filter,
            order_by,
            null_treatment,
        } = expression.kind.resolved("aggregate expression")?
            && matches!(function.name(), "grouping" | "grouping_id")
        {
            if *distinct || filter.is_some() || !order_by.is_empty() || null_treatment.is_some() {
                return Err(PlanError::invalid("invalid grouping function clause"));
            }
            return self.grouping_mask(function.name(), arguments, grouping, grouping_id);
        }
        match expression.kind.resolved("aggregate expression")? {
            ExprKind::AggregateFunction {
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
                            ExprKind::AggregateFunction { .. } | ExprKind::WindowFunction { .. }
                        )
                    })? {
                        return Err(PlanError::analysis(
                            "nested aggregate or window function inside aggregate",
                        ));
                    }
                }
                let attribute = self.context.function_context().attribute(
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
            ExprKind::HigherOrderFunction { .. } => {
                let mut expression = expression;
                let Deferred::Resolved(ExprKind::HigherOrderFunction { arguments, .. }) =
                    &mut expression.kind
                else {
                    unreachable!()
                };
                for argument in arguments {
                    let value = match argument {
                        FunctionArg::Value(value) => value,
                        FunctionArg::Lambda { body, .. } => body,
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
            ExprKind::ScalarSubquery(_) | ExprKind::Exists { .. } | ExprKind::InSubquery { .. } => {
                use sail_common_plan::rewrite::QueryRewriter;
                GroupOuter {
                    resolver: self,
                    grouping,
                    depth: 0,
                    fields: Default::default(),
                }
                .rewrite_expression(expression)
            }
            ExprKind::Attribute(_) => Err(PlanError::analysis(
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

fn is_invalid_volatile(node: &ExprKind) -> bool {
    matches!(node, ExprKind::ScalarFunction { function, .. }
        if function.signature().volatility == datafusion_expr::Volatility::Volatile && function.as_async().is_none()
        && !function.inner().is::<sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId>()
        && !function.inner().is::<sail_function::scalar::misc::spark_partition_id::SparkPartitionId>())
}

fn volatile_name(expression: &Expr) -> PlanResult<Option<String>> {
    let mut found = None;
    expression
        .clone()
        .transform_up::<PlanError>(&mut |expression| {
            if let Deferred::Resolved(node) = &expression.kind
                && is_invalid_volatile(node)
                && let ExprKind::ScalarFunction { function, .. } = node
            {
                found = Some(function.name().to_string());
            }
            Ok(expression)
        })?;
    Ok(found)
}

/// Correlation in a grouped projection refers to grouping outputs, rather than
/// the input slots that existed while the subquery was initially analyzed.
struct GroupOuter<'a, 'b, 'd> {
    resolver: &'a mut PlanResolver<'b>,
    grouping: &'d [Projection],
    depth: usize,
    fields: std::collections::HashMap<AttributeId, ExprType>,
}
impl sail_common_plan::rewrite::QueryRewriter for GroupOuter<'_, '_, '_> {
    type Error = PlanError;
    fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
        let boundary = matches!(
            expression.kind,
            Deferred::Resolved(
                ExprKind::ScalarSubquery(_) | ExprKind::Exists { .. } | ExprKind::InSubquery { .. }
            )
        );
        if !boundary
            && self.depth > 0
            && expression.any(
                &|n| matches!(n, ExprKind::OuterAttribute { depth, .. } if *depth == self.depth),
            )?
            && !expression.any(&|n| match n {
                ExprKind::Attribute(_) | ExprKind::LambdaVariable(_) => true,
                ExprKind::OuterAttribute { depth, .. } => *depth != self.depth,
                _ => false,
            })?
        {
            let local = expression
                .clone()
                .transform_up::<PlanError>(&mut |mut value| {
                    if let Deferred::Resolved(ExprKind::OuterAttribute { attribute, .. }) =
                        value.kind
                    {
                        value.kind = Deferred::Resolved(ExprKind::Attribute(attribute));
                    }
                    Ok(value)
                })?;
            for key in self.grouping {
                if local.equivalent(&key.expression)? {
                    return Ok(Expr {
                        id: expression.id,
                        kind: Deferred::Resolved(ExprKind::OuterAttribute {
                            depth: self.depth,
                            attribute: key.attribute.id,
                        }),
                        result_type: key.attribute.result_type.clone(),
                    });
                }
            }
        }
        if matches!(expression.kind, Deferred::Resolved(ExprKind::OuterAttribute { depth, .. }) if depth == self.depth && self.depth > 0)
        {
            return Err(PlanError::analysis(
                "correlated expression references a column that is not grouped or aggregated",
            ));
        }
        if boundary {
            self.depth += 1;
        }
        let value = sail_common_plan::rewrite::walk_expression(self, expression);
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
    fn rewrite_query(&mut self, plan: Query) -> PlanResult<Query> {
        let mut plan = sail_common_plan::rewrite::walk_query(self, plan)?;
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
