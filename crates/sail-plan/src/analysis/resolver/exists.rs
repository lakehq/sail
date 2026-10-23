//! Projected EXISTS is analyzed into scalar counts or mark joins before lowering.
use std::collections::{HashMap, HashSet};

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{JoinType, Operator};
use sail_query::rewrite::{QueryRewriter, walk_expression, walk_plan};

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn projected_exists(
        &mut self,
        input: &mut LogicalPlan,
        expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        expression.transform_up::<PlanError>(&mut |expression| {
            let ExpressionNode::Exists { query, negated } =
                expression.node.resolved("projected expression")?
            else {
                return Ok(expression);
            };
            let value = self.exists_value(input, *query.clone())?;
            let mut value = if *negated { self.not(value)? } else { value };
            value.id = expression.id;
            Ok(value)
        })
    }

    fn exists_value(
        &mut self,
        input: &mut LogicalPlan,
        mut query: LogicalPlan,
    ) -> PlanResult<LogicalExpression> {
        if !contains_outer(&query)? {
            return self.exists_count(query, 0);
        }
        loop {
            query = match query.node.into_resolved("EXISTS input")? {
                PlanNode::Project { input, .. }
                | PlanNode::Scope { input }
                | PlanNode::Sort { input, .. } => *input,
                PlanNode::Aggregate { ref grouping, .. } if grouping.is_empty() => {
                    return Ok(self.literal(ScalarValue::Boolean(Some(true))));
                }
                PlanNode::Limit { input, skip, fetch } => {
                    if fetch.as_deref().map(bound).transpose()? == Some(0) {
                        return Ok(self.literal(ScalarValue::Boolean(Some(false))));
                    }
                    let skip = skip.as_deref().map(bound).transpose()?.unwrap_or(0);
                    if skip > 0 {
                        if scalar_aggregate(&input)? {
                            return Ok(self.literal(ScalarValue::Boolean(Some(false))));
                        }
                        validate_correlated(&input, true)?;
                        return self.exists_count(*input, skip);
                    }
                    *input
                }
                node => {
                    query.node = Deferred::Resolved(node);
                    break;
                }
            };
        }
        validate_correlated(&query, false)?;
        let mut pull = PullCorrelation {
            resolver: self,
            predicates: vec![],
        };
        let query = pull.rewrite_plan(query)?;
        let predicates = std::mem::take(&mut pull.predicates);
        let mut condition = None;
        for predicate in predicates {
            let predicate = predicate.transform_up::<PlanError>(&mut |mut value| {
                if let Deferred::Resolved(ExpressionNode::OuterAttribute { depth, attribute }) =
                    value.node
                {
                    value.node = Deferred::Resolved(if depth == 1 {
                        ExpressionNode::Attribute(attribute)
                    } else {
                        ExpressionNode::OuterAttribute {
                            depth: depth - 1,
                            attribute,
                        }
                    });
                }
                Ok(value)
            })?;
            condition = Some(match condition {
                None => predicate,
                Some(previous) => self.binary(previous, Operator::And, predicate)?,
            });
        }
        let mark = self.attribute(
            "exists".into(),
            ExpressionType::new(DataType::Boolean, false),
        );
        let mut schema = input.schema.resolved("EXISTS left")?.clone();
        schema.attributes.push(mark.clone());
        let left = std::mem::replace(input, Self::empty(false));
        *input = LogicalPlan {
            schema: Deferred::Resolved(schema),
            node: Deferred::Resolved(PlanNode::Join {
                left: Box::new(left),
                right: Box::new(query),
                kind: JoinType::LeftMark,
                condition: condition.map(Box::new),
                lateral: false,
            }),
        };
        self.reference(&mark)
    }

    fn exists_count(&mut self, query: LogicalPlan, skip: i64) -> PlanResult<LogicalExpression> {
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let count = self.aggregate_call(self.aggregate_kernel("count")?, vec![one])?;
        let skip = self.literal(ScalarValue::Int64(Some(skip)));
        let expression = self.binary(count, Operator::Gt, skip)?;
        let query = self.aggregate(
            query,
            vec![],
            vec![NamedExpression {
                name: "exists".into(),
                expression,
            }],
            None,
        )?;
        let value = LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(ExpressionNode::ScalarSubquery(Box::new(query))),
            result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, true)),
        };
        let no = self.literal(ScalarValue::Boolean(Some(false)));
        self.scalar("coalesce", vec![value, no])
    }
}

fn bound(value: &LogicalExpression) -> PlanResult<i64> {
    let value = crate::analysis::lowering::evaluate_constant(value)?.cast_to(&DataType::Int64)?;
    match value {
        ScalarValue::Int64(Some(value)) if value >= 0 => Ok(value),
        _ => Err(PlanError::analysis(
            "LIMIT/OFFSET must be a nonnegative constant",
        )),
    }
}

fn scalar_aggregate(plan: &LogicalPlan) -> PlanResult<bool> {
    Ok(match plan.node.resolved("EXISTS scalar input")? {
        PlanNode::Project { input, .. }
        | PlanNode::Scope { input }
        | PlanNode::Sort { input, .. } => scalar_aggregate(input)?,
        PlanNode::Aggregate { grouping, .. } => grouping.is_empty(),
        _ => false,
    })
}

fn contains_outer(plan: &LogicalPlan) -> PlanResult<bool> {
    struct Find(bool);
    impl QueryRewriter for Find {
        type Error = PlanError;
        fn rewrite_expression(
            &mut self,
            expression: LogicalExpression,
        ) -> PlanResult<LogicalExpression> {
            if matches!(
                expression.node,
                Deferred::Resolved(ExpressionNode::OuterAttribute { .. })
            ) {
                self.0 = true;
            }
            walk_expression(self, expression)
        }
    }
    let mut find = Find(false);
    find.rewrite_plan(plan.clone())?;
    Ok(find.0)
}

fn correlated_cast(plan: &LogicalPlan) -> PlanResult<bool> {
    struct Find(bool);
    impl QueryRewriter for Find {
        type Error = PlanError;
        fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
            if let Deferred::Resolved(PlanNode::Filter { condition, .. }) = &plan.node {
                for predicate in conjunctions(*condition.clone())? {
                    if predicate.any(&|n| matches!(n, ExpressionNode::OuterAttribute { .. }))?
                        && predicate.any(&|n| matches!(n, ExpressionNode::Cast { expression, .. } if expression.any(&|n| matches!(n, ExpressionNode::Attribute(_))).unwrap_or(false)))? { self.0 = true; }
                }
            }
            walk_plan(self, plan)
        }
    }
    let mut find = Find(false);
    find.rewrite_plan(plan.clone())?;
    Ok(find.0)
}

fn validate_correlated(plan: &LogicalPlan, counting: bool) -> PlanResult<()> {
    if counting && correlated_cast(plan)? {
        return Err(PlanError::unsupported(
            "projected correlated EXISTS with cast correlation below aggregation",
        ));
    }
    struct Validate;
    impl QueryRewriter for Validate {
        type Error = PlanError;
        fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
            match plan.node.resolved("correlated EXISTS operator")? {
                PlanNode::Window { input, .. } if contains_outer(input)? => {
                    return Err(PlanError::unsupported(
                        "projected correlated EXISTS with correlation below a window",
                    ));
                }
                PlanNode::Aggregate {
                    input, grouping, ..
                } => {
                    if grouping.is_empty() {
                        return Err(PlanError::unsupported(
                            "projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET",
                        ));
                    }
                    if correlated_cast(input)? {
                        return Err(PlanError::unsupported(
                            "projected correlated EXISTS with cast correlation below aggregation",
                        ));
                    }
                }
                PlanNode::Limit { input, skip, .. }
                    if contains_outer(input)?
                        || skip.as_deref().map(bound).transpose()?.unwrap_or(0) > 0 =>
                {
                    return Err(PlanError::unsupported(
                        "projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET",
                    ));
                }
                _ => {}
            }
            walk_plan(self, plan)
        }
    }
    Validate.rewrite_plan(plan.clone())?;
    Ok(())
}

fn conjunctions(value: LogicalExpression) -> PlanResult<Vec<LogicalExpression>> {
    match value.node.resolved("EXISTS predicate")? {
        ExpressionNode::Binary {
            left,
            operator: Operator::And,
            right,
        } => {
            let mut values = conjunctions(*left.clone())?;
            values.extend(conjunctions(*right.clone())?);
            Ok(values)
        }
        _ => Ok(vec![value]),
    }
}

struct PullCorrelation<'a, 'b, 'c> {
    resolver: &'a mut Resolver<'b, 'c>,
    predicates: Vec<LogicalExpression>,
}
impl QueryRewriter for PullCorrelation<'_, '_, '_> {
    type Error = PlanError;
    fn rewrite_expression(
        &mut self,
        expression: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        Ok(expression)
    }
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        let before = self.predicates.len();
        let mut plan = walk_plan(self, plan)?;
        let mut schema = plan.schema.into_resolved("correlated output")?;
        let mut required = HashSet::new();
        for predicate in &self.predicates {
            predicate.clone().transform_up::<PlanError>(&mut |value| {
                if let Deferred::Resolved(ExpressionNode::Attribute(id)) = value.node {
                    required.insert(id);
                }
                Ok(value)
            })?;
        }
        plan.node = Deferred::Resolved(match plan.node.into_resolved("correlated operator")? {
            PlanNode::Filter { input, condition } => {
                let mut local = None;
                for value in conjunctions(*condition)? {
                    if value.any(&|n| matches!(n, ExpressionNode::OuterAttribute { .. }))? {
                        self.predicates.push(value);
                    } else {
                        local = Some(match local {
                            None => value,
                            Some(previous) => {
                                self.resolver.binary(previous, Operator::And, value)?
                            }
                        });
                    }
                }
                let Some(condition) = local else {
                    return Ok(*input);
                };
                schema = input.schema.resolved("correlated filter")?.clone();
                PlanNode::Filter {
                    input,
                    condition: Box::new(condition),
                }
            }
            PlanNode::Project {
                input,
                mut expressions,
            } => {
                for attribute in &input.schema.resolved("correlated projection")?.attributes {
                    if required.contains(&attribute.id)
                        && !schema.attributes.iter().any(|a| a.id == attribute.id)
                    {
                        expressions.push(Projection {
                            expression: self.resolver.reference(attribute)?,
                            attribute: attribute.clone(),
                        });
                        schema.attributes.push(attribute.clone());
                    }
                }
                PlanNode::Project { input, expressions }
            }
            PlanNode::Aggregate {
                input,
                mut grouping,
                mut grouping_sets,
                grouping_id,
                aggregates,
            } => {
                let mut mapping = HashMap::new();
                for source in &input.schema.resolved("correlated aggregate")?.attributes {
                    if !required.contains(&source.id) {
                        continue;
                    }
                    let existing = grouping.iter().find(|p| matches!(p.expression.node, Deferred::Resolved(ExpressionNode::Attribute(id)) if id == source.id));
                    let attribute = if let Some(existing) = existing {
                        existing.attribute.clone()
                    } else {
                        let attribute = self.resolver.attribute(
                            source.name.clone(),
                            source.result_type.resolved("correlation key")?.clone(),
                        );
                        if let Some(sets) = &mut grouping_sets {
                            for set in sets {
                                set.push(grouping.len());
                            }
                        }
                        grouping.push(Projection {
                            expression: self.resolver.reference(source)?,
                            attribute: attribute.clone(),
                        });
                        attribute
                    };
                    mapping.insert(source.id, attribute);
                }
                for predicate in &mut self.predicates {
                    *predicate = predicate.clone().transform_up::<PlanError>(&mut |value| {
                        if let Deferred::Resolved(ExpressionNode::Attribute(id)) = value.node
                            && let Some(attribute) = mapping.get(&id)
                        {
                            return self.resolver.reference(attribute);
                        }
                        Ok(value)
                    })?;
                }
                schema.attributes = grouping
                    .iter()
                    .map(|p| p.attribute.clone())
                    .chain(grouping_id.iter().cloned())
                    .chain(aggregates.iter().map(|p| p.attribute.clone()))
                    .collect();
                PlanNode::Aggregate {
                    input,
                    grouping,
                    grouping_sets,
                    grouping_id,
                    aggregates,
                }
            }
            PlanNode::Scope { input } => {
                schema = input.schema.resolved("correlated scope")?.clone();
                PlanNode::Scope { input }
            }
            PlanNode::Join { kind, .. }
                if self.predicates.len() > before && kind != JoinType::Inner =>
            {
                return Err(PlanError::unsupported(
                    "projected EXISTS with unsupported correlation",
                ));
            }
            node => node,
        });
        plan.schema = Deferred::Resolved(schema);
        Ok(plan)
    }
}
