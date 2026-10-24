//! Projected EXISTS is analyzed into scalar counts or mark joins before lowering.
use std::collections::{HashMap, HashSet};

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{JoinType, Operator};
use sail_query::rewrite::{QueryRewriter, walk_expression, walk_query};

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn projected_exists(
        &mut self,
        input: &mut QueryNode,
        expression: ExprNode,
    ) -> PlanResult<ExprNode> {
        expression.transform_up::<PlanError>(&mut |expression| {
            let ExprKind::Exists { query, negated } =
                expression.kind.resolved("projected expression")?
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
        input: &mut QueryNode,
        mut query: QueryNode,
    ) -> PlanResult<ExprNode> {
        if !contains_outer(&query)? {
            return self.exists_count(query, 0);
        }
        loop {
            query = match query.kind.into_resolved("EXISTS input")? {
                QueryKind::Project { input, .. }
                | QueryKind::Scope { input }
                | QueryKind::Sort { input, .. } => *input,
                QueryKind::Aggregate { ref grouping, .. } if grouping.is_empty() => {
                    return Ok(self.literal(ScalarValue::Boolean(Some(true))));
                }
                QueryKind::Limit { input, skip, fetch } => {
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
                    query.kind = Deferred::Resolved(node);
                    break;
                }
            };
        }
        validate_correlated(&query, false)?;
        let mut pull = PullCorrelation {
            resolver: self,
            predicates: vec![],
        };
        let query = pull.rewrite_query(query)?;
        let predicates = std::mem::take(&mut pull.predicates);
        let mut condition = None;
        for predicate in predicates {
            let predicate = predicate.transform_up::<PlanError>(&mut |mut value| {
                if let Deferred::Resolved(ExprKind::OuterAttribute { depth, attribute }) =
                    value.kind
                {
                    value.kind = Deferred::Resolved(if depth == 1 {
                        ExprKind::Attribute(attribute)
                    } else {
                        ExprKind::OuterAttribute {
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
        let mark = self.attribute("exists".into(), ExprType::new(DataType::Boolean, false));
        let mut schema = input.schema.resolved("EXISTS left")?.clone();
        schema.attributes.push(mark.clone());
        let left = std::mem::replace(input, Self::empty(false));
        *input = QueryNode {
            schema: Deferred::Resolved(schema),
            kind: Deferred::Resolved(QueryKind::Join {
                left: Box::new(left),
                right: Box::new(query),
                kind: JoinType::LeftMark,
                condition: condition.map(Box::new),
                lateral: false,
            }),
        };
        self.reference(&mark)
    }

    fn exists_count(&mut self, query: QueryNode, skip: i64) -> PlanResult<ExprNode> {
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
        let value = ExprNode {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::ScalarSubquery(Box::new(query))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, true)),
        };
        let no = self.literal(ScalarValue::Boolean(Some(false)));
        self.scalar("coalesce", vec![value, no])
    }
}

fn bound(value: &ExprNode) -> PlanResult<i64> {
    let value = crate::analysis::lowering::evaluate_constant(value)?.cast_to(&DataType::Int64)?;
    match value {
        ScalarValue::Int64(Some(value)) if value >= 0 => Ok(value),
        _ => Err(PlanError::analysis(
            "LIMIT/OFFSET must be a nonnegative constant",
        )),
    }
}

fn scalar_aggregate(plan: &QueryNode) -> PlanResult<bool> {
    Ok(match plan.kind.resolved("EXISTS scalar input")? {
        QueryKind::Project { input, .. }
        | QueryKind::Scope { input }
        | QueryKind::Sort { input, .. } => scalar_aggregate(input)?,
        QueryKind::Aggregate { grouping, .. } => grouping.is_empty(),
        _ => false,
    })
}

fn contains_outer(plan: &QueryNode) -> PlanResult<bool> {
    struct Find(bool);
    impl QueryRewriter for Find {
        type Error = PlanError;
        fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
            if matches!(
                expression.kind,
                Deferred::Resolved(ExprKind::OuterAttribute { .. })
            ) {
                self.0 = true;
            }
            walk_expression(self, expression)
        }
    }
    let mut find = Find(false);
    find.rewrite_query(plan.clone())?;
    Ok(find.0)
}

fn correlated_cast(plan: &QueryNode) -> PlanResult<bool> {
    struct Find(bool);
    impl QueryRewriter for Find {
        type Error = PlanError;
        fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
            if let Deferred::Resolved(QueryKind::Filter { condition, .. }) = &plan.kind {
                for predicate in conjunctions(*condition.clone())? {
                    if predicate.any(&|n| matches!(n, ExprKind::OuterAttribute { .. }))?
                        && predicate.any(&|n| matches!(n, ExprKind::Cast { expression, .. } if expression.any(&|n| matches!(n, ExprKind::Attribute(_))).unwrap_or(false)))? { self.0 = true; }
                }
            }
            walk_query(self, plan)
        }
    }
    let mut find = Find(false);
    find.rewrite_query(plan.clone())?;
    Ok(find.0)
}

fn validate_correlated(plan: &QueryNode, counting: bool) -> PlanResult<()> {
    if counting && correlated_cast(plan)? {
        return Err(PlanError::unsupported(
            "projected correlated EXISTS with cast correlation below aggregation",
        ));
    }
    struct Validate;
    impl QueryRewriter for Validate {
        type Error = PlanError;
        fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
            match plan.kind.resolved("correlated EXISTS operator")? {
                QueryKind::Window { input, .. } if contains_outer(input)? => {
                    return Err(PlanError::unsupported(
                        "projected correlated EXISTS with correlation below a window",
                    ));
                }
                QueryKind::Aggregate {
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
                QueryKind::Limit { input, skip, .. }
                    if contains_outer(input)?
                        || skip.as_deref().map(bound).transpose()?.unwrap_or(0) > 0 =>
                {
                    return Err(PlanError::unsupported(
                        "projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET",
                    ));
                }
                _ => {}
            }
            walk_query(self, plan)
        }
    }
    Validate.rewrite_query(plan.clone())?;
    Ok(())
}

fn conjunctions(value: ExprNode) -> PlanResult<Vec<ExprNode>> {
    match value.kind.resolved("EXISTS predicate")? {
        ExprKind::Binary {
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
    predicates: Vec<ExprNode>,
}
impl QueryRewriter for PullCorrelation<'_, '_, '_> {
    type Error = PlanError;
    fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
        Ok(expression)
    }
    fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
        let before = self.predicates.len();
        let mut plan = walk_query(self, plan)?;
        let mut schema = plan.schema.into_resolved("correlated output")?;
        let mut required = HashSet::new();
        for predicate in &self.predicates {
            predicate.clone().transform_up::<PlanError>(&mut |value| {
                if let Deferred::Resolved(ExprKind::Attribute(id)) = value.kind {
                    required.insert(id);
                }
                Ok(value)
            })?;
        }
        plan.kind = Deferred::Resolved(match plan.kind.into_resolved("correlated operator")? {
            QueryKind::Filter { input, condition } => {
                let mut local = None;
                for value in conjunctions(*condition)? {
                    if value.any(&|n| matches!(n, ExprKind::OuterAttribute { .. }))? {
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
                QueryKind::Filter {
                    input,
                    condition: Box::new(condition),
                }
            }
            QueryKind::Project {
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
                QueryKind::Project { input, expressions }
            }
            QueryKind::Aggregate {
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
                    let existing = grouping.iter().find(|p| matches!(p.expression.kind, Deferred::Resolved(ExprKind::Attribute(id)) if id == source.id));
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
                        if let Deferred::Resolved(ExprKind::Attribute(id)) = value.kind
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
                QueryKind::Aggregate {
                    input,
                    grouping,
                    grouping_sets,
                    grouping_id,
                    aggregates,
                }
            }
            QueryKind::Scope { input } => {
                schema = input.schema.resolved("correlated scope")?.clone();
                QueryKind::Scope { input }
            }
            QueryKind::Join { kind, .. }
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
