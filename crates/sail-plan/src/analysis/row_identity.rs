//! Materialize per-row identity functions before backend lowering.
use std::collections::HashMap;

use sail_query::rewrite::{QueryRewriter, walk_expression, walk_query};

use super::deferred::Deferred;
use super::model::*;
use crate::error::{PlanError, PlanResult};

pub(super) fn materialize(plan: QueryNode, ids: &mut IdGenerator) -> PlanResult<QueryNode> {
    Materialize { ids }.rewrite_query(plan)
}

struct Materialize<'a> {
    ids: &'a mut IdGenerator,
}

impl QueryRewriter for Materialize<'_> {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
        let plan = walk_query(self, plan)?;
        let original = plan.schema.resolved("row identity output")?.clone();
        let mut extractor = Extract {
            ids: self.ids,
            attributes: HashMap::new(),
        };
        let node = match plan.kind.into_resolved("row identity plan")? {
            QueryKind::Project { input, expressions } => {
                let expressions = extractor.rewrite_projections(expressions)?;
                QueryKind::Project {
                    input: Box::new(extractor.input(*input)?),
                    expressions,
                }
            }
            QueryKind::Aggregate {
                input,
                grouping,
                grouping_sets,
                grouping_id,
                aggregates,
            } => {
                let grouping = extractor.rewrite_projections(grouping)?;
                let aggregates = extractor.rewrite_projections(aggregates)?;
                QueryKind::Aggregate {
                    input: Box::new(extractor.input(*input)?),
                    grouping,
                    grouping_sets,
                    grouping_id,
                    aggregates,
                }
            }
            QueryKind::Window { input, expressions } => {
                let expressions = extractor.rewrite_projections(expressions)?;
                QueryKind::Window {
                    input: Box::new(extractor.input(*input)?),
                    expressions,
                }
            }
            QueryKind::Filter { input, condition } => {
                let condition = Box::new(extractor.rewrite_expression(*condition)?);
                QueryKind::Filter {
                    input: Box::new(extractor.input(*input)?),
                    condition,
                }
            }
            QueryKind::Sort {
                input,
                order,
                global,
                required,
            } => {
                let order = extractor.rewrite_sorts(order)?;
                QueryKind::Sort {
                    input: Box::new(extractor.input(*input)?),
                    order,
                    global,
                    required,
                }
            }
            QueryKind::DistinctOn { input, keys } => {
                let keys = keys
                    .into_iter()
                    .map(|e| extractor.rewrite_expression(e))
                    .collect::<PlanResult<_>>()?;
                QueryKind::DistinctOn {
                    input: Box::new(extractor.input(*input)?),
                    keys,
                }
            }
            node => node,
        };
        // Operators that preserve their input also carry the internal slots.
        // Remove these slots at the operator boundary by identity projection.
        let additions = match &node {
            QueryKind::Window { input, .. }
            | QueryKind::Filter { input, .. }
            | QueryKind::Sort { input, .. }
            | QueryKind::DistinctOn { input, .. } => input
                .schema
                .resolved("row identity input")?
                .attributes
                .iter()
                .filter(|a| extractor.attributes.values().any(|x| x.id == a.id))
                .cloned()
                .collect::<Vec<_>>(),
            _ => vec![],
        };
        if additions.is_empty() {
            return Ok(QueryNode {
                kind: Deferred::Resolved(node),
                schema: Deferred::Resolved(original),
            });
        }
        let mut schema = original.clone();
        if let QueryKind::Window { input, expressions } = &node {
            schema = input.schema.resolved("window input")?.clone();
            schema
                .attributes
                .extend(expressions.iter().map(|p| p.attribute.clone()));
        } else {
            schema.attributes.extend(additions);
        }
        let input = QueryNode {
            kind: Deferred::Resolved(node),
            schema: Deferred::Resolved(schema),
        };
        let expressions = original
            .attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: ExprNode {
                        id: extractor.ids.expression(),
                        kind: Deferred::Resolved(ExprKind::Attribute(attribute.id)),
                        result_type: attribute.result_type.clone(),
                    },
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(QueryNode {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(original),
        })
    }
}

struct Extract<'a> {
    ids: &'a mut IdGenerator,
    attributes: HashMap<RowIdentityKind, Attribute>,
}

impl Extract<'_> {
    fn input(&self, mut input: QueryNode) -> PlanResult<QueryNode> {
        // Stable order also makes explain output deterministic.
        for kind in [RowIdentityKind::Monotonic, RowIdentityKind::Partition] {
            if let Some(attribute) = self.attributes.get(&kind) {
                let mut schema = input.schema.resolved("row identity input")?.clone();
                schema.attributes.push(attribute.clone());
                input = QueryNode {
                    kind: Deferred::Resolved(QueryKind::RowIdentity {
                        input: Box::new(input),
                        attribute: attribute.clone(),
                        kind,
                    }),
                    schema: Deferred::Resolved(schema),
                };
            }
        }
        Ok(input)
    }
}

impl QueryRewriter for Extract<'_> {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
        // Subqueries have already been materialized in their own row scope.
        Ok(plan)
    }

    fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
        let mut expression = walk_expression(self, expression)?;
        let ExprKind::ScalarFunction {
            function,
            arguments,
        } = expression.kind.resolved("row identity expression")?
        else {
            return Ok(expression);
        };
        let kind = if function.inner().is::<sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId>() {
            RowIdentityKind::Monotonic
        } else if function.inner().is::<sail_function::scalar::misc::spark_partition_id::SparkPartitionId>() {
            RowIdentityKind::Partition
        } else { return Ok(expression); };
        if !arguments.is_empty() {
            return Err(PlanError::invalid(
                "row identity functions take no arguments",
            ));
        }
        let attribute = self.attributes.entry(kind).or_insert_with(|| {
            let id = self.ids.attribute();
            Attribute {
                id,
                origin: id,
                name: function.name().to_string(),
                qualifier: vec![],
                qualified_position: 0,
                plan_ids: vec![],
                hidden: true,
                result_type: expression.result_type.clone(),
            }
        });
        expression.kind = Deferred::Resolved(ExprKind::Attribute(attribute.id));
        Ok(expression)
    }
}
