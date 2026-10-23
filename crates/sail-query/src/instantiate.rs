//! Instantiating a stored relation allocates identities, never display names.

use std::collections::HashMap;

use datafusion_common::Result;

use crate::deferred::Deferred;
use crate::ir::*;

impl LogicalPlan {
    /// Copy an analyzed relation into a query-local ID domain. The same mapping
    /// is used across subqueries, outer references, and nested lambda scopes.
    pub fn instantiate(&self, ids: &mut IdGenerator) -> Result<Self> {
        Instance {
            ids,
            attributes: HashMap::new(),
            preserve_origins: false,
            preserve_bindings: false,
        }
        .plan(self.clone())
    }
    /// Instantiate a CTE or parameter relation while retaining semantic origins
    /// until analysis determines that the reference must renew them.
    pub fn instantiate_reference(
        &self,
        ids: &mut IdGenerator,
        renew: bool,
        preserve_bindings: bool,
    ) -> Result<Self> {
        Instance {
            ids,
            attributes: HashMap::new(),
            preserve_origins: !renew,
            preserve_bindings,
        }
        .plan(self.clone())
    }
}

struct Instance<'a> {
    ids: &'a mut IdGenerator,
    attributes: HashMap<AttributeId, AttributeId>,
    preserve_origins: bool,
    preserve_bindings: bool,
}

impl Instance<'_> {
    fn id(&mut self, id: AttributeId) -> AttributeId {
        *self
            .attributes
            .entry(id)
            .or_insert_with(|| self.ids.attribute())
    }

    fn attribute(&mut self, mut attribute: Attribute) -> Result<Attribute> {
        attribute.result_type.resolved("stored attribute type")?;
        attribute.id = self.id(attribute.id);
        if !self.preserve_origins {
            attribute.origin = self.id(attribute.origin);
        }
        // Spark Connect plan IDs belong to the original request, not the view
        // instance. A reference request adds its own IDs after instantiation.
        if !self.preserve_bindings {
            attribute.plan_ids.clear();
        }
        Ok(attribute)
    }

    fn schema(&mut self, schema: PlanSchema) -> Result<PlanSchema> {
        Ok(PlanSchema {
            attributes: schema
                .attributes
                .into_iter()
                .map(|a| self.attribute(a))
                .collect::<Result<_>>()?,
        })
    }

    fn expression(&mut self, expression: LogicalExpression) -> Result<LogicalExpression> {
        expression.result_type.resolved("stored expression type")?;
        let mut expression = expression.map_children(&mut |child| self.expression(child))?;
        expression.id = self.ids.expression();
        expression.node =
            Deferred::Resolved(match expression.node.into_resolved("stored expression")? {
                ExpressionNode::Attribute(id) => ExpressionNode::Attribute(self.id(id)),
                ExpressionNode::OuterAttribute { depth, attribute } => {
                    ExpressionNode::OuterAttribute {
                        depth,
                        attribute: self.id(attribute),
                    }
                }
                ExpressionNode::LambdaVariable(id) => ExpressionNode::LambdaVariable(self.id(id)),
                ExpressionNode::ScalarSubquery(query) => {
                    ExpressionNode::ScalarSubquery(Box::new(self.plan(*query)?))
                }
                ExpressionNode::Exists { query, negated } => ExpressionNode::Exists {
                    query: Box::new(self.plan(*query)?),
                    negated,
                },
                ExpressionNode::InSubquery {
                    expression,
                    query,
                    negated,
                } => ExpressionNode::InSubquery {
                    expression,
                    query: Box::new(self.plan(*query)?),
                    negated,
                },
                ExpressionNode::HigherOrderFunction {
                    function,
                    arguments,
                } => ExpressionNode::HigherOrderFunction {
                    function,
                    arguments: arguments
                        .into_iter()
                        .map(|argument| {
                            Ok(match argument {
                                FunctionArgument::Value(_) => argument,
                                FunctionArgument::Lambda {
                                    parameters, body, ..
                                } => FunctionArgument::Lambda {
                                    id: self.ids.expression(),
                                    parameters: self.schema(parameters)?,
                                    body: Box::new(self.expression(*body)?),
                                },
                            })
                        })
                        .collect::<Result<_>>()?,
                },
                node => node,
            });
        Ok(expression)
    }

    fn projections(&mut self, projections: Vec<Projection>) -> Result<Vec<Projection>> {
        projections
            .into_iter()
            .map(|p| {
                Ok(Projection {
                    expression: self.expression(p.expression)?,
                    attribute: self.attribute(p.attribute)?,
                })
            })
            .collect()
    }

    fn sorts(&mut self, sorts: Vec<SortExpression>) -> Result<Vec<SortExpression>> {
        sorts
            .into_iter()
            .map(|sort| {
                Ok(SortExpression {
                    expression: self.expression(sort.expression)?,
                    ..sort
                })
            })
            .collect()
    }

    fn plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let schema = self.schema(plan.schema.into_resolved("stored plan schema")?)?;
        let node = match plan.node.into_resolved("stored plan node")? {
            node @ (PlanNode::Empty { .. }
            | PlanNode::Range { .. }
            | PlanNode::Scan { .. }
            | PlanNode::RemoteCheckpoint { .. }) => node,
            PlanNode::Scope { input } => PlanNode::Scope {
                input: Box::new(self.plan(*input)?),
            },
            PlanNode::Values { rows } => PlanNode::Values {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| self.expression(e))
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },
            PlanNode::Project { input, expressions } => PlanNode::Project {
                input: Box::new(self.plan(*input)?),
                expressions: self.projections(expressions)?,
            },
            PlanNode::Filter { input, condition } => PlanNode::Filter {
                input: Box::new(self.plan(*input)?),
                condition: Box::new(self.expression(*condition)?),
            },
            PlanNode::Aggregate {
                input,
                grouping,
                aggregates,
                grouping_sets,
                grouping_id,
            } => PlanNode::Aggregate {
                input: Box::new(self.plan(*input)?),
                grouping: self.projections(grouping)?,
                aggregates: self.projections(aggregates)?,
                grouping_sets,
                grouping_id: grouping_id.map(|a| self.attribute(a)).transpose()?,
            },
            PlanNode::Window { input, expressions } => PlanNode::Window {
                input: Box::new(self.plan(*input)?),
                expressions: self.projections(expressions)?,
            },
            PlanNode::Sort {
                input,
                order,
                global,
                required,
            } => PlanNode::Sort {
                input: Box::new(self.plan(*input)?),
                order: self.sorts(order)?,
                global,
                required,
            },
            PlanNode::Limit { input, skip, fetch } => PlanNode::Limit {
                input: Box::new(self.plan(*input)?),
                skip: skip
                    .map(|e| self.expression(*e).map(Box::new))
                    .transpose()?,
                fetch: fetch
                    .map(|e| self.expression(*e).map(Box::new))
                    .transpose()?,
            },
            PlanNode::Join {
                lateral,
                left,
                right,
                kind,
                condition,
            } => PlanNode::Join {
                lateral,
                left: Box::new(self.plan(*left)?),
                right: Box::new(self.plan(*right)?),
                kind,
                condition: condition
                    .map(|e| self.expression(*e).map(Box::new))
                    .transpose()?,
            },
            PlanNode::Union { inputs } => PlanNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|input| self.plan(input))
                    .collect::<Result<_>>()?,
            },
            PlanNode::Distinct { input } => PlanNode::Distinct {
                input: Box::new(self.plan(*input)?),
            },
            PlanNode::RowIdentity {
                input,
                attribute,
                kind,
            } => PlanNode::RowIdentity {
                input: Box::new(self.plan(*input)?),
                attribute: self.attribute(attribute)?,
                kind,
            },
            PlanNode::MapPartitions { input, function } => PlanNode::MapPartitions {
                input: Box::new(self.plan(*input)?),
                function,
            },
            PlanNode::Unnest {
                input,
                column,
                outer,
            } => PlanNode::Unnest {
                input: Box::new(self.plan(*input)?),
                column: self.id(column),
                outer,
            },
            PlanNode::DistinctOn { input, keys } => PlanNode::DistinctOn {
                input: Box::new(self.plan(*input)?),
                keys: keys
                    .into_iter()
                    .map(|e| self.expression(e))
                    .collect::<Result<_>>()?,
            },
            PlanNode::Repartition {
                input,
                partitioning,
            } => PlanNode::Repartition {
                input: Box::new(self.plan(*input)?),
                partitioning: match partitioning {
                    Partitioning::Hash(expressions, count) => Partitioning::Hash(
                        expressions
                            .into_iter()
                            .map(|e| self.expression(e))
                            .collect::<Result<_>>()?,
                        count,
                    ),
                    Partitioning::Distribute(expressions) => Partitioning::Distribute(
                        expressions
                            .into_iter()
                            .map(|e| self.expression(e))
                            .collect::<Result<_>>()?,
                    ),
                    partitioning => partitioning,
                },
            },
        };
        Ok(LogicalPlan {
            node: Deferred::Resolved(node),
            schema: Deferred::Resolved(schema),
        })
    }
}
