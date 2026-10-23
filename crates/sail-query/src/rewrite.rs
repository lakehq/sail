//! Complete native-tree rewrites, including subqueries and lambda bodies.
//!
//! The default methods visit children before returning their parent. Override a
//! method and invoke `walk_*` to customize a pass while retaining traversal.

use datafusion_common::DataFusionError;

use crate::deferred::Deferred;
use crate::ir::*;

pub trait QueryRewriter: Sized {
    type Error: From<DataFusionError>;

    fn rewrite_attribute(&mut self, attribute: Attribute) -> Result<Attribute, Self::Error> {
        Ok(attribute)
    }
    fn rewrite_schema(&mut self, schema: PlanSchema) -> Result<PlanSchema, Self::Error> {
        Ok(PlanSchema {
            attributes: schema
                .attributes
                .into_iter()
                .map(|a| self.rewrite_attribute(a))
                .collect::<Result<_, _>>()?,
        })
    }
    fn rewrite_expression(
        &mut self,
        expression: LogicalExpression,
    ) -> Result<LogicalExpression, Self::Error> {
        walk_expression(self, expression)
    }
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, Self::Error> {
        walk_plan(self, plan)
    }
    fn rewrite_projections(
        &mut self,
        projections: Vec<Projection>,
    ) -> Result<Vec<Projection>, Self::Error> {
        projections
            .into_iter()
            .map(|p| {
                Ok(Projection {
                    expression: self.rewrite_expression(p.expression)?,
                    attribute: self.rewrite_attribute(p.attribute)?,
                })
            })
            .collect()
    }

    fn rewrite_sorts(
        &mut self,
        sorts: Vec<SortExpression>,
    ) -> Result<Vec<SortExpression>, Self::Error> {
        sorts
            .into_iter()
            .map(|sort| {
                Ok(SortExpression {
                    expression: self.rewrite_expression(sort.expression)?,
                    ..sort
                })
            })
            .collect()
    }
}
pub fn walk_expression<R: QueryRewriter>(
    rewriter: &mut R,
    expression: LogicalExpression,
) -> Result<LogicalExpression, R::Error> {
    if matches!(expression.node, Deferred::Unresolved(_)) {
        return Ok(expression);
    }
    let mut expression =
        expression.map_children(&mut |child| rewriter.rewrite_expression(child))?;
    expression.node =
        Deferred::Resolved(match expression.node.into_resolved("expression rewrite")? {
            ExpressionNode::ScalarSubquery(query) => {
                ExpressionNode::ScalarSubquery(Box::new(rewriter.rewrite_plan(*query)?))
            }
            ExpressionNode::Exists { query, negated } => ExpressionNode::Exists {
                query: Box::new(rewriter.rewrite_plan(*query)?),
                negated,
            },
            ExpressionNode::InSubquery {
                expression,
                query,
                negated,
            } => ExpressionNode::InSubquery {
                expression,
                query: Box::new(rewriter.rewrite_plan(*query)?),
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
                                id,
                                parameters,
                                body,
                            } => FunctionArgument::Lambda {
                                id,
                                parameters: rewriter.rewrite_schema(parameters)?,
                                body: Box::new(rewriter.rewrite_expression(*body)?),
                            },
                        })
                    })
                    .collect::<Result<_, R::Error>>()?,
            },
            node => node,
        });
    Ok(expression)
}

pub fn walk_plan<R: QueryRewriter>(
    rewriter: &mut R,
    plan: LogicalPlan,
) -> Result<LogicalPlan, R::Error> {
    let schema = match plan.schema {
        Deferred::Resolved(schema) => Deferred::Resolved(rewriter.rewrite_schema(schema)?),
        schema => schema,
    };
    let Deferred::Resolved(node) = plan.node else {
        return Ok(LogicalPlan { schema, ..plan });
    };
    let node = match node {
        node @ (PlanNode::Empty { .. }
        | PlanNode::Range { .. }
        | PlanNode::Scan { .. }
        | PlanNode::RemoteCheckpoint { .. }) => node,
        PlanNode::Scope { input } => PlanNode::Scope {
            input: Box::new(rewriter.rewrite_plan(*input)?),
        },
        PlanNode::Values { rows } => PlanNode::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|e| rewriter.rewrite_expression(e))
                        .collect::<Result<_, R::Error>>()
                })
                .collect::<Result<_, R::Error>>()?,
        },
        PlanNode::Project { input, expressions } => PlanNode::Project {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            expressions: rewriter.rewrite_projections(expressions)?,
        },
        PlanNode::Filter { input, condition } => PlanNode::Filter {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            condition: Box::new(rewriter.rewrite_expression(*condition)?),
        },
        PlanNode::Aggregate {
            input,
            grouping,
            aggregates,
            grouping_sets,
            grouping_id,
        } => PlanNode::Aggregate {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            grouping: rewriter.rewrite_projections(grouping)?,
            aggregates: rewriter.rewrite_projections(aggregates)?,
            grouping_sets,
            grouping_id: grouping_id
                .map(|a| rewriter.rewrite_attribute(a))
                .transpose()?,
        },
        PlanNode::Window { input, expressions } => PlanNode::Window {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            expressions: rewriter.rewrite_projections(expressions)?,
        },
        PlanNode::Sort {
            input,
            order,
            global,
            required,
        } => PlanNode::Sort {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            order: rewriter.rewrite_sorts(order)?,
            global,
            required,
        },
        PlanNode::Limit { input, skip, fetch } => PlanNode::Limit {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            skip: skip
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
                .transpose()?,
            fetch: fetch
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
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
            left: Box::new(rewriter.rewrite_plan(*left)?),
            right: Box::new(rewriter.rewrite_plan(*right)?),
            kind,
            condition: condition
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
                .transpose()?,
        },
        PlanNode::Union { inputs } => PlanNode::Union {
            inputs: inputs
                .into_iter()
                .map(|input| rewriter.rewrite_plan(input))
                .collect::<Result<_, R::Error>>()?,
        },
        PlanNode::Distinct { input } => PlanNode::Distinct {
            input: Box::new(rewriter.rewrite_plan(*input)?),
        },
        PlanNode::RowIdentity {
            input,
            attribute,
            kind,
        } => PlanNode::RowIdentity {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            attribute: rewriter.rewrite_attribute(attribute)?,
            kind,
        },
        PlanNode::MapPartitions { input, function } => PlanNode::MapPartitions {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            function,
        },
        PlanNode::Unnest {
            input,
            column,
            outer,
        } => PlanNode::Unnest {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            column,
            outer,
        },
        PlanNode::DistinctOn { input, keys } => PlanNode::DistinctOn {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            keys: keys
                .into_iter()
                .map(|e| rewriter.rewrite_expression(e))
                .collect::<Result<_, R::Error>>()?,
        },
        PlanNode::Repartition {
            input,
            partitioning,
        } => PlanNode::Repartition {
            input: Box::new(rewriter.rewrite_plan(*input)?),
            partitioning: match partitioning {
                Partitioning::Hash(expressions, count) => Partitioning::Hash(
                    expressions
                        .into_iter()
                        .map(|e| rewriter.rewrite_expression(e))
                        .collect::<Result<_, R::Error>>()?,
                    count,
                ),
                Partitioning::Distribute(expressions) => Partitioning::Distribute(
                    expressions
                        .into_iter()
                        .map(|e| rewriter.rewrite_expression(e))
                        .collect::<Result<_, R::Error>>()?,
                ),
                partitioning => partitioning,
            },
        },
    };
    Ok(LogicalPlan {
        node: Deferred::Resolved(node),
        schema,
    })
}
