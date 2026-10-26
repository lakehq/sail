//! Complete native-tree rewrites, including subqueries and lambda bodies.
//!
//! The default methods visit children before returning their parent. Override a
//! method and invoke `walk_*` to customize a pass while retaining traversal.

use datafusion_common::DataFusionError;

use crate::deferred::Deferred;
use crate::model::*;

pub trait QueryRewriter: Sized {
    type Error: From<DataFusionError>;

    fn rewrite_attribute(&mut self, attribute: Attribute) -> Result<Attribute, Self::Error> {
        Ok(attribute)
    }
    fn rewrite_schema(&mut self, schema: QuerySchema) -> Result<QuerySchema, Self::Error> {
        Ok(QuerySchema {
            attributes: schema
                .attributes
                .into_iter()
                .map(|a| self.rewrite_attribute(a))
                .collect::<Result<_, _>>()?,
        })
    }
    fn rewrite_expression(&mut self, expression: Expr) -> Result<Expr, Self::Error> {
        walk_expression(self, expression)
    }
    fn rewrite_query(&mut self, plan: Query) -> Result<Query, Self::Error> {
        walk_query(self, plan)
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

    fn rewrite_sorts(&mut self, sorts: Vec<SortKey>) -> Result<Vec<SortKey>, Self::Error> {
        sorts
            .into_iter()
            .map(|sort| {
                Ok(SortKey {
                    expression: self.rewrite_expression(sort.expression)?,
                    ..sort
                })
            })
            .collect()
    }
}
pub fn walk_expression<R: QueryRewriter>(
    rewriter: &mut R,
    expression: Expr,
) -> Result<Expr, R::Error> {
    if matches!(expression.kind, Deferred::Unresolved(_)) {
        return Ok(expression);
    }
    let mut expression =
        expression.map_children(&mut |child| rewriter.rewrite_expression(child))?;
    expression.kind =
        Deferred::Resolved(match expression.kind.into_resolved("expression rewrite")? {
            ExprKind::ScalarSubquery(query) => {
                ExprKind::ScalarSubquery(Box::new(rewriter.rewrite_query(*query)?))
            }
            ExprKind::Exists { query, negated } => ExprKind::Exists {
                query: Box::new(rewriter.rewrite_query(*query)?),
                negated,
            },
            ExprKind::InSubquery {
                expression,
                query,
                negated,
            } => ExprKind::InSubquery {
                expression,
                query: Box::new(rewriter.rewrite_query(*query)?),
                negated,
            },
            ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => ExprKind::HigherOrderFunction {
                function,
                arguments: arguments
                    .into_iter()
                    .map(|argument| {
                        Ok(match argument {
                            FunctionArg::Value(_) => argument,
                            FunctionArg::Lambda {
                                id,
                                parameters,
                                body,
                            } => FunctionArg::Lambda {
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

pub fn walk_query<R: QueryRewriter>(rewriter: &mut R, plan: Query) -> Result<Query, R::Error> {
    let schema = match plan.schema {
        Deferred::Resolved(schema) => Deferred::Resolved(rewriter.rewrite_schema(schema)?),
        schema => schema,
    };
    let Deferred::Resolved(node) = plan.kind else {
        return Ok(Query { schema, ..plan });
    };
    let node = match node {
        node @ (QueryKind::Empty { .. }
        | QueryKind::Range { .. }
        | QueryKind::Scan { .. }
        | QueryKind::RemoteCheckpoint { .. }) => node,
        QueryKind::Scope { input, scope } => QueryKind::Scope {
            scope,
            input: Box::new(rewriter.rewrite_query(*input)?),
        },
        QueryKind::Values { rows } => QueryKind::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|e| rewriter.rewrite_expression(e))
                        .collect::<Result<_, R::Error>>()
                })
                .collect::<Result<_, R::Error>>()?,
        },
        QueryKind::Project { input, expressions } => QueryKind::Project {
            input: Box::new(rewriter.rewrite_query(*input)?),
            expressions: rewriter.rewrite_projections(expressions)?,
        },
        QueryKind::Filter { input, condition } => QueryKind::Filter {
            input: Box::new(rewriter.rewrite_query(*input)?),
            condition: Box::new(rewriter.rewrite_expression(*condition)?),
        },
        QueryKind::Aggregate {
            input,
            grouping,
            aggregates,
            grouping_sets,
            grouping_id,
        } => QueryKind::Aggregate {
            input: Box::new(rewriter.rewrite_query(*input)?),
            grouping: rewriter.rewrite_projections(grouping)?,
            aggregates: rewriter.rewrite_projections(aggregates)?,
            grouping_sets,
            grouping_id: grouping_id
                .map(|a| rewriter.rewrite_attribute(a))
                .transpose()?,
        },
        QueryKind::Window { input, expressions } => QueryKind::Window {
            input: Box::new(rewriter.rewrite_query(*input)?),
            expressions: rewriter.rewrite_projections(expressions)?,
        },
        QueryKind::Sort {
            input,
            order,
            global,
            required,
        } => QueryKind::Sort {
            input: Box::new(rewriter.rewrite_query(*input)?),
            order: rewriter.rewrite_sorts(order)?,
            global,
            required,
        },
        QueryKind::Limit { input, skip, fetch } => QueryKind::Limit {
            input: Box::new(rewriter.rewrite_query(*input)?),
            skip: skip
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
                .transpose()?,
            fetch: fetch
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
                .transpose()?,
        },
        QueryKind::Join {
            lateral,
            left,
            right,
            kind,
            condition,
        } => QueryKind::Join {
            lateral,
            left: Box::new(rewriter.rewrite_query(*left)?),
            right: Box::new(rewriter.rewrite_query(*right)?),
            kind,
            condition: condition
                .map(|e| rewriter.rewrite_expression(*e).map(Box::new))
                .transpose()?,
        },
        QueryKind::Union { inputs } => QueryKind::Union {
            inputs: inputs
                .into_iter()
                .map(|input| rewriter.rewrite_query(input))
                .collect::<Result<_, R::Error>>()?,
        },
        QueryKind::Distinct { input } => QueryKind::Distinct {
            input: Box::new(rewriter.rewrite_query(*input)?),
        },
        QueryKind::RowIdentity {
            input,
            attribute,
            kind,
        } => QueryKind::RowIdentity {
            input: Box::new(rewriter.rewrite_query(*input)?),
            attribute: rewriter.rewrite_attribute(attribute)?,
            kind,
        },
        QueryKind::MapPartitions { input, function } => QueryKind::MapPartitions {
            input: Box::new(rewriter.rewrite_query(*input)?),
            function,
        },
        QueryKind::Unnest {
            input,
            column,
            outer,
        } => QueryKind::Unnest {
            input: Box::new(rewriter.rewrite_query(*input)?),
            column,
            outer,
        },
        QueryKind::DistinctOn { input, keys } => QueryKind::DistinctOn {
            input: Box::new(rewriter.rewrite_query(*input)?),
            keys: keys
                .into_iter()
                .map(|e| rewriter.rewrite_expression(e))
                .collect::<Result<_, R::Error>>()?,
        },
        QueryKind::Repartition {
            input,
            partitioning,
        } => QueryKind::Repartition {
            input: Box::new(rewriter.rewrite_query(*input)?),
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
    Ok(Query {
        kind: Deferred::Resolved(node),
        schema,
    })
}
