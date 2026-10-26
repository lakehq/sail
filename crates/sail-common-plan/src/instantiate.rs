//! Instantiating a stored relation allocates identities, never display names.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::Result;

use crate::deferred::Deferred;
use crate::model::*;
use crate::scope::{CteDefinition, PlanScope};

impl Query {
    /// Copy an analyzed relation into a query-local ID domain. The same mapping
    /// is used across subqueries, outer references, and nested lambda scopes.
    pub fn instantiate(&self, ids: &mut IdGenerator) -> Result<Self> {
        Instance {
            ids,
            attributes: HashMap::new(),
            definitions: HashMap::new(),
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
            definitions: HashMap::new(),
            preserve_origins: !renew,
            preserve_bindings,
        }
        .plan(self.clone())
    }
}

struct Instance<'a> {
    ids: &'a mut IdGenerator,
    attributes: HashMap<AttributeId, AttributeId>,
    definitions: HashMap<*const CteDefinition, Arc<CteDefinition>>,
    preserve_origins: bool,
    preserve_bindings: bool,
}

impl Instance<'_> {
    fn scope(&mut self, scope: &PlanScope) -> Result<Arc<PlanScope>> {
        let mut scope = scope.clone();
        // Captured environments share definitions. Instantiate each template once
        // so nested WITH scopes do not repeatedly copy the same definition graph.
        scope.ctes = scope
            .ctes
            .into_iter()
            .map(|(name, definition)| {
                let key = Arc::as_ptr(&definition);
                let definition = if let Some(existing) = self.definitions.get(&key) {
                    Arc::clone(existing)
                } else {
                    let value = Arc::new(CteDefinition {
                        plan: self.plan(definition.plan.clone())?,
                        parameter: definition.parameter,
                    });
                    self.definitions.insert(key, Arc::clone(&value));
                    value
                };
                Ok((name, definition))
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(scope))
    }

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

    fn schema(&mut self, schema: QuerySchema) -> Result<QuerySchema> {
        Ok(QuerySchema {
            attributes: schema
                .attributes
                .into_iter()
                .map(|a| self.attribute(a))
                .collect::<Result<_>>()?,
        })
    }

    fn expression(&mut self, expression: Expr) -> Result<Expr> {
        expression.result_type.resolved("stored expression type")?;
        let mut expression = expression.map_children(&mut |child| self.expression(child))?;
        expression.id = self.ids.expression();
        expression.kind =
            Deferred::Resolved(match expression.kind.into_resolved("stored expression")? {
                ExprKind::Attribute(id) => ExprKind::Attribute(self.id(id)),
                ExprKind::OuterAttribute { depth, attribute } => ExprKind::OuterAttribute {
                    depth,
                    attribute: self.id(attribute),
                },
                ExprKind::LambdaVariable(id) => ExprKind::LambdaVariable(self.id(id)),
                ExprKind::ScalarSubquery(query) => {
                    ExprKind::ScalarSubquery(Box::new(self.plan(*query)?))
                }
                ExprKind::Exists { query, negated } => ExprKind::Exists {
                    query: Box::new(self.plan(*query)?),
                    negated,
                },
                ExprKind::InSubquery {
                    expression,
                    query,
                    negated,
                } => ExprKind::InSubquery {
                    expression,
                    query: Box::new(self.plan(*query)?),
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
                                    parameters, body, ..
                                } => FunctionArg::Lambda {
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

    fn sorts(&mut self, sorts: Vec<SortKey>) -> Result<Vec<SortKey>> {
        sorts
            .into_iter()
            .map(|sort| {
                Ok(SortKey {
                    expression: self.expression(sort.expression)?,
                    ..sort
                })
            })
            .collect()
    }

    fn plan(&mut self, plan: Query) -> Result<Query> {
        let schema = self.schema(plan.schema.into_resolved("stored plan schema")?)?;
        let node = match plan.kind.into_resolved("stored plan node")? {
            node @ (QueryKind::Empty { .. }
            | QueryKind::Range { .. }
            | QueryKind::Scan { .. }
            | QueryKind::RemoteCheckpoint { .. }) => node,
            QueryKind::Scope { input, scope } => QueryKind::Scope {
                scope: scope.map(|scope| self.scope(&scope)).transpose()?,
                input: Box::new(self.plan(*input)?),
            },
            QueryKind::Values { rows } => QueryKind::Values {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| self.expression(e))
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?,
            },
            QueryKind::Project { input, expressions } => QueryKind::Project {
                input: Box::new(self.plan(*input)?),
                expressions: self.projections(expressions)?,
            },
            QueryKind::Filter { input, condition } => QueryKind::Filter {
                input: Box::new(self.plan(*input)?),
                condition: Box::new(self.expression(*condition)?),
            },
            QueryKind::Aggregate {
                input,
                grouping,
                aggregates,
                grouping_sets,
                grouping_id,
            } => QueryKind::Aggregate {
                input: Box::new(self.plan(*input)?),
                grouping: self.projections(grouping)?,
                aggregates: self.projections(aggregates)?,
                grouping_sets,
                grouping_id: grouping_id.map(|a| self.attribute(a)).transpose()?,
            },
            QueryKind::Window { input, expressions } => QueryKind::Window {
                input: Box::new(self.plan(*input)?),
                expressions: self.projections(expressions)?,
            },
            QueryKind::Sort {
                input,
                order,
                global,
                required,
            } => QueryKind::Sort {
                input: Box::new(self.plan(*input)?),
                order: self.sorts(order)?,
                global,
                required,
            },
            QueryKind::Limit { input, skip, fetch } => QueryKind::Limit {
                input: Box::new(self.plan(*input)?),
                skip: skip
                    .map(|e| self.expression(*e).map(Box::new))
                    .transpose()?,
                fetch: fetch
                    .map(|e| self.expression(*e).map(Box::new))
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
                left: Box::new(self.plan(*left)?),
                right: Box::new(self.plan(*right)?),
                kind,
                condition: condition
                    .map(|e| self.expression(*e).map(Box::new))
                    .transpose()?,
            },
            QueryKind::Union { inputs } => QueryKind::Union {
                inputs: inputs
                    .into_iter()
                    .map(|input| self.plan(input))
                    .collect::<Result<_>>()?,
            },
            QueryKind::Distinct { input } => QueryKind::Distinct {
                input: Box::new(self.plan(*input)?),
            },
            QueryKind::RowIdentity {
                input,
                attribute,
                kind,
            } => QueryKind::RowIdentity {
                input: Box::new(self.plan(*input)?),
                attribute: self.attribute(attribute)?,
                kind,
            },
            QueryKind::MapPartitions { input, function } => QueryKind::MapPartitions {
                input: Box::new(self.plan(*input)?),
                function,
            },
            QueryKind::Unnest {
                input,
                column,
                outer,
            } => QueryKind::Unnest {
                input: Box::new(self.plan(*input)?),
                column: self.id(column),
                outer,
            },
            QueryKind::DistinctOn { input, keys } => QueryKind::DistinctOn {
                input: Box::new(self.plan(*input)?),
                keys: keys
                    .into_iter()
                    .map(|e| self.expression(e))
                    .collect::<Result<_>>()?,
            },
            QueryKind::Repartition {
                input,
                partitioning,
            } => QueryKind::Repartition {
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
        Ok(Query {
            kind: Deferred::Resolved(node),
            schema: Deferred::Resolved(schema),
        })
    }
}
