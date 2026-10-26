use std::collections::HashSet;
use std::sync::Arc;

use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{CteDefinition, NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn with_relations(
        &mut self,
        root: spec::QueryPlan,
        references: Vec<spec::QueryPlan>,
    ) -> PlanResult<Query> {
        let previous_references = self.relations.clone();
        let previous_ctes = self.ctes.clone();
        let result = async {
            let mut seen = HashSet::new();
            for reference in references {
                let id = reference
                    .plan_id
                    .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
                if !seen.insert(id) {
                    return Err(PlanError::invalid(format!(
                        "duplicate subquery reference for plan_id {id}"
                    )));
                }
                if let spec::QueryNode::SubqueryAlias {
                    alias, qualifier, ..
                } = &reference.node
                {
                    let key = qualifier
                        .iter()
                        .map(|x| x.as_ref())
                        .chain(std::iter::once(alias.as_ref()))
                        .collect::<Vec<_>>()
                        .join(".");
                    let plan = self.resolve_query_plan(reference.clone()).await?;
                    self.ctes.insert(
                        self.name_key(&key),
                        Arc::new(CteDefinition {
                            plan,
                            parameter: true,
                        }),
                    );
                }
                self.relations.insert(id, reference);
            }
            self.resolve_query_plan(root).await
        }
        .await;
        self.relations = previous_references;
        self.ctes = previous_ctes;
        result
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn referenced_subquery(
        &mut self,
        plan_id: i64,
        kind: spec::SubqueryType,
        values: Vec<spec::Expr>,
        negated: bool,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpression> {
        let query = self.relations.get(&plan_id).cloned().ok_or_else(|| {
            PlanError::analysis(format!(
                "subquery plan_id {plan_id} not found in WithRelations references"
            ))
        })?;
        let request = match kind {
            spec::SubqueryType::Scalar => spec::Expr::ScalarSubquery {
                subquery: Box::new(query),
            },
            spec::SubqueryType::Exists => spec::Expr::Exists {
                subquery: Box::new(query),
                negated,
            },
            spec::SubqueryType::In => {
                let mut values = values.into_iter();
                let first = values
                    .next()
                    .ok_or_else(|| PlanError::invalid("IN subquery missing value expression"))?;
                let remaining: Vec<_> = values.collect();
                if remaining.is_empty() {
                    spec::Expr::InSubquery {
                        expr: Box::new(first),
                        subquery: Box::new(query),
                        negated,
                    }
                } else {
                    return self
                        .multi_column_in_subquery(
                            std::iter::once(first).chain(remaining).collect(),
                            query,
                            negated,
                            schema,
                        )
                        .await;
                }
            }
        };
        self.resolve_named_expression(request, schema).await
    }

    pub(in crate::resolver) async fn multi_column_in_subquery(
        &mut self,
        values: Vec<spec::Expr>,
        query: spec::QueryPlan,
        negated: bool,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpression> {
        let mut outer = vec![];
        for value in values {
            let value = self
                .resolve_named_expression(value, schema)
                .await?
                .expression;
            outer.push(value.transform_up::<PlanError>(&mut |mut e| {
                e.kind = Deferred::Resolved(match e.kind.into_resolved("IN outer value")? {
                    ExprKind::Attribute(attribute) => ExprKind::OuterAttribute {
                        depth: 1,
                        attribute,
                    },
                    ExprKind::OuterAttribute { depth, attribute } => ExprKind::OuterAttribute {
                        depth: depth + 1,
                        attribute,
                    },
                    node => node,
                });
                Ok(e)
            })?);
        }
        let query = self.resolve_expression_subquery(query, schema).await?;
        let output = query.schema.resolved("IN subquery output")?;
        if output.attributes.len() != outer.len() {
            return Err(PlanError::invalid("IN subquery column count mismatch"));
        }
        let mut condition = None;
        for (outer, inner) in outer.into_iter().zip(&output.attributes) {
            let inner = self.reference(inner)?;
            let (inner, outer) = crate::resolver::in_arguments(
                &mut self.context.function_context(),
                vec![inner, outer],
            )?
            .two()?;
            let comparison = self
                .context
                .function_context()
                .binary(inner, Operator::Eq, outer)?;
            condition = Some(match condition {
                None => comparison,
                Some(condition) => {
                    self.context
                        .function_context()
                        .binary(condition, Operator::And, comparison)?
                }
            });
        }
        let query = Self::filter(
            query,
            condition.ok_or_else(|| PlanError::internal("empty IN comparison"))?,
        )?;
        Ok(NamedExpression {
            name: "in_subquery".into(),
            expression: Expr {
                id: self.context.ids.expression(),
                kind: Deferred::Resolved(ExprKind::Exists {
                    query: Box::new(query),
                    negated,
                }),
                result_type: Deferred::Resolved(ExprType::new(
                    arrow_schema::DataType::Boolean,
                    false,
                )),
            },
        })
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn identifier(
        &mut self,
        request: spec::Expr,
    ) -> PlanResult<spec::ObjectName> {
        let expression = self
            .resolve_named_expression(request, &QuerySchema::default())
            .await?
            .expression;
        let value = crate::analyzer::lowering::evaluate_constant(&expression)?;
        let name = value.try_as_str().flatten().ok_or_else(|| {
            PlanError::invalid("IDENTIFIER expression must evaluate to a non-null string")
        })?;
        Ok(sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(name)?,
        )?)
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn query_relations(
        &mut self,
        node: spec::QueryNode,
    ) -> PlanResult<Query> {
        Ok(match node {
            spec::QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => {
                let input = match input {
                    Some(input) => self.resolve_query_plan(*input).await?,
                    None => Self::empty(true),
                };
                let original = input
                    .schema
                    .resolved("lateral view input")?
                    .attributes
                    .clone();
                let function = if outer {
                    spec::ObjectName::bare(
                        match Vec::<String>::from(function)
                            .join(".")
                            .to_ascii_lowercase()
                            .as_str()
                        {
                            "explode" => "explode_outer".to_string(),
                            "posexplode" => "posexplode_outer".to_string(),
                            "inline" => "inline_outer".to_string(),
                            name => name.to_string(),
                        },
                    )
                } else {
                    function
                };
                let mut request = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: function,
                    arguments,
                    named_arguments,
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                });
                if let Some(name) = column_aliases {
                    request = spec::Expr::Alias {
                        expr: Box::new(request),
                        name,
                        metadata: None,
                    };
                }
                let (input, generated) = self.generator(input, request).await?;
                let mut projections = original
                    .iter()
                    .map(|a| {
                        let a = input
                            .schema
                            .resolved("lateral passthrough")?
                            .attribute(a.id)?;
                        Ok(Projection {
                            expression: self.reference(a)?,
                            attribute: a.clone(),
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                for named in generated {
                    let mut projection =
                        self.projection(named, input.schema.resolved("lateral output")?)?;
                    if let Some(qualifier) = &table_alias {
                        projection.attribute.qualifier = qualifier.clone().into();
                    }
                    projections.push(projection);
                }
                let schema = QuerySchema {
                    attributes: projections.iter().map(|p| p.attribute.clone()).collect(),
                };
                Query {
                    kind: Deferred::Resolved(QueryKind::Project {
                        input: Box::new(input),
                        expressions: projections,
                    }),
                    schema: Deferred::Resolved(schema),
                }
            }
            spec::QueryNode::Repartition {
                input,
                num_partitions,
                shuffle,
            } => {
                if num_partitions == 0 {
                    return Err(PlanError::analysis("partition count must be positive"));
                }
                let input = self.resolve_query_plan(*input).await?;
                let schema = input.schema.clone();
                Query {
                    kind: Deferred::Resolved(QueryKind::Repartition {
                        input: Box::new(input),
                        partitioning: if shuffle {
                            Partitioning::RoundRobin(num_partitions)
                        } else {
                            Partitioning::Coalesce(num_partitions)
                        },
                    }),
                    schema,
                }
            }
            spec::QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                let mut input = self.resolve_query_plan(*input).await?;
                let schema = input.schema.resolved("repartition input")?.clone();
                let mut expressions = vec![];
                for expression in partition_expressions {
                    let (next, value) = self.recover_expression(input, expression, false).await?;
                    input = next;
                    expressions.push(value.expression);
                }
                let partitioning = if let Some(partitions) = num_partitions {
                    if partitions == 0 {
                        return Err(PlanError::analysis("partition count must be positive"));
                    }
                    Partitioning::Hash(expressions, partitions)
                } else {
                    Partitioning::Distribute(expressions)
                };
                let partitioned = Query {
                    schema: input.schema.clone(),
                    kind: Deferred::Resolved(QueryKind::Repartition {
                        input: Box::new(input),
                        partitioning,
                    }),
                };
                self.restore_output(partitioned, schema)?
            }
            spec::QueryNode::Deduplicate(dedup) => self.resolve_query_deduplicate(dedup).await?,
            spec::QueryNode::Hint {
                input,
                name,
                parameters,
            } => self.resolve_query_hint(*input, name, parameters).await?,
            spec::QueryNode::Sample(sample) => self.resolve_query_sample(sample).await?,
            spec::QueryNode::TableSample { input, sample } => {
                let input = self.resolve_query_plan(*input).await?;
                self.resolve_query_table_sample(input, sample).await?
            }
            spec::QueryNode::SetOperation(set) => self.resolve_query_set_operation(set).await?,
            spec::QueryNode::LateralJoin {
                left,
                right,
                join_condition,
                join_type,
            } => {
                self.resolve_query_join(
                    spec::Join {
                        left,
                        right,
                        join_type,
                        join_criteria: join_condition.map(spec::JoinCriteria::On),
                        join_data_type: None,
                    },
                    true,
                )
                .await?
            }
            spec::QueryNode::Join(join) => self.resolve_query_join(join, false).await?,
            node => {
                return Err(PlanError::unsupported(format!(
                    "native query resolution: {node:?}"
                )));
            }
        })
    }
}
