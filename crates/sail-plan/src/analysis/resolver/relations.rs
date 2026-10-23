use std::collections::HashSet;
use std::sync::Arc;

use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use super::{CteDefinition, NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn with_relations(
        &mut self,
        root: spec::QueryPlan,
        references: Vec<spec::QueryPlan>,
    ) -> PlanResult<LogicalPlan> {
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
                    let plan = self.query(reference.clone()).await?;
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
            self.query(root).await
        }
        .await;
        self.relations = previous_references;
        self.ctes = previous_ctes;
        result
    }

    #[async_recursion::async_recursion]
    pub(super) async fn referenced_subquery(
        &mut self,
        plan_id: i64,
        kind: spec::SubqueryType,
        values: Vec<spec::Expr>,
        negated: bool,
        schema: &PlanSchema,
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
        self.expression(request, schema).await
    }

    pub(super) async fn multi_column_in_subquery(
        &mut self,
        values: Vec<spec::Expr>,
        query: spec::QueryPlan,
        negated: bool,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
        let mut outer = vec![];
        for value in values {
            let value = self.expression(value, schema).await?.expression;
            outer.push(value.transform_up::<PlanError>(&mut |mut e| {
                e.node = Deferred::Resolved(match e.node.into_resolved("IN outer value")? {
                    ExpressionNode::Attribute(attribute) => ExpressionNode::OuterAttribute {
                        depth: 1,
                        attribute,
                    },
                    ExpressionNode::OuterAttribute { depth, attribute } => {
                        ExpressionNode::OuterAttribute {
                            depth: depth + 1,
                            attribute,
                        }
                    }
                    node => node,
                });
                Ok(e)
            })?);
        }
        let query = self.subquery(query, schema).await?;
        let output = query.schema.resolved("IN subquery output")?;
        if output.attributes.len() != outer.len() {
            return Err(PlanError::invalid("IN subquery column count mismatch"));
        }
        let mut condition = None;
        for (outer, inner) in outer.into_iter().zip(&output.attributes) {
            let inner = self.reference(inner)?;
            let (inner, outer) = self.in_arguments(vec![inner, outer])?.two()?;
            let comparison = self.binary(inner, Operator::Eq, outer)?;
            condition = Some(match condition {
                None => comparison,
                Some(condition) => self.binary(condition, Operator::And, comparison)?,
            });
        }
        let query = Self::filter(
            query,
            condition.ok_or_else(|| PlanError::internal("empty IN comparison"))?,
        )?;
        Ok(NamedExpression {
            name: "in_subquery".into(),
            expression: LogicalExpression {
                id: self.context.ids.expression(),
                node: Deferred::Resolved(ExpressionNode::Exists {
                    query: Box::new(query),
                    negated,
                }),
                result_type: Deferred::Resolved(ExpressionType::new(
                    arrow_schema::DataType::Boolean,
                    false,
                )),
            },
        })
    }

    #[async_recursion::async_recursion]
    pub(super) async fn identifier(&mut self, request: spec::Expr) -> PlanResult<spec::ObjectName> {
        let expression = self
            .expression(request, &PlanSchema::default())
            .await?
            .expression;
        let value = crate::analysis::lowering::evaluate_constant(&expression)?;
        let name = value.try_as_str().flatten().ok_or_else(|| {
            PlanError::invalid("IDENTIFIER expression must evaluate to a non-null string")
        })?;
        Ok(sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(name)?,
        )?)
    }
}
