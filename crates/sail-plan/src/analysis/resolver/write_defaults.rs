//! DEFAULT stays a deferred expression until INSERT binds its target column.
use std::collections::HashMap;

use sail_common::spec;
use sail_query::rewrite::{QueryRewriter, walk_expression};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn plan_has_default_column_value(plan: &QueryNode) -> PlanResult<bool> {
        struct Find(bool);
        impl QueryRewriter for Find {
            type Error = PlanError;
            fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
                self.0 |= is_default(&expression);
                walk_expression(self, expression)
            }
        }
        let mut find = Find(false);
        find.rewrite_query(plan.clone())?;
        Ok(find.0)
    }

    pub(super) fn replace_write_defaults(
        &mut self,
        plan: QueryNode,
        bindings: &HashMap<AttributeId, usize>,
        defaults: &[ExprNode],
    ) -> PlanResult<QueryNode> {
        let original = plan.schema.into_resolved("DEFAULT input schema")?;
        match plan.kind.into_resolved("DEFAULT input plan")? {
            QueryKind::Values { rows } => {
                let mut replaced = vec![];
                for row in rows {
                    let mut values = vec![];
                    for (expression, attribute) in row.into_iter().zip(&original.attributes) {
                        let standalone = is_default(&expression)
                            || matches!(&expression.kind, Deferred::Resolved(ExprKind::Cast { expression, .. }) if is_default(expression));
                        values.push(if standalone {
                            let index = bindings.get(&attribute.id).ok_or_else(|| {
                                PlanError::invalid(
                                    "DEFAULT could not be matched to a target column",
                                )
                            })?;
                            let value = defaults.get(*index).ok_or_else(|| {
                                PlanError::internal("DEFAULT target column missing")
                            })?;
                            self.copy_expression(value)?
                        } else {
                            expression
                        });
                    }
                    replaced.push(values);
                }
                let mut result = self.typed_values(replaced)?;
                let inferred = result.schema.into_resolved("DEFAULT inferred schema")?;
                result.schema = Deferred::Resolved(QuerySchema {
                    attributes: original
                        .attributes
                        .into_iter()
                        .zip(inferred.attributes)
                        .map(|(mut original, inferred)| {
                            original.result_type = inferred.result_type;
                            original
                        })
                        .collect(),
                });
                Ok(result)
            }
            QueryKind::Scope { input } => {
                let input = self.replace_write_defaults(*input, bindings, defaults)?;
                let inferred = input.schema.resolved("DEFAULT scope input")?;
                let mut schema = original;
                for attribute in &mut schema.attributes {
                    attribute.result_type = inferred.attribute(attribute.id)?.result_type.clone();
                }
                Ok(QueryNode {
                    kind: Deferred::Resolved(QueryKind::Scope {
                        input: Box::new(input),
                    }),
                    schema: Deferred::Resolved(schema),
                })
            }
            QueryKind::Project { input, expressions } => {
                let mut child_bindings = HashMap::new();
                for projection in &expressions {
                    if let Some(index) = bindings.get(&projection.attribute.id)
                        && let Some(attribute) = reference_id(&projection.expression)
                    {
                        child_bindings.insert(attribute, *index);
                    }
                }
                let input = self.replace_write_defaults(*input, &child_bindings, defaults)?;
                let schema = input.schema.resolved("DEFAULT child output")?;
                let expressions = expressions
                    .into_iter()
                    .map(|projection| {
                        let expression = projection.expression.transform_up::<PlanError>(
                            &mut |mut expression| {
                                if let Deferred::Resolved(ExprKind::Attribute(id)) =
                                    &expression.kind
                                {
                                    expression.result_type =
                                        schema.attribute(*id)?.result_type.clone();
                                    Ok(expression)
                                } else {
                                    self.retype(expression)
                                }
                            },
                        )?;
                        let attribute = Attribute {
                            result_type: expression.result_type.clone(),
                            ..projection.attribute
                        };
                        Ok(Projection {
                            expression,
                            attribute,
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let schema = QuerySchema {
                    attributes: expressions.iter().map(|p| p.attribute.clone()).collect(),
                };
                Ok(QueryNode {
                    kind: Deferred::Resolved(QueryKind::Project {
                        input: Box::new(input),
                        expressions,
                    }),
                    schema: Deferred::Resolved(schema),
                })
            }
            node => Ok(QueryNode {
                kind: Deferred::Resolved(node),
                schema: Deferred::Resolved(original),
            }),
        }
    }
}

fn is_default(expression: &ExprNode) -> bool {
    match &expression.kind {
        Deferred::Unresolved(request) => matches!(request.as_ref(), spec::Expr::DefaultColumnValue),
        Deferred::Resolved(
            ExprKind::WithMetadata(child)
            | ExprKind::Cast {
                expression: child, ..
            },
        ) => is_default(child),
        _ => false,
    }
}

fn reference_id(expression: &ExprNode) -> Option<AttributeId> {
    match &expression.kind {
        Deferred::Resolved(ExprKind::Attribute(id)) => Some(*id),
        Deferred::Resolved(ExprKind::WithMetadata(child)) => reference_id(child),
        _ => None,
    }
}
