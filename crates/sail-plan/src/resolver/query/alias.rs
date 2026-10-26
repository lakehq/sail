use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn rename(
        &mut self,
        input: Query,
        names: Vec<spec::Identifier>,
    ) -> PlanResult<Query> {
        let schema = input.schema.resolved("rename input")?;
        if names.len() != schema.attributes.len() {
            return Err(PlanError::analysis(
                "column alias count does not match input width",
            ));
        }
        let expressions = schema
            .attributes
            .iter()
            .zip(names)
            .map(|(attribute, name)| {
                Ok(NamedExpression {
                    name: name.into(),
                    expression: Expr {
                        id: self.context.ids.expression(),
                        result_type: attribute.result_type.clone(),
                        kind: Deferred::Resolved(ExprKind::WithMetadata(Box::new(
                            self.reference(attribute)?,
                        ))),
                    },
                })
            })
            .collect::<PlanResult<_>>()?;
        self.project(input, expressions)
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_subquery_alias(
        &mut self,
        input: spec::QueryPlan,
        alias: spec::Identifier,
        qualifier: Vec<spec::Identifier>,
    ) -> PlanResult<Query> {
        Ok({
            let input = self.resolve_query_plan(input).await?;
            let mut input = self.visible_output(input)?;
            let mut qualifier: Vec<String> = qualifier.into_iter().map(String::from).collect();
            qualifier.push(alias.into());
            let mut schema = input.schema.into_resolved("alias input")?;
            for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                attribute.qualifier.clone_from(&qualifier);
                attribute.qualified_position = position;
            }
            input.schema = Deferred::Resolved(schema);
            Query {
                schema: input.schema.clone(),
                kind: Deferred::Resolved(QueryKind::Scope {
                    input: Box::new(input),
                }),
            }
        })
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_table_alias(
        &mut self,
        input: spec::QueryPlan,
        name: spec::Identifier,
        columns: Vec<spec::Identifier>,
    ) -> PlanResult<Query> {
        Ok({
            let input = self.resolve_query_plan(input).await?;
            let input = self.visible_output(input)?;
            let mut input = if columns.is_empty() {
                input
            } else {
                self.rename(input, columns)?
            };
            let mut schema = input.schema.into_resolved("table alias input")?;
            for (position, attribute) in schema.attributes.iter_mut().enumerate() {
                attribute.qualifier = vec![name.as_ref().into()];
                attribute.qualified_position = position;
            }
            input.schema = Deferred::Resolved(schema);
            Query {
                schema: input.schema.clone(),
                kind: Deferred::Resolved(QueryKind::Scope {
                    input: Box::new(input),
                }),
            }
        })
    }
}
