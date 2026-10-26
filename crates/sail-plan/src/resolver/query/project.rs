use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_project(
        &mut self,
        input: Option<spec::QueryPlan>,
        expressions: Vec<spec::Expr>,
    ) -> PlanResult<Query> {
        Ok({
            let mut input = match input {
                Some(input) => self.resolve_query_plan(input).await?,
                None => Self::empty(true),
            };
            let mut resolved = vec![];
            let replacements_start = self.expression_replacements.len();
            let schema = input.schema.resolved("project input")?.clone();
            for expression in expressions {
                if crate::resolver::tree::explode::is_generator(&expression) {
                    let (next, generated) = self.generator(input, expression).await?;
                    input = next;
                    resolved.extend(generated);
                } else if let spec::Expr::UnresolvedRegex { col_name, plan_id } = expression {
                    resolved.extend(self.expand_regex(&col_name, plan_id, &schema)?);
                } else if let spec::Expr::UnresolvedStar {
                    target,
                    plan_id,
                    wildcard_options,
                } = expression
                {
                    resolved.extend(
                        self.expand_star(target, plan_id, wildcard_options, &schema)
                            .await?,
                    );
                } else {
                    let mut generators = vec![];
                    crate::resolver::tree::explode::nested_generators(&expression, &mut generators);
                    for request in generators {
                        let (next, mut generated) = self.generator(input, request.clone()).await?;
                        input = next;
                        if generated.len() != 1 {
                            return Err(PlanError::analysis(
                                "nested generator must produce exactly one column",
                            ));
                        }
                        let generated = generated
                            .pop()
                            .ok_or_else(|| PlanError::internal("missing generator column"))?;
                        self.expression_replacements.push((request, generated));
                    }
                    resolved.push(
                        self.resolve_named_expression(
                            expression,
                            input.schema.resolved("project input")?,
                        )
                        .await?,
                    );
                }
            }
            self.expression_replacements.truncate(replacements_start);
            let has_aggregate = resolved
                .iter()
                .map(|named| {
                    Ok(named
                        .expression
                        .any(&|node| matches!(node, ExprKind::AggregateFunction { .. }))?)
                })
                .collect::<PlanResult<Vec<_>>>()?
                .into_iter()
                .any(|value| value);
            if has_aggregate {
                self.aggregate(input, vec![], resolved, None)?
            } else {
                self.project(input, resolved)?
            }
        })
    }
}
