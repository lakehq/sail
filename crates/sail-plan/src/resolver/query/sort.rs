use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_sort(
        &mut self,
        input: spec::QueryPlan,
        order: Vec<spec::SortOrder>,
        is_global: bool,
    ) -> PlanResult<Query> {
        Ok({
            let mut input = self.resolve_query_plan(input).await?;
            let output = input.schema.resolved("sort output")?.clone();
            let visible = output
                .attributes
                .iter()
                .filter(|a| !a.hidden)
                .collect::<Vec<_>>();
            let mut resolved = vec![];
            for sort in order {
                let ascending = !matches!(sort.direction, spec::SortDirection::Descending);
                let expression = if let Some(position) =
                    crate::resolver::query::aggregate_request::ordinal(&sort.child)
                {
                    if position < 1 || position as usize > visible.len() {
                        return Err(PlanError::invalid(format!(
                            "Cannot resolve column position {position}. Valid positions are 1 to {}.",
                            visible.len()
                        )));
                    }
                    self.reference(visible[position as usize - 1])?
                } else {
                    let (next, expression) =
                        self.recover_expression(input, *sort.child, true).await?;
                    input = next;
                    expression.expression
                };
                let expression =
                    if matches!(expression.kind, Deferred::Resolved(ExprKind::Attribute(_))) {
                        expression
                    } else {
                        let (next, attribute) =
                            self.append(input, expression, "sort_key".into())?;
                        input = next;
                        self.reference(&attribute)?
                    };
                resolved.push(SortKey {
                    expression,
                    ascending,
                    nulls_first: match sort.null_ordering {
                        spec::NullOrdering::NullsFirst => true,
                        spec::NullOrdering::NullsLast => false,
                        spec::NullOrdering::Unspecified => ascending,
                    },
                });
            }
            let schema = input.schema.clone();
            let result = Query {
                kind: Deferred::Resolved(QueryKind::Sort {
                    input: Box::new(input),
                    order: resolved,
                    global: is_global,
                    required: false,
                }),
                schema,
            };
            self.restore_output(result, output)?
        })
    }
}
