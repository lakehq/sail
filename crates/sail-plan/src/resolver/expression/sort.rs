use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_sort_order(
        &mut self,
        sort: spec::SortOrder,
        schema: &QuerySchema,
    ) -> PlanResult<SortKey> {
        let ascending = !matches!(sort.direction, spec::SortDirection::Descending);
        Ok(SortKey {
            expression: self
                .resolve_named_expression(*sort.child, schema)
                .await?
                .expression,
            ascending,
            nulls_first: match sort.null_ordering {
                spec::NullOrdering::Unspecified => ascending,
                spec::NullOrdering::NullsFirst => true,
                spec::NullOrdering::NullsLast => false,
            },
        })
    }
}
