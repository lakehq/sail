use datafusion_common::{Column, DFSchemaRef};
use datafusion_expr::expr;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_sort_order(
        &self,
        sort: spec::SortOrder,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let sort = self.resolve_sort_order(sort, true, schema, state).await?;
        Ok(NamedExpr::new(vec![], sort.expr))
    }

    pub(in super::super) async fn resolve_sort_order(
        &self,
        sort: spec::SortOrder,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Sort> {
        let spec::SortOrder {
            child,
            direction,
            null_ordering,
        } = sort;
        let expression = match child.as_ref() {
            spec::Expr::Literal(literal) if resolve_literals => {
                // Ordinals refer only to the visible output, even when other sort
                // keys are resolved against a combined descendant schema.
                let schema = Self::local_schema(schema, state);
                let num_fields = schema.fields().len();
                // The position keeps its SIGN: `-1 as usize` wrapped to 2^64-1 and named a position
                // nobody wrote. Spark reports the index it read, negative included
                // (`Analyzer.scala:2157-2161`, `QueryCompilationErrors.scala:698-705`).
                let position = match literal {
                    // ONLY an INT literal is an ordinal (`TryExtractOrdinal.scala:30-34`,
                    // `AstBuilder.scala:7591`); a BIGINT is a constant, so `ORDER BY 1L` sorts by
                    // nothing and `ORDER BY 5L` answers instead of naming a position out of range.
                    spec::Literal::Int32 { value: Some(value) } => i64::from(*value),
                    _ => {
                        return Ok(Self::sort_with_options(
                            self.resolve_expression(*child, &schema, state).await?,
                            direction,
                            null_ordering,
                        ));
                    }
                };
                let index = usize::try_from(position)
                    .ok()
                    .filter(|index| *index > 0 && *index <= num_fields);
                if let Some(index) = index {
                    return Ok(Self::sort_with_options(
                        expr::Expr::Column(Column::from(schema.qualified_field(index - 1))),
                        direction,
                        null_ordering,
                    ));
                } else {
                    return Err(PlanError::invalid(format!(
                        "[ORDER_BY_POS_OUT_OF_RANGE] ORDER BY position {position} is not in select \
                         list (valid range is [1, {num_fields}])."
                    )));
                }
            }
            _ => self.resolve_expression(*child, schema, state).await?,
        };
        Ok(Self::sort_with_options(
            expression,
            direction,
            null_ordering,
        ))
    }

    pub(in super::super) fn sort_with_options(
        expr: expr::Expr,
        direction: spec::SortDirection,
        null_ordering: spec::NullOrdering,
    ) -> expr::Sort {
        let asc = !matches!(direction, spec::SortDirection::Descending);
        let nulls_first = match null_ordering {
            spec::NullOrdering::NullsFirst => true,
            spec::NullOrdering::NullsLast => false,
            spec::NullOrdering::Unspecified => asc,
        };
        expr::Sort {
            expr,
            asc,
            nulls_first,
        }
    }

    pub(in super::super) async fn resolve_sort_orders(
        &self,
        sort: Vec<spec::SortOrder>,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Sort>> {
        let mut results: Vec<expr::Sort> = Vec::with_capacity(sort.len());
        for s in sort {
            let expr = self
                .resolve_sort_order(s, resolve_literals, schema, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }
}
