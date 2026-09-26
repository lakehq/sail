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
                let schema = state.get_local_schema(schema);
                let num_fields = schema.fields().len();
                let position = match literal {
                    spec::Literal::Int32 { value: Some(value) } => Some(*value as usize),
                    spec::Literal::Int64 { value: Some(value) } => Some(*value as usize),
                    _ => None,
                };
                match position {
                    Some(position) if position > 0 && position <= num_fields => {
                        expr::Expr::Column(Column::from(schema.qualified_field(position - 1)))
                    }
                    Some(position) => {
                        return Err(PlanError::invalid(format!(
                            "Cannot resolve column position {position}. Valid positions are 1 to {num_fields}."
                        )));
                    }
                    None => self.resolve_expression(*child, &schema, state).await?,
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
