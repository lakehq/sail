use datafusion_common::{Column, DFSchemaRef};
use datafusion_expr::expr;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
        use spec::{NullOrdering, SortDirection};

        let spec::SortOrder {
            child,
            direction,
            null_ordering,
        } = sort;
        let asc = match direction {
            SortDirection::Ascending => true,
            SortDirection::Descending => false,
            SortDirection::Unspecified => true,
        };
        let nulls_first = match null_ordering {
            NullOrdering::NullsFirst => true,
            NullOrdering::NullsLast => false,
            NullOrdering::Unspecified => asc,
        };

        match child.as_ref() {
            spec::Expr::Literal(literal) if resolve_literals => {
                let num_fields = schema.fields().len();
                let position = match literal {
                    spec::Literal::Int32 { value: Some(value) } => *value as usize,
                    spec::Literal::Int64 { value: Some(value) } => *value as usize,
                    _ => {
                        return Ok(expr::Sort {
                            expr: self.resolve_expression(*child, schema, state).await?,
                            asc,
                            nulls_first,
                        })
                    }
                };
                if position > 0 && position <= num_fields {
                    Ok(expr::Sort {
                        expr: expr::Expr::Column(Column::from(
                            schema.qualified_field(position - 1),
                        )),
                        asc,
                        nulls_first,
                    })
                } else {
                    Err(PlanError::invalid(format!(
                        "Cannot resolve column position {position}. Valid positions are 1 to {num_fields}."
                    )))
                }
            }
            _ => Ok(expr::Sort {
                expr: self.resolve_expression(*child, schema, state).await?,
                asc,
                nulls_first,
            }),
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
