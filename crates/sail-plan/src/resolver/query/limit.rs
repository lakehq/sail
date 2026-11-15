use std::sync::Arc;

use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{
    EmptyRelation, Expr, Extension, Limit, LogicalPlan, Projection, Sort, SortExpr,
};
use sail_common::spec;
use sail_logical_plan::tail::TailNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_limit(
        &self,
        input: spec::QueryPlan,
        skip: Option<spec::Expr>,
        limit: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let skip = if let Some(skip) = skip {
            Some(self.resolve_expression(skip, input.schema(), state).await?)
        } else {
            None
        };
        let limit = if let Some(limit) = limit {
            Some(
                self.resolve_expression(limit, input.schema(), state)
                    .await?,
            )
        } else {
            None
        };
        Ok(LogicalPlan::Limit(Limit {
            skip: skip.map(Box::new),
            fetch: limit.map(Box::new),
            input: Arc::new(input),
        }))
    }

    pub(super) async fn resolve_query_tail(
        &self,
        input: spec::QueryPlan,
        limit: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let limit = self
            .resolve_expression(limit, input.schema(), state)
            .await?;
        let limit_num = match &limit {
            Expr::Literal(ScalarValue::Int8(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int16(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int32(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int64(Some(value)), _metadata) => Ok(*value),
            Expr::Literal(ScalarValue::UInt8(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt16(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt32(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt64(Some(value)), _metadata) => Ok(*value as i64),
            _ => Err(PlanError::invalid("`tail` limit must be an integer")),
        }?;
        let Ok(fetch) = usize::try_from(limit_num) else {
            return Err(PlanError::AnalysisError(format!(
                "The limit like expression \"{limit_num}\" is invalid. \
                 The tail expression must be equal to or greater than 0, but got {limit_num}."
            )));
        };
        if fetch == 0 {
            // Like Spark, do not evaluate any part of the input, including its subqueries.
            return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(input.schema()),
            }));
        }
        let output = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        // The tail depends on the order of the input rows. A projection above the sort
        // can hide the sort keys, after which the physical plan no longer knows the order
        // and may change it. Pass the sort keys through to the tail and remove them after it.
        let (input, keys) = match Self::carry_tail_sort_keys(input.clone(), state)? {
            Some((input, names, true)) => {
                let keys = names
                    .iter()
                    .map(|name| Ok(Expr::Column(qualified_column(&input, name)?)))
                    .collect::<PlanResult<Vec<_>>>()?;
                (input, keys)
            }
            _ => (input, vec![]),
        };
        let hidden = !keys.is_empty();
        let (input, _, _) = Self::require_input_sort_inner(input)?;
        let tail = LogicalPlan::Extension(Extension {
            node: Arc::new(TailNode::new(Arc::new(input), fetch).with_keys(keys)),
        });
        if !hidden {
            return Ok(tail);
        }
        Ok(LogicalPlan::Projection(Projection::try_new(
            output,
            Arc::new(tail),
        )?))
    }

    /// Exposes the keys of the sort that determines the order of `plan` as hidden columns.
    /// Returns the rewritten plan, the key column names, and whether a projection between
    /// the sort and the plan output could have hidden the keys. Returns `None` if the
    /// order of `plan` does not come from a sort through order-preserving nodes.
    fn carry_tail_sort_keys(
        plan: LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(LogicalPlan, Vec<String>, bool)>> {
        match plan {
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                // Evaluate the keys once below the sort, so that the sort and the tail
                // see the same values even for non-deterministic keys.
                let names = expr
                    .iter()
                    .map(|_| state.register_hidden_field_name("tail_sort_key"))
                    .collect::<Vec<_>>();
                let exprs = input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .chain(
                        expr.iter()
                            .zip(&names)
                            .map(|(sort, name)| sort.expr.clone().alias(name)),
                    )
                    .collect();
                let input = LogicalPlan::Projection(Projection::try_new(exprs, input)?);
                let expr = expr
                    .into_iter()
                    .zip(&names)
                    .map(|(sort, name)| SortExpr {
                        expr: Expr::Column(Column::from_name(name)),
                        ..sort
                    })
                    .collect();
                let sort = LogicalPlan::Sort(Sort {
                    expr,
                    input: Arc::new(input),
                    fetch,
                });
                Ok(Some((sort, names, false)))
            }
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let Some((input, names, _)) =
                    Self::carry_tail_sort_keys(Arc::unwrap_or_clone(input), state)?
                else {
                    return Ok(None);
                };
                let mut expr = expr;
                for name in &names {
                    expr.push(Expr::Column(qualified_column(&input, name)?));
                }
                let plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?);
                Ok(Some((plan, names, true)))
            }
            plan @ (LogicalPlan::Filter(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::SubqueryAlias(_)) => {
                let child = plan.inputs()[0].clone();
                let Some((child, names, hidden)) = Self::carry_tail_sort_keys(child, state)? else {
                    return Ok(None);
                };
                let plan = plan.with_new_exprs(plan.expressions(), vec![child])?;
                Ok(Some((plan, names, hidden)))
            }
            _ => Ok(None),
        }
    }
}

fn qualified_column(plan: &LogicalPlan, name: &str) -> PlanResult<Column> {
    let (qualifier, field) = plan.schema().qualified_field_with_unqualified_name(name)?;
    Ok(Column::from((qualifier, field)))
}
