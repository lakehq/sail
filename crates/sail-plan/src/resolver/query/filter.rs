use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Filter, LogicalPlan, Operator};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::in_subquery_nullable::InSubqueryNullable;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::tree::exists::ExistsRewriter;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_filter(
        &self,
        input: spec::QueryPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let output_schema = Arc::clone(input.schema());
        let (predicate, input) = self
            .resolve_expressions_with_missing_inputs(vec![condition], input, state)
            .await?;
        let (input, predicate) = self.rewrite_partition_expressions(input, predicate, state)?;
        let predicate = remove_tuple_in_annotation(predicate.one()?)?;
        let mut rewriter = ExistsRewriter::new_from_plan(input, state);
        let predicate = rewrite_filter_in(predicate, true, &mut rewriter)?;
        let filter =
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(rewriter.into_plan()))?);
        Self::restore_missing_input_output(filter, output_schema)
    }
}

/// Spark lowers value-observing IN contexts to existence marks, not SQL UNKNOWN.
/// Leave truth-only predicates to the optimizer's semi/anti and mark join paths.
fn rewrite_filter_in(
    expr: Expr,
    truth_only: bool,
    rewriter: &mut ExistsRewriter<'_>,
) -> Result<Expr> {
    if !expr.exists(|expr| Ok(matches!(expr, Expr::InSubquery(_))))? {
        return Ok(expr);
    }
    // Spark simplifies Boolean comparisons before rewriting predicate subqueries.
    // In particular, `IN = false` is null-aware NOT IN, unlike `IN <=> false`.
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr
        && matches!(op, Operator::Eq | Operator::NotEq)
    {
        let comparison = match (left.as_ref(), right.as_ref()) {
            (Expr::Literal(ScalarValue::Boolean(Some(value)), _), child)
            | (child, Expr::Literal(ScalarValue::Boolean(Some(value)), _)) => {
                Some((child, *value == (*op == Operator::NotEq)))
            }
            _ => None,
        };
        if let Some((child, negated)) = comparison {
            if truth_only {
                return Ok(expr
                    .map_children(|child| {
                        Ok(Transformed::yes(rewrite_filter_in(child, true, rewriter)?))
                    })?
                    .data);
            }
            return rewrite_filter_in(
                if negated {
                    !child.clone()
                } else {
                    child.clone()
                },
                false,
                rewriter,
            );
        }
    }
    match expr {
        Expr::InSubquery(mut query) => {
            query.expr = Box::new(rewrite_filter_in(*query.expr, false, rewriter)?);
            let expr = Expr::InSubquery(query);
            if truth_only {
                return Ok(expr);
            }
            let value = rewriter.f_up(expr)?.data;
            // No predicate field is exposed by a filter. Do not inhibit NULL
            // simplification with the analyzed output annotation used by SELECT.
            if let Expr::ScalarFunction(function) = &value
                && function.func.inner().is::<InSubqueryNullable>()
                && let [inner] = function.args.as_slice()
            {
                return Ok(inner.clone());
            }
            Ok(value)
        }
        Expr::Not(inner) if !truth_only => match *inner {
            Expr::InSubquery(mut query) => {
                query.negated = !query.negated;
                rewrite_filter_in(Expr::InSubquery(query), false, rewriter)
            }
            Expr::Not(inner) => rewrite_filter_in(*inner, false, rewriter),
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::And | Operator::Or) =>
            {
                let left = rewrite_filter_in(!*left, false, rewriter)?;
                let right = rewrite_filter_in(!*right, false, rewriter)?;
                Ok(if op == Operator::And {
                    left.or(right)
                } else {
                    left.and(right)
                })
            }
            inner => Ok(!rewrite_filter_in(inner, false, rewriter)?),
        },
        expr => {
            let truth_only = truth_only
                && matches!(
                    &expr,
                    Expr::Not(_)
                        | Expr::BinaryExpr(BinaryExpr {
                            op: Operator::And | Operator::Or,
                            ..
                        })
                );
            Ok(expr
                .map_children(|child| {
                    Ok(Transformed::yes(rewrite_filter_in(
                        child, truth_only, rewriter,
                    )?))
                })?
                .data)
        }
    }
}

/// Filters expose no predicate field. Keep their existing EXISTS simplification
/// and semi/anti joins; the source annotation is needed only when materialized.
fn remove_tuple_in_annotation(expr: Expr) -> Result<Expr> {
    Ok(expr
        .transform_up(|expr| {
            if let Expr::ScalarFunction(function) = &expr
                && function.func.inner().is::<InSubqueryNullable>()
                && let [inner @ Expr::Exists(_)] = function.args.as_slice()
            {
                return Ok(Transformed::yes(inner.clone()));
            }
            Ok(Transformed::no(expr))
        })?
        .data)
}

#[cfg(test)]
mod tests {
    use datafusion_common::DFSchema;
    use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{
        EmptyRelation, ExprSchemable, JoinType, LogicalPlanBuilder, col, expr_fn, lit,
    };
    use sail_common_datafusion::simplify_expressions::ExprSimplifier;

    use super::*;

    fn relation(name: &str) -> Result<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from_qualified_schema(
                name,
                &Schema::new(vec![Field::new("k", DataType::Int32, true)]),
            )?),
        }))
    }

    #[test]
    fn truth_only_filters_keep_membership_and_existence_paths() -> Result<()> {
        let input = relation("o")?;
        let query = Arc::new(
            LogicalPlanBuilder::from(relation("r")?)
                .project(vec![col("r.k")])?
                .build()?,
        );
        for expr in [
            expr_fn::in_subquery(col("o.k"), query.clone()),
            expr_fn::not_in_subquery(col("o.k"), query.clone()),
            expr_fn::exists(query),
        ] {
            for predicate in [
                expr.clone(),
                !expr.clone(),
                expr.clone().and(col("o.k").gt(lit(0))),
                col("o.k").gt(lit(0)).or(expr.clone()),
                expr.clone().eq(lit(false)),
                expr.clone().not_eq(lit(true)),
            ] {
                let mut state = PlanResolverState::new();
                let mut rewriter = ExistsRewriter::new_from_plan(input.clone(), &mut state);
                assert_eq!(
                    rewrite_filter_in(predicate.clone(), true, &mut rewriter)?,
                    predicate
                );
                assert_eq!(rewriter.into_plan(), input);
            }
        }
        Ok(())
    }

    #[test]
    fn value_observing_filters_use_one_existence_mark_without_output_annotation() -> Result<()> {
        for negated in [false, true] {
            let input = relation("o")?;
            let query = Arc::new(
                LogicalPlanBuilder::from(relation("r")?)
                    .project(vec![col("r.k")])?
                    .build()?,
            );
            let expr = if negated {
                expr_fn::not_in_subquery(col("o.k"), query)
            } else {
                expr_fn::in_subquery(col("o.k"), query)
            };
            let mut state = PlanResolverState::new();
            let mut rewriter = ExistsRewriter::new_from_plan(input.clone(), &mut state);
            let result = rewrite_filter_in(expr.is_null(), true, &mut rewriter)?;
            let plan = rewriter.into_plan();
            assert!(!result.exists(|expr| Ok(matches!(expr, Expr::InSubquery(_))
                || matches!(expr, Expr::ScalarFunction(function) if function.func.inner().is::<InSubqueryNullable>())))?);
            assert!(!result.nullable(plan.schema().as_ref())?);
            let simplifier = ExprSimplifier::new(
                SimplifyContext::builder()
                    .with_schema(plan.schema().clone())
                    .build(),
            );
            assert_eq!(simplifier.simplify(result)?, lit(false));
            let LogicalPlan::Join(join) = plan else {
                panic!("expected one existence join");
            };
            assert_eq!(join.join_type, JoinType::LeftMark);
            assert!(!join.null_aware);
            assert_eq!(join.left.as_ref(), &input);
            let LogicalPlan::SubqueryAlias(inner) = join.right.as_ref() else {
                panic!("expected one RHS");
            };
            let equal = Expr::Column(datafusion_common::Column::new(
                Some(inner.alias.clone()),
                "k",
            ))
            .eq(col("o.k"));
            assert_eq!(
                join.filter,
                Some(if negated {
                    equal.clone().or(equal.is_null())
                } else {
                    equal
                })
            );
        }
        Ok(())
    }

    #[test]
    fn filter_removes_only_unmaterialized_tuple_annotations() -> Result<()> {
        let query = Arc::new(
            LogicalPlanBuilder::empty(true)
                .project(vec![lit(1)])?
                .build()?,
        );
        for negated in [false, true] {
            let exists = if negated {
                expr_fn::not_exists(query.clone())
            } else {
                expr_fn::exists(query.clone())
            };
            let annotated = InSubqueryNullable::wrap(exists.clone(), true);
            assert_eq!(remove_tuple_in_annotation(annotated.clone())?, exists);
            assert_eq!(
                remove_tuple_in_annotation(annotated.is_null())?,
                exists.is_null()
            );
        }
        for expr in [
            expr_fn::in_subquery(lit(1), query),
            InSubqueryNullable::wrap(lit(false), true),
        ] {
            assert_eq!(remove_tuple_in_annotation(expr.clone())?, expr);
        }
        Ok(())
    }
}
