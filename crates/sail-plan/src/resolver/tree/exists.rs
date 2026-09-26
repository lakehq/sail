use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;

use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result, not_impl_err, plan_datafusion_err};
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, expr_fn, ident, lit,
};
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

pub(crate) struct ExistsRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for ExistsRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for ExistsRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let mut inner = &expr;
        while let Expr::Not(child) = inner {
            inner = child;
        }
        if matches!(inner, Expr::InSubquery(subquery)
            if subquery.subquery.outer_ref_columns.is_empty())
        {
            return Ok(Transformed::no(expr));
        }
        let indirect_negation = matches!(expr, Expr::Not(_))
            || matches!(&expr, Expr::BinaryExpr(binary)
                if matches!(binary.op, Operator::Eq | Operator::NotEq));
        if indirect_negation
            && expr.exists(|child| {
                Ok(matches!(child, Expr::InSubquery(subquery)
                    if subquery.subquery.outer_ref_columns.is_empty()))
            })?
        {
            // TODO: Normalize indirect negation before decorrelation with the query's
            // optimizer context; early constant folding can change stable functions.
            return not_impl_err!("projected IN under indirect negation or Boolean comparison");
        }
        Ok(Transformed::no(expr))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let Expr::Exists(exists) = expr else {
            return Ok(Transformed::no(expr));
        };
        let result = if exists.subquery.outer_ref_columns.is_empty() {
            let query = LogicalPlanBuilder::from(exists.subquery.subquery)
                .project(Vec::<Expr>::new())?
                .limit(0, Some(1))?
                .build()?;
            self.count_rows(query, 0)?
        } else {
            self.correlated_exists(Arc::unwrap_or_clone(exists.subquery.subquery))?
        };
        Ok(Transformed::yes(if exists.negated {
            !result
        } else {
            result
        }))
    }
}

impl ExistsRewriter<'_> {
    fn count_rows(&mut self, query: LogicalPlan, skip: usize) -> Result<Expr> {
        let output = self.state.register_field_name("");
        let skip = i64::try_from(skip).map_err(|error| plan_datafusion_err!("{error}"))?;
        // COUNT produces one non-null result even when the subquery has no rows.
        let query = LogicalPlanBuilder::from(query)
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64)).alias(&output)])?
            .project(vec![ident(output).gt(lit(skip))])?
            .build()?;
        Self::validate_correlated_operators(&query)?;
        Ok(coalesce(vec![
            expr_fn::scalar_subquery(Arc::new(query)),
            lit(false),
        ]))
    }

    fn correlated_exists(&mut self, query: LogicalPlan) -> Result<Expr> {
        let mut query = query
            .transform_up(|plan| {
                let LogicalPlan::Limit(mut limit) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let evaluator = LiteralEvaluator::new();
                for bound in [&mut limit.skip, &mut limit.fetch].into_iter().flatten() {
                    if bound.is_volatile() {
                        return Err(plan_datafusion_err!("LIMIT/OFFSET must be constant"));
                    }
                    let value = evaluator.evaluate(bound)?;
                    **bound = lit(LiteralValue(&value).try_to_i64()?);
                }
                Ok(Transformed::yes(LogicalPlan::Limit(limit)))
            })
            .data()?;
        let query = loop {
            query = match query {
                LogicalPlan::Projection(projection) => Arc::unwrap_or_clone(projection.input),
                LogicalPlan::SubqueryAlias(alias) => Arc::unwrap_or_clone(alias.input),
                LogicalPlan::Sort(sort) => Arc::unwrap_or_clone(sort.input),
                LogicalPlan::Aggregate(ref aggregate) if aggregate.group_expr.is_empty() => {
                    return Ok(lit(true));
                }
                LogicalPlan::Limit(limit) => {
                    if matches!(limit.get_fetch_type()?, FetchType::Literal(Some(0))) {
                        return Ok(lit(false));
                    }
                    match limit.get_skip_type()? {
                        SkipType::Literal(0) => Arc::unwrap_or_clone(limit.input),
                        SkipType::Literal(skip) => {
                            if Self::is_scalar_aggregate(&limit.input) {
                                return Ok(lit(false));
                            }
                            if limit.input.exists(|plan| {
                                Ok(matches!(plan, LogicalPlan::Aggregate(aggregate) if aggregate.group_expr.is_empty()))
                            })? {
                                return not_impl_err!("projected correlated EXISTS with OFFSET over nested scalar aggregation");
                            }
                            return self.count_rows(Arc::unwrap_or_clone(limit.input), skip);
                        }
                        _ => {
                            return not_impl_err!(
                                "projected correlated EXISTS with non-literal OFFSET"
                            );
                        }
                    }
                }
                query => break query,
            };
        };
        // Pulling these operators above correlation would change empty-input semantics.
        if query.exists(|plan| {
            Ok(match plan {
                LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
                LogicalPlan::Limit(limit) => {
                    !matches!(limit.get_skip_type()?, SkipType::Literal(0))
                        || !limit.input.all_out_ref_exprs().is_empty()
                }
                _ => false,
            })
        })? {
            return not_impl_err!(
                "projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET"
            );
        }
        Self::validate_correlated_operators(&query)?;
        let mut pull_up = PullUpCorrelatedExpr::new();
        let query = query.rewrite(&mut pull_up).data()?;
        if !pull_up.can_pull_up {
            return not_impl_err!("projected EXISTS with unsupported correlation");
        }
        let columns: BTreeSet<_> = pull_up
            .correlated_subquery_cols_map
            .into_values()
            .flatten()
            .collect();
        let alias = self.state.register_field_name("");
        let predicate = conjunction(pull_up.join_filters).unwrap_or_else(|| lit(true));
        let predicate = predicate
            .transform_up(|expr| match expr {
                Expr::Column(column) if columns.contains(&column) => Ok(Transformed::yes(
                    Expr::Column(Column::new(Some(alias.clone()), column.name)),
                )),
                expr => Ok(Transformed::no(expr)),
            })
            .data()?;
        let query = LogicalPlanBuilder::from(query)
            .alias(alias.clone())?
            .build()?;
        let input = mem::replace(&mut self.plan, empty_logical_plan());
        self.plan = LogicalPlanBuilder::from(input)
            .join_on(query, JoinType::LeftMark, Some(predicate))?
            .build()?;
        Ok(coalesce(vec![
            Expr::Column(Column::new(Some(alias), "mark")),
            lit(false),
        ]))
    }

    fn validate_correlated_operators(query: &LogicalPlan) -> Result<()> {
        query.apply(|plan| {
            match plan {
                LogicalPlan::Window(window) if !window.input.all_out_ref_exprs().is_empty() => {
                    // Correlation must remain part of the window's input or partitioning.
                    return not_impl_err!(
                        "projected correlated EXISTS with correlation below a window"
                    );
                }
                LogicalPlan::Aggregate(aggregate) => {
                    let has_correlated_cast = aggregate.input.exists(|plan| {
                        let LogicalPlan::Filter(filter) = plan else {
                            return Ok(false);
                        };
                        for predicate in split_conjunction(&filter.predicate) {
                            if predicate.contains_outer()
                                && predicate.exists(|expr| {
                                    Ok(match expr {
                                        Expr::Cast(cast) => cast.expr.any_column_refs(),
                                        Expr::TryCast(cast) => cast.expr.any_column_refs(),
                                        _ => false,
                                    })
                                })?
                            {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    })?;
                    if has_correlated_cast {
                        // Grouping by the source column can split equal cast results.
                        return not_impl_err!(
                            "projected correlated EXISTS with cast correlation below aggregation"
                        );
                    }
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn is_scalar_aggregate(query: &LogicalPlan) -> bool {
        match query {
            LogicalPlan::Projection(projection) => Self::is_scalar_aggregate(&projection.input),
            LogicalPlan::SubqueryAlias(alias) => Self::is_scalar_aggregate(&alias.input),
            LogicalPlan::Sort(sort) => Self::is_scalar_aggregate(&sort.input),
            LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
            _ => false,
        }
    }
}
