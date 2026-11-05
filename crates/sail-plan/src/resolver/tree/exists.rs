use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;

use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_nested::expr_fn::{array_element, make_array};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result, not_impl_err, plan_datafusion_err};
use datafusion_expr::expr_rewriter::strip_outer_reference;
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, expr_fn, ident, lit,
};
use sail_common_datafusion::decorrelate::PullUpCorrelatedExpr;
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_function::scalar::misc::in_subquery_nullable::InSubqueryNullable;

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

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::InSubquery(query) = expr {
            let inner = query.subquery.subquery;
            if inner.schema().fields().len() != 1 {
                return Err(plan_datafusion_err!(
                    "IN subquery must return exactly one column"
                ));
            }
            let nullable = query.expr.nullable(self.plan.schema().as_ref())?
                || inner.schema().field(0).is_nullable();
            let preserve = |expr| InSubqueryNullable::wrap(expr, nullable);
            let column = inner
                .schema()
                .columns()
                .into_iter()
                .next()
                .ok_or_else(|| plan_datafusion_err!("IN subquery has no output"))?;
            if inner.all_out_ref_exprs().is_empty() {
                // Evaluate an uncorrelated RHS in full before testing membership.
                // In particular, its scalar aggregate must retain its empty-input row.
                let alias = self.state.register_field_name("");
                let equal =
                    Expr::Column(Column::new(Some(alias.clone()), column.name)).eq(*query.expr);
                let predicate = if query.negated {
                    equal.clone().or(equal.is_null())
                } else {
                    equal
                };
                let inner = LogicalPlanBuilder::from(inner)
                    .alias(alias.clone())?
                    .build()?;
                let result = self.mark_join(inner, predicate, alias)?;
                return Ok(Transformed::yes(preserve(if query.negated {
                    !result
                } else {
                    result
                })));
            }
            if Self::scalar_aggregate_has_filter(&inner) == Some(false) {
                // A global aggregate produces exactly one row even when its
                // correlated input is empty. Let scalar decorrelation preserve
                // that row before applying the membership comparison.
                let equal = (*query.expr).eq(expr_fn::scalar_subquery(inner));
                return Ok(Transformed::yes(preserve(coalesce(vec![
                    if query.negated { !equal } else { equal },
                    lit(false),
                ]))));
            }
            if Self::scalar_aggregate_has_filter(&inner) == Some(true) {
                // A scalar subquery uses NULL both for a missing row and for
                // a null value. A singleton array distinguishes HAVING's rejected
                // rows (NULL) from retained null values ([NULL]). Use DataFusion's
                // nullable make_array result so scalar-subquery nullability does
                // not let simplification discard the missing-row check.
                let packed = make_array(vec![Expr::Column(column)]);
                let inner = LogicalPlanBuilder::from(inner)
                    .project(vec![packed])?
                    .build()?;
                let field = self.state.register_field_name("");
                let mut expressions = self
                    .plan
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                expressions.push(expr_fn::scalar_subquery(Arc::new(inner)).alias(&field));
                let input = mem::replace(&mut self.plan, empty_logical_plan());
                self.plan = LogicalPlanBuilder::from(input)
                    .project(expressions)?
                    .build()?;
                let packed = ident(field);
                let value = array_element(packed.clone(), lit(1_i64));
                let equal = (*query.expr).eq(value);
                return Ok(Transformed::yes(preserve(
                    datafusion_expr::when(packed.is_null(), lit(query.negated)).otherwise(
                        coalesce(vec![if query.negated { !equal } else { equal }, lit(false)]),
                    )?,
                )));
            }
            let outer = query
                .expr
                .transform_up(|expr| {
                    if let Expr::Column(column) = expr {
                        let (_, field) = self.plan.schema().qualified_field_from_column(&column)?;
                        Ok(Transformed::yes(Expr::OuterReferenceColumn(
                            field.clone(),
                            column,
                        )))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?
                .data;
            let equal = Expr::Column(column).eq(outer);
            // Spark projects IN as an existence test. NOT IN additionally excludes
            // rows whose comparison can be unknown because either operand is null.
            let predicate = if query.negated {
                equal.clone().or(equal.is_null())
            } else {
                equal
            };
            let result = self
                .correlated_exists(LogicalPlanBuilder::from(inner).filter(predicate)?.build()?)?;
            return Ok(Transformed::yes(preserve(if query.negated {
                !result
            } else {
                result
            })));
        }
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
                            if Self::scalar_aggregate_has_filter(&limit.input).is_some() {
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
        // Qualify inner columns first so identical outer names retain their scope.
        let predicate = strip_outer_reference(predicate);
        self.mark_join(query, predicate, alias)
    }

    fn mark_join(&mut self, query: LogicalPlan, predicate: Expr, alias: String) -> Result<Expr> {
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

    /// Identify a scalar aggregate and whether a filter can remove its output row.
    fn scalar_aggregate_has_filter(query: &LogicalPlan) -> Option<bool> {
        match query {
            LogicalPlan::Projection(projection) => {
                Self::scalar_aggregate_has_filter(&projection.input)
            }
            LogicalPlan::SubqueryAlias(alias) => Self::scalar_aggregate_has_filter(&alias.input),
            LogicalPlan::Sort(sort) => Self::scalar_aggregate_has_filter(&sort.input),
            LogicalPlan::Filter(filter) => {
                Self::scalar_aggregate_has_filter(&filter.input).map(|_| true)
            }
            LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty().then_some(false),
            _ => None,
        }
    }
}

#[cfg(test)]
#[expect(clippy::panic, reason = "Assert typed plan shapes in tests")]
mod tests {
    use datafusion_common::DFSchema;
    use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{EmptyRelation, col, out_ref_col};

    use super::*;

    fn input(alias: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]);
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from_qualified_schema(alias, &schema)?),
        }))
    }

    #[test]
    fn scalar_in_preserves_source_nullability_without_changing_marks() -> Result<()> {
        for left_nullable in [false, true] {
            for right_nullable in [false, true] {
                for negated in [false, true] {
                    for correlated in [false, true] {
                        let relation = |alias: &str, nullable| -> Result<LogicalPlan> {
                            let schema = Schema::new(vec![
                                Field::new("k", DataType::Int64, nullable),
                                // An unrelated correlation key must not make IN nullable.
                                Field::new("v", DataType::Int64, true),
                            ]);
                            Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::new(DFSchema::try_from_qualified_schema(
                                    alias, &schema,
                                )?),
                            }))
                        };
                        let outer = relation("o", left_nullable)?;
                        let mut inner = LogicalPlanBuilder::from(relation("r", right_nullable)?);
                        if correlated {
                            inner =
                                inner.filter(col("r.v").gt(out_ref_col(DataType::Int64, "o.v")))?;
                        }
                        let inner = Arc::new(inner.project(vec![col("r.k")])?.build()?);
                        let expr = if negated {
                            expr_fn::not_in_subquery(col("o.k"), inner)
                        } else {
                            expr_fn::in_subquery(col("o.k"), inner)
                        };
                        let mut state = PlanResolverState::new();
                        let mut rewriter = ExistsRewriter::new_from_plan(outer.clone(), &mut state);
                        let result = expr.rewrite(&mut rewriter)?.data;
                        let plan = rewriter.into_plan();
                        assert_eq!(
                            result.nullable(plan.schema().as_ref())?,
                            left_nullable || right_nullable
                        );
                        assert!(
                            result
                                .to_field(plan.schema().as_ref())?
                                .1
                                .metadata()
                                .is_empty()
                        );
                        let value = if left_nullable || right_nullable {
                            let Expr::ScalarFunction(function) = result else {
                                panic!("nullable IN must retain its typed annotation");
                            };
                            assert!(function.func.inner().is::<InSubqueryNullable>());
                            assert_eq!(function.args.len(), 1);
                            function.args[0].clone()
                        } else {
                            result
                        };
                        let LogicalPlan::Join(join) = &plan else {
                            panic!("expected a single membership mark join");
                        };
                        assert_eq!(join.join_type, JoinType::LeftMark);
                        assert_eq!(join.left.as_ref(), &outer);
                        let LogicalPlan::SubqueryAlias(inner) = join.right.as_ref() else {
                            panic!("expected requalified RHS");
                        };
                        let mark = coalesce(vec![
                            Expr::Column(Column::new(Some(inner.alias.clone()), "mark")),
                            lit(false),
                        ]);
                        assert_eq!(value, if negated { !mark } else { mark });
                        assert!(plan.all_out_ref_exprs().is_empty());
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn correlated_mark_join_preserves_column_scopes_without_outer_references() -> Result<()> {
        // Also cover identical qualified names in distinct scopes, as in
        // DataFusion's decorrelate::tests::test_build_join_filter.
        for outer_alias in ["o", "t"] {
            for null_aware in [false, true] {
                let outer = input(outer_alias)?;
                let comparison = |inner, outer| {
                    let equal = Expr::eq(inner, outer);
                    if null_aware {
                        equal.clone().or(equal.is_null())
                    } else {
                        equal
                    }
                };
                let predicates = ["k", "v"].map(|name| {
                    comparison(
                        col(format!("t.{name}")),
                        out_ref_col(DataType::Int64, format!("{outer_alias}.{name}")),
                    )
                });
                let predicate = predicates[0]
                    .clone()
                    .and(predicates[1].clone())
                    .and(out_ref_col(DataType::Int64, format!("{outer_alias}.k")).gt(lit(0_i64)));
                let query = LogicalPlanBuilder::from(input("t")?)
                    .filter(predicate)?
                    .build()?;
                let mut state = PlanResolverState::new();
                let mut rewriter = ExistsRewriter::new_from_plan(outer.clone(), &mut state);
                let result = rewriter.correlated_exists(query)?;
                let plan = rewriter.into_plan();
                assert!(plan.all_out_ref_exprs().is_empty());
                let LogicalPlan::Join(join) = plan else {
                    panic!("expected mark join")
                };
                assert_eq!(join.join_type, JoinType::LeftMark);
                assert_eq!(join.left.as_ref(), &outer);
                assert!(join.on.is_empty());
                let LogicalPlan::SubqueryAlias(inner) = join.right.as_ref() else {
                    panic!("expected requalified inner input")
                };
                let inner_column =
                    |name| Expr::Column(Column::new(Some(inner.alias.clone()), name));
                let expected = comparison(inner_column("k"), col(format!("{outer_alias}.k")))
                    .and(comparison(
                        inner_column("v"),
                        col(format!("{outer_alias}.v")),
                    ))
                    .and(col(format!("{outer_alias}.k")).gt(lit(0_i64)));
                assert_eq!(join.filter, Some(expected));
                assert_eq!(result, coalesce(vec![inner_column("mark"), lit(false)]));
            }
        }
        Ok(())
    }
}
