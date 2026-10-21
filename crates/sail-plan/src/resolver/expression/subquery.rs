use std::sync::Arc;

use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, ScalarValue};
use datafusion_expr::logical_plan::{Aggregate, Filter, Limit, Projection, Sort, SubqueryAlias};
use datafusion_expr::utils::{conjunction, disjunction, split_binary, split_conjunction};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    SortExpr, expr_fn, ident, lit, when,
};
use sail_common::spec;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_scalar_subquery::SparkScalarSubquery;
use sail_logical_plan::row_bounds::{integer_literal, row_bounds};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::predicate::coerce_timestamp_in_subquery_pair;
use crate::resolver::state::PlanResolverState;

/// Projects the computed inner side of a correlated equality (e.g. `CAST(i.k AS BIGINT) = o.id`)
/// below the filter so that the correlation key is a plain column. DataFusion groups the
/// decorrelated subquery by the inner columns of the predicate, so inner values that are only
/// equal after the computation would otherwise form separate groups and duplicate the outer row.
fn project_correlated_keys(
    plan: LogicalPlan,
    state: &mut PlanResolverState,
) -> PlanResult<LogicalPlan> {
    let output = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let keep_output = !matches!(plan, LogicalPlan::Projection(_));
    let plan = plan.transform_up(|plan| {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !filter.predicate.contains_outer() {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let mut keys = vec![];
        let predicate = filter
            .predicate
            .clone()
            .transform_up(|expr| {
                let Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) = &expr
                else {
                    return Ok(Transformed::no(expr));
                };
                let inner_is_left = match (left.contains_outer(), right.contains_outer()) {
                    (false, true) => true,
                    (true, false) => false,
                    _ => return Ok(Transformed::no(expr)),
                };
                let inner = if inner_is_left { left } else { right };
                if matches!(inner.as_ref(), Expr::Column(_)) || !inner.any_column_refs() {
                    return Ok(Transformed::no(expr));
                }
                let name = state.register_field_name("");
                keys.push(inner.as_ref().clone().alias(&name));
                let key = Box::new(Expr::Column(Column::from_name(name)));
                let (left, right) = if inner_is_left {
                    (key, right.clone())
                } else {
                    (left.clone(), key)
                };
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                })))
            })?
            .data;
        if keys.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let columns = filter
            .input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let input = LogicalPlanBuilder::from(Arc::unwrap_or_clone(filter.input))
            .project(columns.into_iter().chain(keys))?
            .build()?;
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::new(input),
        )?)))
    })?;
    if plan.transformed && keep_output {
        Ok(LogicalPlanBuilder::from(plan.data)
            .project(output)?
            .build()?)
    } else {
        Ok(plan.data)
    }
}

/// Wraps a resolved scalar subquery so that it reports Spark's nullability.
fn nullable_scalar_subquery(plan: LogicalPlan) -> Expr {
    ScalarUDF::from(SparkScalarSubquery::new()).call(vec![expr_fn::scalar_subquery(Arc::new(plan))])
}

fn scalar_subquery_error() -> Expr {
    ScalarUDF::from(RaiseError::new()).call(vec![lit("Scalar subquery returned more than one row")])
}

/// Returns the input column selected by each output of a projection that only
/// selects (and possibly renames) columns, or `None` for any other projection.
fn selected_columns(projection: &Projection) -> Option<Vec<(String, Column)>> {
    projection
        .expr
        .iter()
        .map(|expr| match expr {
            Expr::Column(column) => Some((column.name().to_string(), column.clone())),
            Expr::Alias(alias) => match alias.expr.as_ref() {
                Expr::Column(column) => Some((alias.name.clone(), column.clone())),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expr => expr,
    }
}

/// What a scalar subquery returns once LIMIT, ORDER BY and column selections above its
/// projection are peeled off: the selected output of that projection, the sort that decides
/// the row kept by `LIMIT 1` (in terms of the projection's outputs) and whether such a limit
/// exists. Only `LIMIT 1` changes the result: `LIMIT 0` is always empty and a larger limit
/// still leaves more than one row to fail on, so sorts matter only below `LIMIT 1`.
struct ScalarSubqueryShape<'a> {
    projection: &'a Projection,
    expr: &'a Expr,
    sorts: Vec<SortExpr>,
    single: bool,
    bounded: bool,
}

fn peel_scalar_subquery(plan: &LogicalPlan) -> Option<ScalarSubqueryShape<'_>> {
    let mut single = false;
    let mut bounded = false;
    let mut sorts: Option<Vec<SortExpr>> = None;
    let mut selected: Option<Column> = None;
    let mut node = plan;
    loop {
        node = match node {
            LogicalPlan::Limit(limit) => {
                bounded = true;
                let skip = limit.skip.as_deref().map_or(Some(0), integer_literal)?;
                let fetch = match limit.fetch.as_deref() {
                    None => None,
                    Some(fetch) => Some(integer_literal(fetch)?),
                };
                match (skip, fetch) {
                    (0, Some(0)) => return None,
                    (0, Some(1)) => {
                        single = true;
                        sorts = None;
                    }
                    (0, _) => {}
                    _ => return None,
                }
                limit.input.as_ref()
            }
            LogicalPlan::Sort(sort) => {
                bounded = true;
                match sort.fetch {
                    Some(0) => return None,
                    Some(1) => {
                        single = true;
                        sorts = Some(sort.expr.clone());
                    }
                    _ if single && sorts.is_none() => sorts = Some(sort.expr.clone()),
                    _ => {}
                }
                sort.input.as_ref()
            }
            LogicalPlan::Projection(projection)
                if matches!(
                    projection.input.as_ref(),
                    LogicalPlan::Limit(_) | LogicalPlan::Sort(_) | LogicalPlan::Projection(_)
                ) && let Some(columns) = selected_columns(projection) =>
            {
                let column = match &selected {
                    None => match columns.as_slice() {
                        [(_, column)] => column.clone(),
                        _ => return None,
                    },
                    Some(selected) => columns
                        .iter()
                        .find(|(name, _)| name == selected.name())
                        .map(|(_, column)| column.clone())?,
                };
                if let Some(sorts) = sorts.as_mut() {
                    for sort in sorts.iter_mut() {
                        let expr = std::mem::replace(&mut sort.expr, lit(0));
                        sort.expr = expr
                            .transform(|expr| match expr {
                                Expr::Column(column) => {
                                    match columns.iter().find(|(name, _)| name == column.name()) {
                                        Some((_, input)) => {
                                            Ok(Transformed::yes(Expr::Column(input.clone())))
                                        }
                                        None => Ok(Transformed::no(Expr::Column(column))),
                                    }
                                }
                                expr => Ok(Transformed::no(expr)),
                            })
                            .ok()?
                            .data;
                    }
                }
                selected = Some(column);
                projection.input.as_ref()
            }
            LogicalPlan::Projection(projection) => {
                let expr = match &selected {
                    None => match projection.expr.as_slice() {
                        [expr] => expr,
                        _ => return None,
                    },
                    Some(selected) => projection
                        .expr
                        .iter()
                        .find(|expr| expr.schema_name().to_string() == selected.name())?,
                };
                return Some(ScalarSubqueryShape {
                    projection,
                    expr,
                    sorts: sorts.unwrap_or_default(),
                    single,
                    bounded,
                });
            }
            _ => return None,
        };
    }
}

/// An expression that refers to the outer query only, with no inner column.
fn is_outer_only(expr: &Expr) -> bool {
    expr.contains_outer() && !expr.any_column_refs()
}

/// Splits a filter predicate at its first conjunct that has outer-only disjuncts, returning
/// the guard (those disjuncts), the predicate when the guard holds (the conjunct dropped) and
/// the predicate when it does not (the conjunct reduced to its remaining disjuncts).
fn split_filter_guard(predicate: &Expr) -> Option<(Expr, Option<Expr>, Expr)> {
    let conjuncts = split_conjunction(predicate);
    for (index, conjunct) in conjuncts.iter().enumerate() {
        let (outer, rest): (Vec<_>, Vec<_>) = split_binary(conjunct, Operator::Or)
            .into_iter()
            .cloned()
            .partition(is_outer_only);
        let Some(guard) = disjunction(outer) else {
            continue;
        };
        let others = |skip: usize| {
            conjuncts
                .iter()
                .enumerate()
                .filter(move |(i, _)| *i != skip)
                .map(|(_, expr)| (*expr).clone())
        };
        let when_true = conjunction(others(index));
        let when_false =
            conjunction(others(index).chain([disjunction(rest).unwrap_or(lit(false))]))
                .unwrap_or(lit(false));
        return Some((guard, when_true, when_false));
    }
    None
}

/// DataFusion decorrelates only predicates that compare inner and outer columns. A predicate
/// on the outer row alone, or an OR with such a disjunct, is hoisted out of the topmost spine
/// filter of a correlated subquery as a CASE guard: `(SELECT .. WHERE p)` becomes
/// `CASE WHEN p THEN (SELECT ..) ELSE (SELECT .. WHERE false) END`, and `WHERE r OR p`
/// becomes `CASE WHEN p THEN (SELECT ..) ELSE (SELECT .. WHERE r) END`. Returns the guard
/// and the two subquery plans.
fn split_outer_guard(plan: &LogicalPlan) -> PlanResult<Option<(Expr, LogicalPlan, LogicalPlan)>> {
    let split_input = |input: &Arc<LogicalPlan>| split_outer_guard(input);
    Ok(match plan {
        LogicalPlan::Filter(filter) => match split_filter_guard(&filter.predicate) {
            Some((guard, when_true, when_false)) => {
                let when_true = match when_true {
                    Some(predicate) => {
                        LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&filter.input))?)
                    }
                    None => filter.input.as_ref().clone(),
                };
                let when_false =
                    LogicalPlan::Filter(Filter::try_new(when_false, Arc::clone(&filter.input))?);
                Some((guard, when_true, when_false))
            }
            None => split_input(&filter.input)?
                .map(|(guard, when_true, when_false)| {
                    let rebuild = |input: LogicalPlan| {
                        Filter::try_new(filter.predicate.clone(), Arc::new(input))
                            .map(LogicalPlan::Filter)
                    };
                    (guard, rebuild(when_true), rebuild(when_false))
                })
                .map(|(guard, when_true, when_false)| {
                    Ok::<_, PlanError>((guard, when_true?, when_false?))
                })
                .transpose()?,
        },
        LogicalPlan::Projection(projection) => split_input(&projection.input)?
            .map(|(guard, when_true, when_false)| {
                let rebuild = |input: LogicalPlan| {
                    Projection::try_new(projection.expr.clone(), Arc::new(input))
                        .map(LogicalPlan::Projection)
                };
                Ok::<_, PlanError>((guard, rebuild(when_true)?, rebuild(when_false)?))
            })
            .transpose()?,
        LogicalPlan::Aggregate(aggregate) => split_input(&aggregate.input)?
            .map(|(guard, when_true, when_false)| {
                let rebuild = |input: LogicalPlan| {
                    Aggregate::try_new(
                        Arc::new(input),
                        aggregate.group_expr.clone(),
                        aggregate.aggr_expr.clone(),
                    )
                    .map(LogicalPlan::Aggregate)
                };
                Ok::<_, PlanError>((guard, rebuild(when_true)?, rebuild(when_false)?))
            })
            .transpose()?,
        LogicalPlan::SubqueryAlias(alias) => split_input(&alias.input)?
            .map(|(guard, when_true, when_false)| {
                let rebuild = |input: LogicalPlan| {
                    SubqueryAlias::try_new(Arc::new(input), alias.alias.clone())
                        .map(LogicalPlan::SubqueryAlias)
                };
                Ok::<_, PlanError>((guard, rebuild(when_true)?, rebuild(when_false)?))
            })
            .transpose()?,
        LogicalPlan::Sort(sort) => {
            split_input(&sort.input)?.map(|(guard, when_true, when_false)| {
                let rebuild = |input: LogicalPlan| {
                    LogicalPlan::Sort(Sort {
                        expr: sort.expr.clone(),
                        input: Arc::new(input),
                        fetch: sort.fetch,
                    })
                };
                (guard, rebuild(when_true), rebuild(when_false))
            })
        }
        LogicalPlan::Limit(limit) => {
            split_input(&limit.input)?.map(|(guard, when_true, when_false)| {
                let rebuild = |input: LogicalPlan| {
                    LogicalPlan::Limit(Limit {
                        skip: limit.skip.clone(),
                        fetch: limit.fetch.clone(),
                        input: Arc::new(input),
                    })
                };
                (guard, rebuild(when_true), rebuild(when_false))
            })
        }
        _ => None,
    })
}

/// The expression evaluating a resolved scalar subquery plan.
fn scalar_subquery_expr(plan: LogicalPlan, state: &mut PlanResolverState) -> PlanResult<Expr> {
    Ok(match rewrite_bounded_scalar_subquery(&plan, state)? {
        Some(rewritten) => rewritten,
        None => nullable_scalar_subquery(plan),
    })
}

fn is_aggregated(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => is_aggregated(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => is_aggregated(&alias.input),
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Filter(filter) => matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)),
        _ => false,
    }
}

/// Rewrites a scalar subquery that DataFusion cannot decorrelate: one that is correlated
/// without being aggregated (Spark allows at most one row and fails at runtime otherwise),
/// or one that evaluates outer references in its projection. A producer aggregate over the
/// projection's input returns, in one row, the row count and the value(s) the projection
/// needs; the result is computed from that row, raising Spark's error when more than one row
/// would have been returned and honoring `LIMIT 1` with its ordering through `first_value`.
/// The producer is a scalar subquery itself, so an uncorrelated input is evaluated exactly
/// once and a correlated one is decorrelated by DataFusion as an ordinary aggregate.
fn rewrite_bounded_scalar_subquery(
    plan: &LogicalPlan,
    state: &mut PlanResolverState,
) -> PlanResult<Option<Expr>> {
    let Some(shape) = peel_scalar_subquery(plan) else {
        return Ok(None);
    };
    let input = shape.projection.input.as_ref();
    let correlated = !input.all_out_ref_exprs().is_empty();
    let expr = strip_alias(shape.expr);
    let outer = expr.contains_outer();
    // DataFusion handles the other shapes itself: aggregated subqueries are decorrelated and
    // uncorrelated ones have their row count checked at execution. LIMIT and ORDER BY above a
    // correlated ungrouped aggregate are dropped, since its single row makes them no-ops and
    // DataFusion cannot decorrelate through them.
    if !outer && (!correlated || is_aggregated(input)) {
        return Ok(
            if correlated && shape.bounded && row_bounds(input) == (1, Some(1)) {
                let plan = LogicalPlanBuilder::from(input.clone())
                    .project(vec![shape.expr.clone()])?
                    .build()?;
                Some(nullable_scalar_subquery(plan))
            } else {
                None
            },
        );
    }
    // The sort keys refer to the projection's outputs; express them through its input.
    let outputs = shape
        .projection
        .expr
        .iter()
        .map(|expr| (expr.schema_name().to_string(), strip_alias(expr).clone()))
        .collect::<Vec<_>>();
    let mut sorts = vec![];
    for sort in shape.sorts {
        let key = sort
            .expr
            .transform(|expr| match expr {
                Expr::Column(column) => {
                    match outputs.iter().find(|(name, _)| name == column.name()) {
                        Some((_, output)) => Ok(Transformed::yes(output.clone())),
                        None => Ok(Transformed::no(Expr::Column(column))),
                    }
                }
                expr => Ok(Transformed::no(expr)),
            })?
            .data;
        if key.contains_outer() {
            return Ok(None);
        }
        sorts.push(SortExpr { expr: key, ..sort });
    }
    // The values the producer must return: the inner columns an outer-referencing expression
    // uses, or the projected expression itself.
    let mut values: Vec<Expr> = vec![];
    if outer {
        expr.apply(|expr| {
            if let Expr::Column(_) = expr
                && !values.contains(expr)
            {
                values.push(expr.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    } else {
        values.push(expr.clone());
    }
    let rows = state.register_field_name("");
    let names = values
        .iter()
        .map(|_| state.register_field_name(""))
        .collect::<Vec<_>>();
    let aggregates = std::iter::once(count(lit(1_i64)).alias(&rows))
        .chain(
            values
                .iter()
                .zip(&names)
                .map(|(value, name)| first_value(value.clone(), sorts.clone()).alias(name)),
        )
        .collect::<Vec<_>>();
    let fields = std::iter::once(&rows)
        .chain(&names)
        .flat_map(|name| [lit(name.as_str()), ident(name)])
        .collect::<Vec<_>>();
    let producer = LogicalPlanBuilder::from(input.clone())
        .aggregate(Vec::<Expr>::new(), aggregates)?
        .project(vec![
            named_struct(fields).alias(state.register_field_name("")),
        ])?
        .build()?;
    let producer = expr_fn::scalar_subquery(Arc::new(producer));
    let value = |index: usize| get_field(producer.clone(), names[index].as_str());
    let result = if outer {
        expr.clone()
            .transform(|expr| match expr {
                Expr::OuterReferenceColumn(_, column) => Ok(Transformed::yes(Expr::Column(column))),
                Expr::Column(_) => match values.iter().position(|value| value == &expr) {
                    Some(index) => Ok(Transformed::yes(value(index))),
                    None => Ok(Transformed::no(expr)),
                },
                expr => Ok(Transformed::no(expr)),
            })?
            .data
    } else {
        value(0)
    };
    let rows = get_field(producer, rows.as_str());
    let mut case = when(rows.clone().eq(lit(0_i64)), lit(ScalarValue::Null));
    if !shape.single {
        case = case.when(rows.gt(lit(1_i64)), scalar_subquery_error());
    }
    Ok(Some(case.otherwise(result)?))
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_in_subquery(
        &self,
        expr: spec::Expr,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut expr = self.resolve_expression(expr, schema, state).await?;
        let mut subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            let plan = self.resolve_query_plan(subquery, scope.state()).await?;
            // Wrap in a Projection so that DataFusion's head_output_expr() works
            // for plans without a top-level Projection (e.g. bare Extension nodes
            // like RangeNode from Spark Connect). SQL subqueries already have a
            // Projection on top, so wrapping them again can cause duplicate column
            // names during decorrelation.
            if matches!(plan, LogicalPlan::Projection(_)) {
                plan
            } else {
                let exprs: Vec<Expr> = plan
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
                LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?)
            }
        };
        // The RHS is not part of the parent expression tree, so timestamp coercion must update
        // both plans before DataFusion analyzes the IN-subquery comparison.
        if subquery.schema().fields().len() == 1 {
            let left_type = expr.get_type(schema.as_ref())?;
            let right_type = subquery.schema().field(0).data_type().clone();
            let right_column = subquery
                .schema()
                .columns()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    PlanError::internal("IN subquery should expose one output column")
                })?;
            let (coerced_left, coerced_right, _, right_changed) =
                coerce_timestamp_in_subquery_pair(
                    expr,
                    &left_type,
                    schema,
                    Expr::Column(right_column.clone()),
                    &right_type,
                    subquery.schema(),
                    &self.config,
                )?;
            expr = coerced_left;
            if right_changed {
                subquery = LogicalPlan::Projection(Projection::try_new(
                    vec![coerced_right.alias(right_column.name)],
                    Arc::new(subquery),
                )?);
            }
        }
        let in_subquery = if !negated {
            expr_fn::in_subquery(expr, Arc::new(subquery))
        } else {
            expr_fn::not_in_subquery(expr, Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], in_subquery))
    }

    pub(super) async fn resolve_expression_scalar_subquery(
        &self,
        subquery: spec::QueryPlan,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        // Spark reports every scalar subquery as nullable since an empty result is NULL.
        // DataFusion 55 derives nullability from the projected field, so wrap the expression.
        let subquery = if subquery.all_out_ref_exprs().is_empty() {
            subquery
        } else {
            project_correlated_keys(subquery, state)?
        };
        let subquery = match split_outer_guard(&subquery)? {
            Some((guard, when_true, when_false)) => {
                let guard = guard
                    .transform(|expr| match expr {
                        Expr::OuterReferenceColumn(_, column) => {
                            Ok(Transformed::yes(Expr::Column(column)))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })?
                    .data;
                when(guard, scalar_subquery_expr(when_true, state)?)
                    .otherwise(scalar_subquery_expr(when_false, state)?)?
            }
            None => scalar_subquery_expr(subquery, state)?,
        };
        Ok(NamedExpr::new(vec!["subquery".to_string()], subquery))
    }

    pub(super) async fn resolve_expression_exists(
        &self,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let exists = if !negated {
            expr_fn::exists(Arc::new(subquery))
        } else {
            expr_fn::not_exists(Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["exists".to_string()], exists))
    }

    /// Resolves a multi-column IN subquery by converting it to a correlated EXISTS.
    /// `(e1, e2, ...) IN (SELECT c1, c2, ... FROM sub)` becomes
    /// `[NOT] EXISTS (SELECT ... FROM sub WHERE c1 = e1 AND c2 = e2 AND ...)`
    pub(super) async fn resolve_multi_column_in_subquery(
        &self,
        in_subquery_values: Vec<spec::Expr>,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut outer_refs = Vec::with_capacity(in_subquery_values.len());
        for value in in_subquery_values {
            let expr = self.resolve_expression(value, schema, state).await?;
            // Recursively convert column references to OuterReferenceColumn so
            // DataFusion's decorrelation pass recognizes them as correlated
            // references. This handles both bare columns (e.g. `a`) and
            // expressions containing columns (e.g. `a + 1`).
            let outer_ref = expr
                .transform(|e| match e {
                    Expr::Column(ref col) => {
                        let (_, field) = schema.qualified_field_from_column(col)?;
                        Ok(Transformed::yes(Expr::OuterReferenceColumn(
                            field.clone(),
                            col.clone(),
                        )))
                    }
                    _ => Ok(Transformed::no(e)),
                })
                .data()?;
            outer_refs.push(outer_ref);
        }
        let subquery_plan = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let subquery_cols = subquery_plan.schema().columns();
        if subquery_cols.len() != outer_refs.len() {
            return Err(PlanError::invalid(format!(
                "IN subquery column count mismatch: {} values vs {} subquery columns",
                outer_refs.len(),
                subquery_cols.len()
            )));
        }
        let filter_expr = outer_refs
            .into_iter()
            .zip(subquery_cols)
            .map(|(outer, inner_col)| -> PlanResult<Expr> {
                let outer_type = outer.get_type(schema.as_ref())?;
                let (_, inner_field) = subquery_plan
                    .schema()
                    .qualified_field_from_column(&inner_col)?;
                let inner_type = inner_field.data_type().clone();
                let (outer, inner, _, _) = coerce_timestamp_in_subquery_pair(
                    outer,
                    &outer_type,
                    schema,
                    Expr::Column(inner_col),
                    &inner_type,
                    subquery_plan.schema(),
                    &self.config,
                )?;
                Ok(Expr::eq(inner, outer))
            })
            .collect::<PlanResult<Vec<_>>>()?
            .into_iter()
            .reduce(Expr::and)
            .ok_or_else(|| PlanError::invalid("empty IN subquery values"))?;
        let filtered = LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(subquery_plan))?);
        let exists_expr = if !negated {
            expr_fn::exists(Arc::new(filtered))
        } else {
            expr_fn::not_exists(Arc::new(filtered))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], exists_expr))
    }

    /// Resolves a Subquery by looking up the referenced plan from state and delegating to existing subquery resolvers.
    pub(super) async fn resolve_expression_subquery(
        &self,
        plan_id: i64,
        subquery_type: spec::SubqueryType,
        in_subquery_values: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery_plan = state.get_subquery_reference(plan_id).ok_or_else(|| {
            PlanError::invalid(format!(
                "subquery plan_id {plan_id} not found in WithRelations references"
            ))
        })?;

        match subquery_type {
            spec::SubqueryType::In => {
                if in_subquery_values.is_empty() {
                    return Err(PlanError::invalid("IN subquery missing value expression"));
                }
                if in_subquery_values.len() == 1 {
                    let expr = in_subquery_values.into_iter().next().ok_or_else(|| {
                        PlanError::invalid("IN subquery missing value expression")
                    })?;
                    self.resolve_expression_in_subquery(expr, subquery_plan, negated, schema, state)
                        .await
                } else {
                    self.resolve_multi_column_in_subquery(
                        in_subquery_values,
                        subquery_plan,
                        negated,
                        schema,
                        state,
                    )
                    .await
                }
            }
            spec::SubqueryType::Scalar => {
                self.resolve_expression_scalar_subquery(subquery_plan, schema, state)
                    .await
            }
            spec::SubqueryType::Exists => {
                self.resolve_expression_exists(subquery_plan, negated, schema, state)
                    .await
            }
        }
    }
}
