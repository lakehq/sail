use std::sync::Arc;

use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, ScalarValue};
use datafusion_expr::logical_plan::{Filter, Limit, Projection, Sort};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    SortExpr, expr_fn, ident, lit, when,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_scalar_subquery::SparkScalarSubquery;

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

/// A LIMIT or ORDER BY found above the projection of an inlined subquery.
enum RowBound {
    Limit(Option<Box<Expr>>, Option<Box<Expr>>),
    Sort(Vec<SortExpr>, Option<usize>),
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

/// Inlines a subquery whose only outer references sit in a projection over an
/// uncorrelated input (e.g. `(SELECT o.id FROM range(1))`), which DataFusion cannot
/// decorrelate. The input is evaluated once by an uncorrelated scalar subquery that
/// returns the row count together with every referenced inner column, and the projected
/// expression is evaluated in the outer scope against that single result.
fn inline_outer_projection(
    plan: &LogicalPlan,
    state: &mut PlanResolverState,
) -> PlanResult<Option<Expr>> {
    // Walk down through LIMIT, ORDER BY and column selections to the projection that
    // carries the outer references, tracking which of its outputs the subquery returns.
    let mut bounds = vec![];
    let mut selected: Option<Column> = None;
    let mut node = plan;
    let (projection, expr) = loop {
        node = match node {
            LogicalPlan::Limit(limit) => {
                bounds.push(RowBound::Limit(limit.skip.clone(), limit.fetch.clone()));
                limit.input.as_ref()
            }
            LogicalPlan::Sort(sort) => {
                bounds.push(RowBound::Sort(sort.expr.clone(), sort.fetch));
                sort.input.as_ref()
            }
            LogicalPlan::Projection(projection) => {
                if let Some(columns) = selected_columns(projection) {
                    let column = match &selected {
                        None => match columns.as_slice() {
                            [(_, column)] => column.clone(),
                            _ => return Ok(None),
                        },
                        Some(selected) => match columns
                            .into_iter()
                            .find(|(name, _)| name == selected.name())
                        {
                            Some((_, column)) => column,
                            None => return Ok(None),
                        },
                    };
                    selected = Some(column);
                    projection.input.as_ref()
                } else {
                    let expr = match &selected {
                        None => match projection.expr.as_slice() {
                            [expr] => expr,
                            _ => return Ok(None),
                        },
                        Some(selected) => match projection
                            .expr
                            .iter()
                            .find(|expr| expr.schema_name().to_string() == selected.name())
                        {
                            Some(expr) => expr,
                            None => return Ok(None),
                        },
                    };
                    break (projection, expr);
                }
            }
            _ => return Ok(None),
        };
    };
    if !expr.contains_outer() || !projection.input.all_out_ref_exprs().is_empty() {
        return Ok(None);
    }
    // A sort only matters below a LIMIT (or its own fetch); otherwise the single row the
    // subquery may return makes the order irrelevant. A sort that matters moves below the
    // projection, so it may only refer to the projection's input.
    let input_columns = projection.input.schema().columns();
    let mut limited = false;
    let mut kept = vec![];
    for bound in bounds {
        match bound {
            RowBound::Limit(..) => {
                limited = true;
                kept.push(bound);
            }
            RowBound::Sort(ref exprs, fetch) => {
                if !limited && fetch.is_none() {
                    continue;
                }
                let on_input = exprs.iter().all(|sort| {
                    !sort.expr.contains_outer()
                        && sort
                            .expr
                            .column_refs()
                            .iter()
                            .all(|column| input_columns.contains(column))
                });
                if !on_input {
                    return Ok(None);
                }
                limited |= fetch.is_some();
                kept.push(bound);
            }
        }
    }
    let mut input = projection.input.as_ref().clone();
    for bound in kept.into_iter().rev() {
        input = match bound {
            RowBound::Limit(skip, fetch) => LogicalPlan::Limit(Limit {
                skip,
                fetch,
                input: Arc::new(input),
            }),
            RowBound::Sort(expr, fetch) => LogicalPlan::Sort(Sort {
                expr,
                input: Arc::new(input),
                fetch,
            }),
        };
    }
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref().clone(),
        expr => expr.clone(),
    };
    let mut columns: Vec<Column> = vec![];
    expr.apply(|expr| {
        if let Expr::Column(column) = expr
            && !columns.contains(column)
        {
            columns.push(column.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let rows = state.register_field_name("");
    let values = columns
        .iter()
        .map(|_| state.register_field_name(""))
        .collect::<Vec<_>>();
    let aggregates =
        std::iter::once(count(lit(1_i64)).alias(&rows))
            .chain(columns.iter().zip(&values).map(|(column, name)| {
                first_value(Expr::Column(column.clone()), vec![]).alias(name)
            }))
            .collect::<Vec<_>>();
    let fields = std::iter::once(&rows)
        .chain(&values)
        .flat_map(|name| [lit(name.as_str()), ident(name)])
        .collect::<Vec<_>>();
    let producer = LogicalPlanBuilder::from(input)
        .aggregate(Vec::<Expr>::new(), aggregates)?
        .project(vec![
            named_struct(fields).alias(state.register_field_name("")),
        ])?
        .build()?;
    let producer = expr_fn::scalar_subquery(Arc::new(producer));
    let expr = expr
        .transform(|expr| match expr {
            Expr::OuterReferenceColumn(_, column) => Ok(Transformed::yes(Expr::Column(column))),
            Expr::Column(column) => match columns.iter().position(|c| c == &column) {
                Some(index) => Ok(Transformed::yes(get_field(
                    producer.clone(),
                    values[index].as_str(),
                ))),
                None => Ok(Transformed::no(Expr::Column(column))),
            },
            expr => Ok(Transformed::no(expr)),
        })?
        .data;
    let rows = get_field(producer, rows.as_str());
    Ok(Some(
        when(rows.clone().eq(lit(0_i64)), lit(ScalarValue::Null))
            .when(rows.gt(lit(1_i64)), scalar_subquery_error())
            .otherwise(expr)?,
    ))
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

/// Spark lets a correlated scalar subquery return at most one row and fails at runtime when it
/// returns more, while DataFusion only decorrelates aggregated correlated subqueries. Aggregate
/// the rows into the single value and raise Spark's error when more than one row is produced.
fn enforce_single_row(plan: LogicalPlan, state: &mut PlanResolverState) -> PlanResult<LogicalPlan> {
    if is_aggregated(&plan) || plan.max_rows().is_some_and(|rows| rows <= 1) {
        return Ok(plan);
    }
    let column = plan.schema().columns().one()?;
    let value = state.register_field_name("");
    let rows = state.register_field_name("");
    Ok(LogicalPlanBuilder::from(plan)
        .aggregate(
            Vec::<Expr>::new(),
            vec![
                first_value(Expr::Column(column.clone()), vec![]).alias(&value),
                count(lit(1_i64)).alias(&rows),
            ],
        )?
        .project(vec![
            when(ident(&rows).gt(lit(1_i64)), scalar_subquery_error())
                .otherwise(ident(&value))?
                .alias(column.name()),
        ])?
        .build()?)
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
            nullable_scalar_subquery(subquery)
        } else if let Some(inlined) = inline_outer_projection(&subquery, state)? {
            inlined
        } else {
            nullable_scalar_subquery(enforce_single_row(
                project_correlated_keys(subquery, state)?,
                state,
            )?)
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
