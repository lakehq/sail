use std::sync::Arc;

use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::functions::expr_fn::greatest;
use datafusion::functions_aggregate::expr_fn::{count, first_value, nth_value};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, ScalarValue, TableReference};
use datafusion_expr::logical_plan::{
    Aggregate, EmptyRelation, Filter, Limit, Projection, Sort, SubqueryAlias,
};
use datafusion_expr::utils::{conjunction, disjunction, split_binary, split_conjunction};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    SortExpr, cast, expr_fn, ident, lit, when,
};
use sail_common::spec;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::spark_scalar_subquery::SparkScalarSubquery;
use sail_logical_plan::correlation::is_outer_only;
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
    wrap_scalar_subquery(expr_fn::scalar_subquery(Arc::new(plan)))
}

/// Marks `expr` as one scalar subquery expression with Spark's nullability.
fn wrap_scalar_subquery(expr: Expr) -> Expr {
    ScalarUDF::from(SparkScalarSubquery::new()).call(vec![expr])
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

fn strip_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    expr
}

/// A LIMIT or ORDER BY found above the projection of a scalar subquery, in root-to-leaf
/// order. Sort keys are kept in terms of the projection's outputs until the projection is
/// peeled.
enum RowBound {
    Limit { skip: usize, fetch: Option<usize> },
    Sort(Vec<SortExpr>, Option<usize>),
}

/// What a scalar subquery returns once LIMIT, ORDER BY and column selections above its
/// projection are peeled off: the selected output of that projection and the bounds above it.
struct ScalarSubqueryShape<'a> {
    projection: &'a Projection,
    expr: &'a Expr,
    bounds: Vec<RowBound>,
}

fn remap_sort_keys(bounds: &mut [RowBound], columns: &[(String, Column)]) -> Option<()> {
    for bound in bounds.iter_mut() {
        let RowBound::Sort(sorts, _) = bound else {
            continue;
        };
        for sort in sorts.iter_mut() {
            let expr = std::mem::replace(&mut sort.expr, lit(0));
            sort.expr = expr
                .transform(|expr| match expr {
                    Expr::Column(column) => {
                        match columns.iter().find(|(name, _)| name == column.name()) {
                            Some((_, input)) => Ok(Transformed::yes(Expr::Column(input.clone()))),
                            None => Ok(Transformed::no(Expr::Column(column))),
                        }
                    }
                    expr => Ok(Transformed::no(expr)),
                })
                .ok()?
                .data;
        }
    }
    Some(())
}

fn peel_scalar_subquery(plan: &LogicalPlan) -> Option<ScalarSubqueryShape<'_>> {
    let mut bounds = vec![];
    let mut selected: Option<Column> = None;
    let mut node = plan;
    loop {
        node = match node {
            LogicalPlan::Limit(limit) => {
                let skip = limit.skip.as_deref().map_or(Some(0), integer_literal)?;
                let fetch = match limit.fetch.as_deref() {
                    None => None,
                    Some(fetch) => Some(integer_literal(fetch)?),
                };
                bounds.push(RowBound::Limit { skip, fetch });
                limit.input.as_ref()
            }
            LogicalPlan::Sort(sort) => {
                bounds.push(RowBound::Sort(sort.expr.clone(), sort.fetch));
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
                remap_sort_keys(&mut bounds, &columns)?;
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
                    bounds,
                });
            }
            _ => return None,
        };
    }
}

/// Expresses a sort key of the peeled bounds through the projection's input. A deterministic
/// key that only depends on the outer row is constant for that row and is dropped (`None`); a
/// key that mixes outer and inner columns, or a volatile outer-only one, cannot be evaluated
/// below the projection (`outer + inner` does not order like `inner` once the addition
/// overflows), so the rewrite is declined.
fn input_sort_key(
    sort: SortExpr,
    outputs: &[(String, Expr)],
) -> PlanResult<Option<Option<SortExpr>>> {
    let key = sort
        .expr
        .transform(|expr| match expr {
            Expr::Column(column) => match outputs.iter().find(|(name, _)| name == column.name()) {
                Some((_, output)) => Ok(Transformed::yes(output.clone())),
                None => Ok(Transformed::no(Expr::Column(column))),
            },
            expr => Ok(Transformed::no(expr)),
        })?
        .data;
    if !key.contains_outer() {
        return Ok(Some(Some(SortExpr { expr: key, ..sort })));
    }
    // A deterministic outer-only key is constant for one outer row; a volatile one such as
    // `rand() + o.id` takes a different value for every inner row and cannot be dropped.
    if is_outer_only(&key) && !key.is_volatile() {
        return Ok(Some(None));
    }
    Ok(None)
}

/// Rewrites a scalar subquery that DataFusion cannot decorrelate: one that is correlated
/// without being aggregated (Spark allows at most one row and fails at runtime otherwise),
/// or one that evaluates outer references in its projection. A producer aggregate over the
/// projection's input returns, in one row, the row count and a struct of the value(s) the
/// projection needs, so that one coherent row is chosen; the result is computed from that
/// row, raising Spark's error when more than one row would have been returned. Over an
/// uncorrelated input the peeled LIMIT and ORDER BY stay as plan nodes, composing exactly;
/// over a correlated one, which DataFusion cannot decorrelate through them, a single
/// `LIMIT [n] [OFFSET m]` with its ordering is emulated with `first_value`/`nth_value`.
/// The producer is a scalar subquery itself, so an uncorrelated input is evaluated exactly
/// once and a correlated one is decorrelated by DataFusion as an ordinary aggregate.
///
/// Each outer row sees its own LIMIT and ORDER BY, as in standard SQL. This intentionally differs
/// from Spark 4.2, which answers wrongly in two cases that must not be mirrored: `LIMIT n` over a
/// literal VALUES relation is folded into its first n rows before the correlated predicate is
/// applied (`ConvertToLocalRelation`), so `(SELECT x FROM VALUES (0), (1) AS t(x) WHERE x = o.id
/// LIMIT 1)` is NULL for `o.id = 1`; and the DESC order of `ORDER BY .. LIMIT 1` is lost under an
/// outer-only predicate. Spark itself gives the per-row answers once the literal relation is
/// replaced by `range`, and so does DuckDB. The divergence is pinned by the `@sail-only` rule in
/// `python/pysail/tests/spark/function/features/scalar_subquery.feature`.
fn rewrite_bounded_scalar_subquery(
    plan: &LogicalPlan,
    state: &mut PlanResolverState,
    lazy: bool,
) -> PlanResult<Option<Expr>> {
    let Some(shape) = peel_scalar_subquery(plan) else {
        return Ok(None);
    };
    let input = shape.projection.input.as_ref();
    let correlated = !input.all_out_ref_exprs().is_empty();
    let expr = strip_alias(shape.expr);
    let outer = expr.contains_outer();
    let index = shape
        .projection
        .expr
        .iter()
        .position(|candidate| candidate == shape.expr);
    let data_type = index.map(|index| shape.projection.schema.field(index).data_type().clone());
    let single_row = row_bounds(input) == (1, Some(1));
    let at_most_one_row = row_bounds(input).1 == Some(1);
    // The single row survives the bounds unless one of them skips or fetches nothing.
    let kept = shape.bounds.iter().all(|bound| match bound {
        RowBound::Limit { skip, fetch } => *skip == 0 && *fetch != Some(0),
        RowBound::Sort(_, fetch) => *fetch != Some(0),
    });
    if outer && at_most_one_row && (is_aggregated(input) || !correlated) {
        // The subquery has at most one row, which DataFusion can produce itself (an aggregate,
        // possibly under HAVING, or an uncorrelated input), so no producer is needed: the inner
        // columns the expression uses are read from a struct the subquery returns, and the
        // expression is evaluated per outer row. DataFusion's count-bug compensation then
        // yields the aggregate's empty-input values for an outer row without a match, and an
        // empty subquery yields NULL fields.
        if !kept {
            return Ok(data_type.map(|data_type| cast(lit(ScalarValue::Null), data_type)));
        }
        let mut columns: Vec<Expr> = vec![];
        expr.apply(|expr| {
            if let Expr::Column(_) = expr
                && !columns.contains(expr)
            {
                columns.push(expr.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let names = (0..columns.len())
            .map(|i| i.to_string())
            .collect::<Vec<_>>();
        let row = if columns.is_empty() {
            None
        } else {
            let plan = LogicalPlanBuilder::from(input.clone())
                .project(vec![
                    named_struct(
                        names
                            .iter()
                            .zip(&columns)
                            .flat_map(|(name, column)| [lit(name.as_str()), column.clone()])
                            .collect(),
                    )
                    .alias(state.register_field_name("")),
                ])?
                .build()?;
            Some(expr_fn::scalar_subquery(Arc::new(plan)))
        };
        let result = expr
            .clone()
            .transform(|expr| match expr {
                Expr::OuterReferenceColumn(_, column) => Ok(Transformed::yes(Expr::Column(column))),
                Expr::Column(_) => match (columns.iter().position(|c| c == &expr), &row) {
                    (Some(index), Some(row)) => Ok(Transformed::yes(get_field(
                        row.clone(),
                        names[index].as_str(),
                    ))),
                    _ => Ok(Transformed::no(expr)),
                },
                expr => Ok(Transformed::no(expr)),
            })?
            .data;
        return Ok(Some(result));
    }
    // DataFusion handles the other shapes itself: a correlated aggregated subquery is
    // decorrelated, and an uncorrelated one has its row count checked at execution, unless
    // that check must be lazy (a guard branch is evaluated eagerly as a bare subquery
    // otherwise) and the subquery may return several rows. LIMIT and ORDER BY above a
    // correlated ungrouped aggregate, which DataFusion cannot decorrelate through, are dropped
    // when they keep its single row (they are no-ops then) and yield NULL otherwise.
    if !outer
        && ((correlated && is_aggregated(input)) || (!correlated && (!lazy || at_most_one_row)))
    {
        return Ok(if correlated && !shape.bounds.is_empty() && single_row {
            if kept {
                let plan = LogicalPlanBuilder::from(input.clone())
                    .project(vec![shape.expr.clone()])?
                    .build()?;
                Some(nullable_scalar_subquery(plan))
            } else {
                data_type.map(|data_type| cast(lit(ScalarValue::Null), data_type))
            }
        } else {
            None
        });
    }
    // A projected expression that is neither a bare column nor dependent on the outer row is
    // evaluated once below the row selection, so that a volatile value is sorted and returned
    // as the same value; sort keys and the selected value refer to its column.
    let mut projected = vec![];
    let outputs = shape
        .projection
        .expr
        .iter()
        .map(|output| {
            let name = output.schema_name().to_string();
            let inner = strip_alias(output);
            let value = if matches!(inner, Expr::Column(_)) || inner.contains_outer() {
                inner.clone()
            } else {
                projected.push(inner.clone().alias(&name));
                ident(&name)
            };
            (name, value)
        })
        .collect::<Vec<_>>();
    let input = if projected.is_empty() {
        input.clone()
    } else {
        LogicalPlanBuilder::from(input.clone())
            .project(
                input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .chain(projected),
            )?
            .build()?
    };
    let mut bounds = vec![];
    let mut shape_bounds = shape.bounds.into_iter().peekable();
    // An ORDER BY above every LIMIT cannot change which single row a scalar subquery returns.
    while matches!(shape_bounds.peek(), Some(RowBound::Sort(_, None))) {
        shape_bounds.next();
    }
    for bound in shape_bounds {
        bounds.push(match bound {
            RowBound::Sort(sorts, fetch) => {
                let mut keys = vec![];
                for sort in sorts {
                    match input_sort_key(sort, &outputs)? {
                        Some(Some(key)) => keys.push(key),
                        Some(None) => {}
                        None => return Ok(None),
                    }
                }
                RowBound::Sort(keys, fetch)
            }
            bound => bound,
        });
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
    } else if let Some(index) = index {
        values.push(outputs[index].1.clone());
    }
    // Struct fields are named by position, so that two producers of the same subquery (which
    // differ only in generated names) are interchangeable when the aggregate rewrite shares one.
    let names = (0..values.len()).map(|i| i.to_string()).collect::<Vec<_>>();
    // One struct per row keeps the chosen row coherent even under a volatile ordering.
    let row = (!values.is_empty()).then(|| {
        named_struct(
            names
                .iter()
                .zip(&values)
                .flat_map(|(name, value)| [lit(name.as_str()), value.clone()])
                .collect(),
        )
    });
    let (input, selected, skip, single) = if correlated {
        // Only one LIMIT (with its ORDER BY) can be emulated; DataFusion decorrelates neither.
        let (skip, fetch, sorts) = match bounds.as_slice() {
            [] => (0, None, vec![]),
            [RowBound::Limit { skip, fetch }] => (*skip, *fetch, vec![]),
            [RowBound::Limit { skip, fetch }, RowBound::Sort(sorts, None)] => {
                (*skip, *fetch, sorts.clone())
            }
            [RowBound::Sort(sorts, Some(fetch))] => (0, Some(*fetch), sorts.clone()),
            _ => return Ok(None),
        };
        if fetch == Some(0) {
            return Ok(data_type.map(|data_type| cast(lit(ScalarValue::Null), data_type)));
        }
        let selected = row.map(|row| {
            if skip == 0 {
                first_value(row, sorts)
            } else {
                nth_value(row, i64::try_from(skip + 1).unwrap_or(i64::MAX), sorts)
            }
        });
        (input, selected, skip, fetch == Some(1))
    } else {
        let mut plan = input;
        for bound in bounds.into_iter().rev() {
            plan = match bound {
                RowBound::Limit { skip, fetch } => {
                    LogicalPlanBuilder::from(plan).limit(skip, fetch)?.build()?
                }
                RowBound::Sort(keys, fetch) if keys.is_empty() => match fetch {
                    Some(fetch) => LogicalPlanBuilder::from(plan)
                        .limit(0, Some(fetch))?
                        .build()?,
                    None => plan,
                },
                RowBound::Sort(keys, fetch) => LogicalPlan::Sort(Sort {
                    expr: keys,
                    input: Arc::new(plan),
                    fetch,
                }),
            };
        }
        (plan, row.map(|row| first_value(row, vec![])), 0, false)
    };
    let count_name = "count".to_string();
    let value_name = "value".to_string();
    let mut aggregates = vec![count(lit(1_i64)).alias(&count_name)];
    let mut fields = vec![lit(count_name.as_str()), ident(&count_name)];
    if let Some(selected) = selected {
        aggregates.push(selected.alias(&value_name));
        fields.extend([lit(value_name.as_str()), ident(&value_name)]);
    }
    let producer = LogicalPlanBuilder::from(input)
        .aggregate(Vec::<Expr>::new(), aggregates)?
        .project(vec![
            named_struct(fields).alias(state.register_field_name("")),
        ])?
        .build()?;
    let producer = expr_fn::scalar_subquery(Arc::new(producer));
    let value = |index: usize| {
        get_field(
            get_field(producer.clone(), value_name.as_str()),
            names[index].as_str(),
        )
    };
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
    let rows = get_field(producer, count_name.as_str());
    // Rows skipped by OFFSET are not returned.
    let rows = if skip == 0 {
        rows
    } else {
        greatest(vec![
            rows - lit(i64::try_from(skip).unwrap_or(i64::MAX)),
            lit(0_i64),
        ])
    };
    let mut case = when(rows.clone().eq(lit(0_i64)), lit(ScalarValue::Null));
    if !single {
        case = case.when(rows.gt(lit(1_i64)), scalar_subquery_error());
    }
    Ok(Some(case.otherwise(result)?))
}

/// A node on the spine of a subquery plan above its first aggregate, which the guard rewrite
/// takes apart and puts back together.
enum SpineNode {
    Projection(Vec<Expr>),
    Alias(TableReference),
    Sort(Vec<SortExpr>, Option<usize>),
    Limit(Option<Box<Expr>>, Option<Box<Expr>>),
    /// A filter and the schema of its input.
    Filter(Expr, DFSchemaRef),
}

/// Takes the spine of `plan` apart down to its first aggregate (or any other node), returning
/// the spine nodes from the top down and the plan below them.
fn peel_spine(plan: &LogicalPlan) -> (Vec<SpineNode>, &LogicalPlan) {
    let mut nodes = vec![];
    let mut plan = plan;
    loop {
        plan = match plan {
            LogicalPlan::Projection(projection) => {
                nodes.push(SpineNode::Projection(projection.expr.clone()));
                &projection.input
            }
            LogicalPlan::SubqueryAlias(alias) => {
                nodes.push(SpineNode::Alias(alias.alias.clone()));
                &alias.input
            }
            LogicalPlan::Sort(sort) => {
                nodes.push(SpineNode::Sort(sort.expr.clone(), sort.fetch));
                &sort.input
            }
            LogicalPlan::Limit(limit) => {
                nodes.push(SpineNode::Limit(limit.skip.clone(), limit.fetch.clone()));
                &limit.input
            }
            LogicalPlan::Filter(filter) => {
                nodes.push(SpineNode::Filter(
                    filter.predicate.clone(),
                    Arc::clone(filter.input.schema()),
                ));
                &filter.input
            }
            plan => return (nodes, plan),
        };
    }
}

/// Puts spine nodes back together over `base`.
fn build_spine(nodes: &[SpineNode], base: LogicalPlan) -> PlanResult<LogicalPlan> {
    let mut plan = base;
    for node in nodes.iter().rev() {
        plan = match node {
            SpineNode::Projection(exprs) => {
                LogicalPlan::Projection(Projection::try_new(exprs.clone(), Arc::new(plan))?)
            }
            SpineNode::Alias(alias) => {
                LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), alias.clone())?)
            }
            SpineNode::Sort(expr, fetch) => LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(plan),
                fetch: *fetch,
            }),
            SpineNode::Limit(skip, fetch) => LogicalPlan::Limit(Limit {
                skip: skip.clone(),
                fetch: fetch.clone(),
                input: Arc::new(plan),
            }),
            SpineNode::Filter(predicate, _) => {
                LogicalPlan::Filter(Filter::try_new(predicate.clone(), Arc::new(plan))?)
            }
        };
    }
    Ok(plan)
}

/// The spine with every filter conjunct mapped by `conjunct` (`None` drops it); a filter left
/// without conjuncts is dropped.
fn map_spine_conjuncts(
    nodes: &[SpineNode],
    conjunct: impl Fn(&Expr) -> Option<Expr>,
) -> Vec<SpineNode> {
    nodes
        .iter()
        .filter_map(|node| match node {
            SpineNode::Filter(predicate, schema) => conjunction(
                split_conjunction(predicate)
                    .into_iter()
                    .filter_map(&conjunct),
            )
            .map(|predicate| SpineNode::Filter(predicate, Arc::clone(schema))),
            SpineNode::Projection(exprs) => Some(SpineNode::Projection(exprs.clone())),
            SpineNode::Alias(alias) => Some(SpineNode::Alias(alias.clone())),
            SpineNode::Sort(expr, fetch) => Some(SpineNode::Sort(expr.clone(), *fetch)),
            SpineNode::Limit(skip, fetch) => Some(SpineNode::Limit(skip.clone(), fetch.clone())),
        })
        .collect()
}

/// The number of `r OR p` guard conjuncts a correlated subquery may have, over all of its
/// aggregate levels, before Sail stops hoisting them (the rewrite builds `2^n` subquery plans);
/// more are left to DataFusion.
const MAX_OR_GUARDS: usize = 4;

/// The number of `r OR p` guard conjuncts on the spine filters of `plan`, at every aggregate
/// level, each of which doubles the number of subquery plans the guard rewrite builds.
fn count_or_guards(plan: &LogicalPlan) -> usize {
    let (nodes, base) = peel_spine(plan);
    let mut count = 0;
    for node in &nodes {
        let SpineNode::Filter(predicate, _) = node else {
            continue;
        };
        for conjunct in split_conjunction(predicate) {
            let (outer, rest): (Vec<_>, Vec<_>) = split_binary(conjunct, Operator::Or)
                .into_iter()
                .partition(|disjunct| is_outer_only(disjunct));
            if !outer.is_empty() && !rest.is_empty() {
                count += 1;
            }
        }
    }
    match base {
        LogicalPlan::Aggregate(aggregate) => count + count_or_guards(&aggregate.input),
        _ => count,
    }
}

/// DataFusion decorrelates only predicates that compare inner and outer columns. The
/// predicates on the outer row alone in the filters on the spine of a correlated subquery, or
/// an OR with such a disjunct, are hoisted out as a CASE guard: `(SELECT .. WHERE p1 AND p2)`
/// becomes `CASE WHEN p1 AND p2 THEN (SELECT ..) ELSE (SELECT .. FROM <nothing>) END`, with
/// all the outer-only conjuncts of every spine filter hoisted together and the empty branch
/// holding no filter at all, so that the rewrite stays linear in the number of filters. A
/// conjunct `r OR p` becomes `CASE WHEN p THEN (SELECT ..) ELSE (SELECT .. WHERE r) END`, one
/// conjunct per CASE, since the two branches keep different predicates. A spine without
/// guards above its first aggregate is searched below that aggregate. A volatile guard such as
/// `o.id > rand()` is hoisted too and so evaluated once per outer row, which is also where
/// Spark evaluates an outer-only predicate it pulls above an aggregate. Returns the guard and
/// the two subquery plans.
fn split_outer_guard(plan: &LogicalPlan) -> PlanResult<Option<(Expr, LogicalPlan, LogicalPlan)>> {
    let (nodes, base) = peel_spine(plan);
    let mut pure = vec![];
    let mut or_guards = vec![];
    for node in &nodes {
        let SpineNode::Filter(predicate, _) = node else {
            continue;
        };
        for conjunct in split_conjunction(predicate) {
            let (outer, rest): (Vec<_>, Vec<_>) = split_binary(conjunct, Operator::Or)
                .into_iter()
                .cloned()
                .partition(is_outer_only);
            let Some(guard) = disjunction(outer) else {
                continue;
            };
            if rest.is_empty() {
                pure.push(conjunct.clone());
            } else {
                or_guards.push((conjunct.clone(), guard, rest));
            }
        }
    }
    if let Some(guard) = conjunction(pure.clone()) {
        let when_true = build_spine(
            &map_spine_conjuncts(&nodes, |conjunct| {
                (!pure.contains(conjunct)).then(|| conjunct.clone())
            }),
            base.clone(),
        )?;
        // The plan is empty at the first filter when a guard does not hold.
        let first = nodes
            .iter()
            .position(|node| matches!(node, SpineNode::Filter(..)))
            .ok_or_else(|| PlanError::internal("guard without a filter"))?;
        let SpineNode::Filter(_, schema) = &nodes[first] else {
            return Err(PlanError::internal("guard without a filter"));
        };
        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(schema),
        });
        let when_false = build_spine(&nodes[..first], empty)?;
        return Ok(Some((guard, when_true, when_false)));
    }
    if let Some((conjunct, guard, rest)) = or_guards.into_iter().next() {
        let when_true = build_spine(
            &map_spine_conjuncts(&nodes, |c| (c != &conjunct).then(|| c.clone())),
            base.clone(),
        )?;
        let rest = disjunction(rest).unwrap_or(lit(false));
        let when_false = build_spine(
            &map_spine_conjuncts(&nodes, |c| {
                Some(if c == &conjunct {
                    rest.clone()
                } else {
                    c.clone()
                })
            }),
            base.clone(),
        )?;
        return Ok(Some((guard, when_true, when_false)));
    }
    let LogicalPlan::Aggregate(aggregate) = base else {
        return Ok(None);
    };
    let Some((guard, when_true, when_false)) = split_outer_guard(&aggregate.input)? else {
        return Ok(None);
    };
    let rebuild = |input: LogicalPlan| -> PlanResult<LogicalPlan> {
        let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(input),
            aggregate.group_expr.clone(),
            aggregate.aggr_expr.clone(),
        )?);
        build_spine(&nodes, aggregate)
    };
    Ok(Some((guard, rebuild(when_true)?, rebuild(when_false)?)))
}

/// Hoists the outer-only guards of the plan's spine filters, so that `WHERE p AND (p1 OR r1)
/// AND (p2 OR r2)` becomes nested CASE expressions over subqueries that DataFusion can
/// decorrelate. Returns whether a guard CASE was built. With `lazy`, a subquery without
/// guards is evaluated lazily (see `scalar_subquery_expr`).
fn guarded_scalar_subquery(
    plan: LogicalPlan,
    state: &mut PlanResolverState,
    lazy: bool,
) -> PlanResult<(Expr, bool)> {
    match split_outer_guard(&plan)? {
        Some((guard, when_true, when_false)) => {
            let guard = guard
                .transform(|expr| match expr {
                    Expr::OuterReferenceColumn(_, column) => {
                        Ok(Transformed::yes(Expr::Column(column)))
                    }
                    expr => Ok(Transformed::no(expr)),
                })?
                .data;
            // A branch is evaluated lazily: it must not raise unless the guard selects it.
            let case = when(guard, guarded_scalar_subquery(when_true, state, true)?.0)
                .otherwise(guarded_scalar_subquery(when_false, state, true)?.0)?;
            Ok((case, true))
        }
        None => Ok((scalar_subquery_expr(plan, state, lazy)?, false)),
    }
}

/// Whether the expression, or a subquery plan inside it, evaluates a volatile function.
fn contains_volatile(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(expr.is_volatile_node()
            || matches!(expr, Expr::ScalarSubquery(subquery) if plan_contains_volatile(&subquery.subquery)))
    })
    .unwrap_or(true)
}

fn plan_contains_volatile(plan: &LogicalPlan) -> bool {
    let mut volatile = false;
    let _ = plan.apply_with_subqueries(|node| {
        if node.expressions().iter().any(|expr| expr.is_volatile()) {
            volatile = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    volatile
}

/// With `lazy`, a subquery that may return several rows raises its error only when its value
/// is used, which a guard CASE requires of its branches.
fn scalar_subquery_expr(
    plan: LogicalPlan,
    state: &mut PlanResolverState,
    lazy: bool,
) -> PlanResult<Expr> {
    Ok(match rewrite_bounded_scalar_subquery(&plan, state, lazy)? {
        Some(rewritten) => rewritten,
        None => nullable_scalar_subquery(plan),
    })
}

fn is_aggregated(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => is_aggregated(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => is_aggregated(&alias.input),
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Filter(filter) => is_aggregated(&filter.input),
        _ => false,
    }
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
        // The same subquery resolved in the same context yields the same expression, so that
        // repeated occurrences (a grouping expression repeated in the select list, two aggregate
        // arguments) are structurally equal and evaluated once, as Spark's canonicalized
        // subquery expressions are.
        let context = state.scalar_subquery_context(Self::local_schema(schema, state));
        if let Some(expr) = state.resolved_scalar_subquery(&subquery, &context) {
            return Ok(NamedExpr::new(vec!["subquery".to_string()], expr.clone()));
        }
        let spec = subquery.clone();
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
        // Each `r OR p` guard doubles the number of subquery plans, so the rewrite is declined
        // before any of them is built when there are too many over all aggregate levels.
        let guarded = if count_or_guards(&subquery) > MAX_OR_GUARDS {
            (scalar_subquery_expr(subquery, state, false)?, false)
        } else {
            guarded_scalar_subquery(subquery, state, false)?
        };
        let subquery = match guarded {
            // The wrapper marks the guard CASE as one scalar subquery expression, so that a
            // null check on it may fold (`never_null_unplannable`) and COUNT may probe it.
            (case, true) => wrap_scalar_subquery(case),
            (expr, false) => expr,
        };
        // A volatile subquery is evaluated once per occurrence, as Spark seeds each occurrence
        // of a random function separately, so it is never shared.
        if !contains_volatile(&subquery) {
            state.remember_scalar_subquery(spec, context, subquery.clone());
        }
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
