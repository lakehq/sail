// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::functions::expr_fn::{coalesce, get_field, named_struct};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator};
use sail_logical_plan::seeded_random::{SeededRandomNode, is_reproducible_seeded_random_call};

use crate::scalar_subquery_keys::normalize_user_window_keys;

/// Evaluate non-equality scalar aggregates over the distinct outer values.
/// Native decorrelation can then pull up equality on those values without
/// incorrectly grouping the inner input by a non-equality predicate's columns.
#[derive(Debug)]
pub struct NormalizeScalarSubqueryDomain;

impl OptimizerRule for NormalizeScalarSubqueryDomain {
    fn name(&self) -> &str {
        "normalize_scalar_subquery_domain"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let input = match &plan {
            LogicalPlan::Projection(projection) => Arc::clone(&projection.input),
            LogicalPlan::Filter(filter) => Arc::clone(&filter.input),
            _ => return Ok(Transformed::no(plan)),
        };
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            expr.transform_up(|expr| {
                let Expr::ScalarSubquery(mut query) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if query.outer_ref_columns.is_empty() {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                let has_window = query
                    .subquery
                    .exists(|node| Ok(matches!(node, LogicalPlan::Window(_))))?;
                if has_window
                    && let Some(rewritten) = bind_single_outer_row(&query.subquery, &input)?
                {
                    query.outer_ref_columns.clear();
                    query.subquery = Arc::new(rewritten);
                    return Ok(Transformed::yes(Expr::ScalarSubquery(query)));
                }
                let mut unsupported = false;
                let mut needs_domain = false;
                let mut aggregates = 0;
                let mut filters = 0;
                let mut volatile_correlation = false;
                query.subquery.apply(|node| {
                    match node {
                        LogicalPlan::Aggregate(aggregate) => {
                            aggregates += 1;
                            if !unsupported {
                                unsupported = !aggregate.group_expr.is_empty();
                            }
                        }
                        LogicalPlan::Filter(filter) if filter.predicate.contains_outer() => {
                            // This traversal is top-down. A filter encountered
                            // before the aggregate is HAVING, not its input.
                            if !unsupported {
                                unsupported = aggregates == 0;
                            }
                            filters += 1;
                            volatile_correlation |= split_conjunction(&filter.predicate)
                                .iter()
                                .any(|expr| expr.contains_outer() && expr.is_volatile());
                            if !needs_domain {
                                needs_domain =
                                    split_conjunction(&filter.predicate).iter().any(|expr| {
                                        expr.contains_outer()
                                            && correlated_equality_key(expr, true).is_none()
                                    });
                            }
                        }
                        // The domain joins the input of the correlated filter, so a join below
                        // the filter only produces that input.
                        LogicalPlan::Join(_) if filters > 0 => {
                            if !unsupported {
                                unsupported = node.contains_outer_reference();
                            }
                        }
                        LogicalPlan::Window(_) if aggregates == 1 && filters == 0 => {
                            unsupported |= node.contains_outer_reference();
                        }
                        LogicalPlan::Join(_)
                        | LogicalPlan::Union(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Limit(_)
                        | LogicalPlan::Subquery(_) => unsupported = true,
                        _ => {
                            if !unsupported {
                                unsupported = node.contains_outer_reference();
                            }
                        }
                    }
                    // An unsupported subquery is left as is, so the rest of it is not needed.
                    Ok(if unsupported {
                        TreeNodeRecursion::Stop
                    } else {
                        TreeNodeRecursion::Continue
                    })
                })?;
                // Rewriting a volatile window predicate needs one evaluation per outer
                // row. Only build a value domain when its exact bindings are distinct.
                if unsupported
                    || aggregates != 1
                    || filters != 1
                    || if has_window {
                        !volatile_correlation
                    } else {
                        !needs_domain
                    }
                {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                // The domain reads the outer input again; do not duplicate volatile
                // evaluations whose values must be shared with the original row.
                // Only reproducible seeded calls may be replayed, including calls inside a node.
                if has_unreproducible_volatile_expression(&input, false)? {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                // A node above the correlated filter is evaluated by a projection before this rule.
                if has_unreproducible_volatile_expression(
                    &query.subquery,
                    has_window && volatile_correlation,
                )? {
                    return datafusion_common::not_impl_err!(
                        "correlated scalar aggregate with volatile inner expressions"
                    );
                }
                let outer = query.subquery.all_out_ref_exprs();
                let mut bindings = Vec::new();
                for reference in outer {
                    let Expr::OuterReferenceColumn(_, column) = &reference else {
                        continue;
                    };
                    if !input.schema().has_column(column) {
                        return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                    }
                    let key =
                        Column::new_unqualified(config.alias_generator().next("__sail_domain"));
                    bindings.push((reference.clone(), column.clone(), key));
                }
                if bindings.is_empty() {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                if volatile_correlation {
                    if !has_window
                        || !unique_literal_bindings(
                            &input,
                            bindings.iter().map(|(_, column, _)| column),
                        )?
                    {
                        return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                    }
                    query.subquery = Arc::new(
                        normalize_user_window_keys(Arc::unwrap_or_clone(query.subquery))?.data,
                    );
                }
                let domain = LogicalPlanBuilder::from(Arc::clone(&input))
                    .project(
                        bindings
                            .iter()
                            .map(|(_, outer, key)| Expr::Column(outer.clone()).alias(&key.name)),
                    )?
                    .distinct()?
                    .build()?;
                let output = query.subquery.schema().columns();
                let default = scalar_aggregate_value(&query.subquery, true)?;
                let value = if matches!(default, Expr::Literal(ref v, _) if v.is_null()) {
                    None
                } else {
                    Some(scalar_aggregate_value(&query.subquery, false)?)
                };
                let correlation = conjunction(bindings.iter().map(|(reference, _, key)| {
                    datafusion_expr::expr_fn::binary_expr(
                        Expr::Column(key.clone()),
                        Operator::IsNotDistinctFrom,
                        reference.clone(),
                    )
                }))
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!("empty scalar correlation domain")
                })?;
                let mut attached = false;
                let mut grouped = None;
                let rewritten = Arc::unwrap_or_clone(query.subquery)
                    .transform_up(|node| {
                        let was_attached = attached;
                        let node = if let LogicalPlan::Filter(mut filter) = node {
                            if filter.predicate.contains_outer() {
                                filter.input = Arc::new(
                                    LogicalPlanBuilder::from(filter.input)
                                        .cross_join(domain.clone())?
                                        .build()?,
                                );
                                attached = true;
                            }
                            LogicalPlan::Filter(filter)
                        } else {
                            node
                        };
                        let names = NamePreserver::new(&node);
                        let node = node
                            .map_expressions(|expr| {
                                let name = names.save(&expr);
                                expr.transform_up(|expr| {
                                    if let Some((_, _, key)) =
                                        bindings.iter().find(|(reference, _, _)| reference == &expr)
                                    {
                                        Ok(Transformed::yes(Expr::Column(key.clone())))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .map(|result| result.update_data(|expr| name.restore(expr)))
                            })?
                            .data;
                        if !was_attached {
                            if attached
                                && volatile_correlation
                                && let LogicalPlan::Filter(filter) = node
                            {
                                let name = config.alias_generator().next("__sail_domain_predicate");
                                let columns = filter
                                    .input
                                    .schema()
                                    .columns()
                                    .into_iter()
                                    .map(Expr::Column)
                                    .collect::<Vec<_>>();
                                // A staged volatile predicate evaluates on the joined rows.
                                // Keep it together when the optimizer extracts join keys.
                                let plan = LogicalPlanBuilder::from(filter.input)
                                    .project(
                                        columns
                                            .iter()
                                            .cloned()
                                            .chain([filter.predicate.alias(&name)]),
                                    )?
                                    .filter(Expr::Column(Column::new_unqualified(name)))?
                                    .project(columns)?
                                    .build()?;
                                return Ok(Transformed::yes(plan));
                            }
                            return Ok(Transformed::yes(node));
                        }
                        let keys = || bindings.iter().map(|(_, _, key)| Expr::Column(key.clone()));
                        let result = match node {
                            LogicalPlan::Filter(mut filter) => {
                                // Keep HAVING and the lifted correlation in one filter
                                // so scalar-subquery validation still sees the aggregate.
                                while let LogicalPlan::Filter(inner) = filter.input.as_ref() {
                                    filter.predicate =
                                        filter.predicate.and(inner.predicate.clone());
                                    filter.input = Arc::clone(&inner.input);
                                }
                                LogicalPlan::Filter(filter)
                            }
                            LogicalPlan::Projection(mut projection) => {
                                projection.expr.extend(keys());
                                LogicalPlanBuilder::from(projection.input)
                                    .project(projection.expr)?
                                    .build()?
                            }
                            LogicalPlan::Window(mut window) => {
                                window.window_expr = window
                                    .window_expr
                                    .into_iter()
                                    .map(|expr| {
                                        let name = expr.schema_name().to_string();
                                        expr.transform_up(|expr| {
                                            let Expr::WindowFunction(mut function) = expr else {
                                                return Ok(Transformed::no(expr));
                                            };
                                            function.params.partition_by.extend(keys());
                                            Ok(Transformed::yes(Expr::WindowFunction(function)))
                                        })
                                        .map(|result| {
                                            if result.data.schema_name().to_string() == name {
                                                result.data
                                            } else {
                                                result.data.alias(name)
                                            }
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                LogicalPlan::Window(window).recompute_schema()?
                            }
                            LogicalPlan::SubqueryAlias(alias) => {
                                LogicalPlanBuilder::from(alias.input)
                                    .alias(alias.alias)?
                                    .build()?
                            }
                            LogicalPlan::Aggregate(mut aggregate) => {
                                aggregate.group_expr.extend(keys());
                                let aggregate = LogicalPlanBuilder::from(aggregate.input)
                                    .aggregate(aggregate.group_expr, aggregate.aggr_expr)?
                                    .filter(correlation.clone())?
                                    .build()?;
                                grouped = Some(aggregate.clone());
                                aggregate
                            }
                            // Ordering a scalar aggregate cannot change its value.
                            // Remove this sort so native decorrelation can reach
                            // the correlation we attached above the aggregate.
                            LogicalPlan::Sort(sort)
                                if grouped.is_some() && sort.fetch.is_none() =>
                            {
                                Arc::unwrap_or_clone(sort.input)
                            }
                            other => other,
                        };
                        Ok(Transformed::yes(result))
                    })?
                    .data;
                let result = if let Some(value) = value {
                    // The struct distinguishes an absent group from a present group whose
                    // projection or HAVING produces NULL, with one evaluation of the input.
                    let pack = |expr| named_struct(vec![datafusion_expr::lit("value"), expr]);
                    let grouped = grouped.ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!("missing scalar aggregate")
                    })?;
                    query.subquery = Arc::new(
                        LogicalPlanBuilder::from(grouped)
                            .project([pack(value)])?
                            .build()?,
                    );
                    get_field(
                        coalesce(vec![Expr::ScalarSubquery(query), pack(default)]),
                        "value",
                    )
                } else {
                    query.subquery = Arc::new(
                        LogicalPlanBuilder::from(rewritten)
                            .project(output.into_iter().map(Expr::Column))?
                            .build()?,
                    );
                    Expr::ScalarSubquery(query)
                };
                Ok(Transformed::yes(result))
            })
            .map(|result| result.update_data(|expr| name.restore(expr)))
        })
    }
}

/// Inspect a small literal input without evaluating expressions or estimating row counts.
fn literal_rows(plan: &LogicalPlan) -> Result<Option<Vec<Vec<Expr>>>> {
    match plan {
        LogicalPlan::Values(values)
            if values.values.len() <= 64
                && values
                    .values
                    .iter()
                    .flatten()
                    .all(|expr| matches!(expr, Expr::Literal(..))) =>
        {
            Ok(Some(values.values.clone()))
        }
        LogicalPlan::EmptyRelation(empty)
            if empty.produce_one_row && empty.schema.fields().is_empty() =>
        {
            Ok(Some(vec![vec![]]))
        }
        LogicalPlan::SubqueryAlias(alias) => literal_rows(&alias.input),
        LogicalPlan::Projection(project) => {
            let Some(rows) = literal_rows(&project.input)? else {
                return Ok(None);
            };
            let Some(expressions) = project
                .expr
                .iter()
                .cloned()
                .map(|expr| match expr.unalias() {
                    Expr::Column(column) => {
                        project.input.schema().index_of_column(&column).ok().map(Ok)
                    }
                    literal @ Expr::Literal(..) => Some(Err(literal)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            Ok(Some(
                rows.into_iter()
                    .map(|row| {
                        expressions
                            .iter()
                            .map(|expr| match expr {
                                Ok(index) => row[*index].clone(),
                                Err(literal) => literal.clone(),
                            })
                            .collect()
                    })
                    .collect(),
            ))
        }
        _ => Ok(None),
    }
}

fn unique_literal_bindings<'a>(
    input: &LogicalPlan,
    columns: impl Iterator<Item = &'a Column>,
) -> Result<bool> {
    let Some(rows) = literal_rows(input)? else {
        return Ok(false);
    };
    let schema = input.schema();
    let indexes = columns
        .map(|column| schema.index_of_column(column))
        .collect::<Result<Vec<_>>>()?;
    let mut distinct = HashSet::new();
    Ok(!rows.is_empty() && rows.iter().all(|row| {
        let values = indexes.iter().map(|&index| &row[index]).collect::<Vec<_>>();
        values.iter().all(|expr| matches!(expr, Expr::Literal(value, _) if value.data_type().is_integer() || value.data_type().is_string()))
            && distinct.insert(values)
    }))
}

/// A single outer literal row can bind a window subquery without replaying its input.
fn bind_single_outer_row(plan: &LogicalPlan, input: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let Some(rows) = literal_rows(input)? else {
        return Ok(None);
    };
    let [row] = rows.as_slice() else {
        return Ok(None);
    };
    let mut bindings = Vec::new();
    for reference in plan.all_out_ref_exprs() {
        let Expr::OuterReferenceColumn(field, column) = &reference else {
            return Ok(None);
        };
        let Ok(index) = input.schema().index_of_column(column) else {
            return Ok(None);
        };
        if !matches!(&row[index], Expr::Literal(value, _) if value.data_type() == *field.data_type())
        {
            return Ok(None);
        }
        bindings.push((reference, &row[index]));
    }
    if bindings.is_empty() {
        return Ok(None);
    }
    let plan = normalize_user_window_keys(plan.clone())?.data;
    let bound = plan
        .transform_up(|node| {
            let names = NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let name = names.save(&expr);
                expr.transform_up(|expr| {
                    match bindings.iter().find(|(reference, _)| *reference == expr) {
                        Some((_, literal)) => Ok(Transformed::yes((*literal).clone())),
                        None => Ok(Transformed::no(expr)),
                    }
                })
                .map(|result| result.update_data(|expr| name.restore(expr)))
            })
        })?
        .data;
    Ok(bound.all_out_ref_exprs().is_empty().then_some(bound))
}

/// Express the scalar output and HAVING in terms of aggregate columns or their empty defaults.
fn scalar_aggregate_value(plan: &LogicalPlan, empty: bool) -> Result<Expr> {
    fn substitute(expr: Expr, input: &LogicalPlan, values: &[Expr]) -> Result<Expr> {
        expr.transform_up(|expr| {
            if let Expr::Column(column) = &expr {
                let index = input.schema().index_of_column(column)?;
                Ok(Transformed::yes(values[index].clone()))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .map(|result| result.data.unalias())
    }

    // Keep row existence separate from values: a projection such as COALESCE
    // must not turn a row rejected by HAVING into a non-null scalar result.
    fn values(plan: &LogicalPlan, empty: bool) -> Result<(Vec<Expr>, Option<Expr>)> {
        match plan {
            LogicalPlan::Aggregate(aggregate) if !empty => Ok((
                aggregate
                    .schema
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect(),
                None,
            )),
            LogicalPlan::Aggregate(aggregate) => aggregate
                .aggr_expr
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform_up(|expr| {
                            if let Expr::AggregateFunction(agg) = &expr {
                                let data_type = expr.get_type(aggregate.input.schema())?;
                                Ok(Transformed::yes(Expr::Literal(
                                    agg.func.default_value(&data_type)?,
                                    None,
                                )))
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })
                        .map(|result| result.data.unalias())
                })
                .collect::<Result<Vec<_>>>()
                .map(|values| (values, None)),
            LogicalPlan::Projection(projection) => {
                let (input, predicate) = values(&projection.input, empty)?;
                let values = projection
                    .expr
                    .iter()
                    .map(|expr| substitute(expr.clone(), &projection.input, &input))
                    .collect::<Result<Vec<_>>>()?;
                Ok((values, predicate))
            }
            LogicalPlan::Filter(filter) => {
                let (input, predicate) = values(&filter.input, empty)?;
                let condition = substitute(filter.predicate.clone(), &filter.input, &input)?;
                let predicate = Some(match predicate {
                    Some(predicate) => predicate.and(condition),
                    None => condition,
                });
                Ok((input, predicate))
            }
            LogicalPlan::SubqueryAlias(alias) => values(&alias.input, empty),
            LogicalPlan::Sort(sort) if sort.fetch.is_none() => values(&sort.input, empty),
            _ => datafusion_common::not_impl_err!("empty result of scalar correlation"),
        }
    }
    let (values, predicate) = values(plan, empty)?;
    let value = values
        .into_iter()
        .next()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("empty scalar output"))?;
    match predicate {
        Some(predicate) => datafusion_expr::when(predicate, value)
            .otherwise(datafusion_expr::lit(datafusion_common::ScalarValue::Null)),
        None => Ok(value),
    }
}

/// Returns the inner column of a correlated equality of an inner column, or of a cast of one if
/// `cast` is true, and an outer expression without inner columns, which DataFusion decorrelates
/// by pulling it up as a join key.
pub(crate) fn correlated_equality_key(expr: &Expr, cast: bool) -> Option<&Column> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    fn key(expr: &Expr, cast: bool) -> Option<&Column> {
        match expr {
            Expr::Column(column) => Some(column),
            Expr::Cast(c) if cast => match c.expr.as_ref() {
                Expr::Column(column) => Some(column),
                _ => None,
            },
            _ => None,
        }
    }
    let outer = |expr: &Expr| expr.contains_outer() && !expr.any_column_refs();
    match (key(&binary.left, cast), key(&binary.right, cast)) {
        (Some(column), _) if outer(&binary.right) => Some(column),
        (_, Some(column)) if outer(&binary.left) => Some(column),
        _ => None,
    }
}

fn has_unreproducible_volatile_expression(
    plan: &LogicalPlan,
    allow_volatile_filter: bool,
) -> Result<bool> {
    // Uncorrelated scalar subqueries are evaluated once and their results are shared by both
    // copies of the plan. Inspect this plan's expressions, not those subquery bodies.
    plan.exists(|node| {
        if allow_volatile_filter
            && matches!(node, LogicalPlan::Filter(filter) if filter.predicate.contains_outer())
        {
            return Ok(false);
        }
        let seeded = matches!(node, LogicalPlan::Extension(extension)
            if extension.node.as_any().is::<SeededRandomNode>());
        for expr in node.expressions() {
            if expr.exists(|expr| {
                Ok(
                    expr.is_volatile_node()
                        && !(seeded && is_reproducible_seeded_random_call(expr)),
                )
            })? {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::sum;
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::{JoinType, col, scalar_subquery};

    use super::*;

    #[test]
    fn keeps_correlated_join_below_correlated_inequality_filter() -> Result<()> {
        let scan = |name| {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            LogicalPlanBuilder::scan(
                name,
                provider_as_source(Arc::new(EmptyTable::new(schema))),
                None,
            )
        };
        let outer = Expr::OuterReferenceColumn(
            Arc::new(Field::new("id", DataType::Int64, true)),
            Column::new(Some("o"), "id"),
        );
        for correlated_join in [false, true] {
            let mut on = col("l.id").eq(col("r.id"));
            if correlated_join {
                on = on.and(col("r.id").eq(outer.clone()));
            }
            let subquery = scan("l")?
                .join_on(scan("r")?.build()?, JoinType::Inner, vec![on])?
                .filter(col("l.id").lt(outer.clone()))?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("l.id"))])?
                .build()?;
            let plan = scan("o")?
                .project(vec![scalar_subquery(Arc::new(subquery))])?
                .build()?;
            let result =
                NormalizeScalarSubqueryDomain.rewrite(plan.clone(), &OptimizerContext::new())?;
            if correlated_join {
                assert!(!result.transformed);
                assert_eq!(result.data, plan);
            } else {
                // The otherwise identical input is eligible for domain normalization.
                assert!(result.transformed);
                assert_ne!(result.data, plan);
            }
        }
        Ok(())
    }
}
