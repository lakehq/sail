use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator};

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
                let mut unsupported = false;
                let mut needs_domain = false;
                let mut aggregates = 0;
                let mut filters = 0;
                query.subquery.apply(|node| {
                    match node {
                        LogicalPlan::Aggregate(aggregate) => {
                            aggregates += 1;
                            unsupported |= !aggregate.group_expr.is_empty();
                        }
                        LogicalPlan::Filter(filter) if filter.predicate.contains_outer() => {
                            filters += 1;
                            needs_domain |= split_conjunction(&filter.predicate)
                                .iter()
                                .any(|expr| expr.contains_outer() && !native_equality(expr));
                        }
                        LogicalPlan::Join(_)
                        | LogicalPlan::Union(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Limit(_)
                        | LogicalPlan::Subquery(_) => unsupported = true,
                        _ => unsupported |= node.contains_outer_reference(),
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                if unsupported || !needs_domain || aggregates != 1 || filters != 1 {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                }
                // The domain reads the outer input again; do not duplicate volatile
                // evaluations whose values must be shared with the original row.
                if input.exists(|node| Ok(node.expressions().iter().any(Expr::is_volatile)))? {
                    return Ok(Transformed::no(Expr::ScalarSubquery(query)));
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
                let domain = LogicalPlanBuilder::from(Arc::clone(&input))
                    .project(
                        bindings
                            .iter()
                            .map(|(_, outer, key)| Expr::Column(outer.clone()).alias(&key.name)),
                    )?
                    .distinct()?
                    .build()?;
                let output = query.subquery.schema().columns();
                let default = empty_scalar_value(&query.subquery)?;
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
                            return Ok(Transformed::yes(node));
                        }
                        let keys = || bindings.iter().map(|(_, _, key)| Expr::Column(key.clone()));
                        let result = match node {
                            LogicalPlan::Projection(mut projection) => {
                                projection.expr.extend(keys());
                                LogicalPlanBuilder::from(projection.input)
                                    .project(projection.expr)?
                                    .build()?
                            }
                            LogicalPlan::Aggregate(mut aggregate) => {
                                aggregate.group_expr.extend(keys());
                                LogicalPlanBuilder::from(aggregate.input)
                                    .aggregate(aggregate.group_expr, aggregate.aggr_expr)?
                                    .filter(correlation.clone())?
                                    .build()?
                            }
                            other => other,
                        };
                        Ok(Transformed::yes(result))
                    })?
                    .data;
                query.subquery = Arc::new(
                    LogicalPlanBuilder::from(rewritten)
                        .project(output.into_iter().map(Expr::Column))?
                        .build()?,
                );
                let result = if matches!(default, Expr::Literal(ref value, _) if value.is_null()) {
                    Expr::ScalarSubquery(query)
                } else {
                    let mut marker = query.clone();
                    marker.subquery = Arc::new(
                        LogicalPlanBuilder::from(Arc::clone(&query.subquery))
                            .project(vec![datafusion_expr::lit(true)])?
                            .build()?,
                    );
                    datafusion_expr::when(
                        Expr::ScalarSubquery(marker).is_not_null(),
                        Expr::ScalarSubquery(query),
                    )
                    .otherwise(default)?
                };
                Ok(Transformed::yes(result))
            })
            .map(|result| result.update_data(|expr| name.restore(expr)))
        })
    }
}

/// Evaluate the scalar aggregate's projection on empty input, retaining defaults
/// such as COUNT = 0 without replacing legitimate null results of nonempty groups.
fn empty_scalar_value(plan: &LogicalPlan) -> Result<Expr> {
    fn values(plan: &LogicalPlan) -> Result<Vec<Expr>> {
        match plan {
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
                .collect(),
            LogicalPlan::Projection(projection) => {
                let input = values(&projection.input)?;
                projection
                    .expr
                    .iter()
                    .map(|expr| {
                        expr.clone()
                            .transform_up(|expr| {
                                if let Expr::Column(column) = &expr {
                                    let index =
                                        projection.input.schema().index_of_column(column)?;
                                    Ok(Transformed::yes(input[index].clone()))
                                } else {
                                    Ok(Transformed::no(expr))
                                }
                            })
                            .map(|result| result.data.unalias())
                    })
                    .collect()
            }
            LogicalPlan::SubqueryAlias(alias) => values(&alias.input),
            _ => datafusion_common::not_impl_err!("empty result of scalar correlation"),
        }
    }
    values(plan)?
        .into_iter()
        .next()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("empty scalar output"))
}

fn native_equality(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op != Operator::Eq {
        return false;
    }
    let inner_key = |expr: &Expr| {
        matches!(expr, Expr::Column(_))
            || matches!(expr, Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_)))
    };
    (inner_key(&binary.left) && !binary.right.any_column_refs())
        || (inner_key(&binary.right) && !binary.left.any_column_refs())
}
