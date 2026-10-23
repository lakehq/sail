use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr_rewriter::{NamePreserver, strip_outer_reference};
use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::utils::{collect_subquery_cols, conjunction, split_conjunction};
use datafusion_expr::{Expr, LogicalPlan, Operator, Volatility};

/// Group native correlated scalar aggregates by the value used in cast equality.
#[derive(Debug)]
pub struct NormalizeScalarSubqueryKeys;

impl OptimizerRule for NormalizeScalarSubqueryKeys {
    fn name(&self) -> &str {
        "normalize_scalar_subquery_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Filter(_)) {
            return Ok(Transformed::no(plan));
        }
        let names = NamePreserver::new(&plan);
        let mut reserved = plan.schema().field_names().into_iter().collect();
        plan.map_expressions(|expression| {
            let name = names.save(&expression);
            expression
                .transform_up(|expression| {
                    let Expr::ScalarSubquery(mut query) = expression else {
                        return Ok(Transformed::no(expression));
                    };
                    if query.outer_ref_columns.is_empty() {
                        return Ok(Transformed::no(Expr::ScalarSubquery(query)));
                    }
                    normalize_keys(&query.subquery, config.alias_generator(), &mut reserved).map(
                        |result| {
                            if result.transformed {
                                query.subquery = Arc::new(result.data);
                            }
                            Transformed::new_transformed(
                                Expr::ScalarSubquery(query),
                                result.transformed,
                            )
                        },
                    )
                })
                .map(|result| result.update_data(|expression| name.restore(expression)))
        })
    }
}

fn native_equality(expression: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::BinaryExpr(binary) = expression else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let inner_key = |expression: &Expr| {
        matches!(expression, Expr::Column(_))
            || matches!(expression, Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Column(_)))
    };
    if inner_key(&binary.left) && !binary.right.any_column_refs() && binary.right.contains_outer() {
        Some((&binary.left, &binary.right))
    } else if inner_key(&binary.right)
        && !binary.left.any_column_refs()
        && binary.left.contains_outer()
    {
        Some((&binary.right, &binary.left))
    } else {
        None
    }
}

fn normalize_keys(
    plan: &LogicalPlan,
    aliases: &AliasGenerator,
    reserved: &mut BTreeSet<String>,
) -> Result<Transformed<LogicalPlan>> {
    let mut eligible = true;
    let mut has_aggregate = false;
    plan.apply(|node| {
        reserved.extend(node.schema().field_names());
        has_aggregate |= matches!(node, LogicalPlan::Aggregate(_));
        if matches!(node, LogicalPlan::Subquery(_))
            || (!matches!(node, LogicalPlan::Filter(_)) && node.contains_outer_reference())
            || (matches!(
                node,
                LogicalPlan::Union(_)
                    | LogicalPlan::Sort(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Extension(_)
            ) && !node.all_out_ref_exprs().is_empty())
        {
            eligible = false;
        }
        if matches!(node, LogicalPlan::Window(_) | LogicalPlan::Unnest(_)) {
            // shortcut: skip keys here until native pull-up can extend these schemas.
            eligible &= !node.exists(|input| {
                let LogicalPlan::Filter(filter) = input else {
                    return Ok(false);
                };
                Ok(split_conjunction(&filter.predicate)
                    .into_iter()
                    .any(|predicate| {
                        matches!(native_equality(predicate), Some((Expr::Cast(_), _)))
                    }))
            })?;
        }
        for expression in node.expressions() {
            eligible &= !expression.exists(|part| {
                Ok(match part {
                    Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_) => true,
                    Expr::ScalarFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::HigherOrderFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::AggregateFunction(function) => {
                        function.func.signature().volatility == Volatility::Volatile
                    }
                    Expr::WindowFunction(function) => {
                        function.fun.signature().volatility == Volatility::Volatile
                    }
                    _ => false,
                })
            })?;
        }
        if let LogicalPlan::Filter(filter) = node {
            eligible &= split_conjunction(&filter.predicate)
                .into_iter()
                .filter(|expression| expression.contains_outer())
                .all(|expression| native_equality(expression).is_some());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !eligible || !has_aggregate {
        return Ok(Transformed::no(plan.clone()));
    }

    let normalized = plan.clone().transform_up(|node| {
        let LogicalPlan::Filter(filter) = &node else {
            return Ok(Transformed::no(node));
        };
        let mut keys: Vec<(Expr, Column)> = Vec::new();
        let (correlated, local): (Vec<_>, Vec<_>) = split_conjunction(&filter.predicate)
            .into_iter()
            .partition(|expression| expression.contains_outer());
        let predicates = correlated
            .into_iter()
            .map(|predicate| {
                let Some((key @ Expr::Cast(_), outer)) = native_equality(predicate) else {
                    return predicate.clone();
                };
                let column = if let Some((_, column)) = keys.iter().find(|(expr, _)| expr == key) {
                    column.clone()
                } else {
                    let name = loop {
                        let name = aliases.next("__sail_correlation_key");
                        if reserved.insert(name.clone()) {
                            break name;
                        }
                    };
                    let column = Column::from_name(name);
                    keys.push((key.clone(), column.clone()));
                    column
                };
                Expr::Column(column).eq(outer.clone())
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(Transformed::no(node));
        }
        let schema = Arc::clone(node.schema());
        let mut expressions = schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        expressions.extend(keys.into_iter().map(|(key, column)| key.alias(column.name)));
        // Native pull-up retains local filters below aggregation. Evaluate casts
        // after those filters so excluded rows cannot introduce cast failures.
        let input = if let Some(predicate) = conjunction(local.into_iter().cloned()) {
            Arc::new(LogicalPlan::Filter(Filter::try_new(
                predicate,
                Arc::clone(&filter.input),
            )?))
        } else {
            Arc::clone(&filter.input)
        };
        let input = LogicalPlan::Projection(Projection::try_new(expressions, input)?);
        let predicate = conjunction(predicates).unwrap_or_else(|| filter.predicate.clone());
        let filtered = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?);
        // Keep every original field; native pull-up carries the hidden key to its aggregate.
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::new_from_schema(Arc::new(filtered), schema),
        )))
    })?;
    if !normalized.transformed {
        return Ok(normalized);
    }

    let mut correlations = Vec::new();
    normalized.data.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            correlations.extend(
                split_conjunction(&filter.predicate)
                    .into_iter()
                    .filter(|expression| expression.contains_outer())
                    .map(|expression| strip_outer_reference(expression.clone())),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Preserve grouped producers when replacing a cast would violate native's
    // requirement that every original grouping column occurs in a correlation.
    let invalid_grouping = normalized.data.exists(|node| {
        let LogicalPlan::Aggregate(aggregate) = node else {
            return Ok(false);
        };
        let correlated = collect_subquery_cols(&correlations, aggregate.input.schema())?;
        Ok(aggregate
            .group_expr
            .iter()
            .flat_map(Expr::column_refs)
            .any(|column| !correlated.contains(column)))
    })?;
    if invalid_grouping {
        Ok(Transformed::no(plan.clone()))
    } else {
        Ok(normalized)
    }
}
