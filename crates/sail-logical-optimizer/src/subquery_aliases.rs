use std::collections::HashSet;
use std::sync::Arc;

use datafusion::optimizer::AnalyzerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr_rewriter::{NamePreserver, SavedName};
use datafusion_expr::{Expr, LogicalPlan, Projection};

/// Opaque field IDs already identify resolved subquery attributes. Remove redundant
/// relation aliases before decorrelation: native predicate pull-up otherwise carries
/// a source column through a rename projection with a stale relation qualifier.
#[derive(Debug)]
pub struct NormalizeSubqueryAliases;

impl AnalyzerRule for NormalizeSubqueryAliases {
    fn name(&self) -> &str {
        "normalize_subquery_aliases"
    }
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|plan| {
                if let LogicalPlan::Subquery(mut query) = plan {
                    query.subquery =
                        Arc::new(normalize(Arc::unwrap_or_clone(query.subquery), true)?);
                    return Ok(Transformed::yes(LogicalPlan::Subquery(query)));
                }
                plan.map_expressions(|expr| {
                    expr.transform_up(|expr| match expr {
                        Expr::ScalarSubquery(mut query) => {
                            query.subquery =
                                Arc::new(normalize(Arc::unwrap_or_clone(query.subquery), false)?);
                            Ok(Transformed::yes(Expr::ScalarSubquery(query)))
                        }
                        Expr::Exists(mut exists) => {
                            exists.subquery.subquery = Arc::new(normalize(
                                Arc::unwrap_or_clone(exists.subquery.subquery),
                                false,
                            )?);
                            Ok(Transformed::yes(Expr::Exists(exists)))
                        }
                        Expr::InSubquery(mut query) => {
                            query.subquery.subquery = Arc::new(normalize(
                                Arc::unwrap_or_clone(query.subquery.subquery),
                                false,
                            )?);
                            Ok(Transformed::yes(Expr::InSubquery(query)))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })
                })
            })?
            .data)
    }
}

fn normalize(plan: LogicalPlan, restore_qualifiers: bool) -> Result<LogicalPlan> {
    if plan.all_out_ref_exprs().is_empty() {
        return Ok(plan);
    }
    // Grouping-set expansion introduces generated field identities consumed by
    // NormalizeScalarSubqueryKeys; retain that established normalization path.
    if plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::Aggregate(aggregate)
        if aggregate.group_expr.iter().any(|expr| matches!(expr, Expr::GroupingSet(_)))))
    })? {
        return Ok(plan);
    }
    let output = plan.schema().columns();
    let mut local_names = HashSet::new();
    plan.apply(|node| {
        local_names.extend(
            node.schema()
                .fields()
                .iter()
                .map(|field| field.name().clone()),
        );
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Nested queries refer to this scope using outer references. Keep their
    // bindings consistent when this scope's redundant relation aliases disappear.
    let plan = plan
        .transform_up_with_subqueries(|node| {
            node.map_expressions(|expr| {
                expr.transform_up(|expr| {
                    let strip = |expr: Expr| {
                        if let Expr::OuterReferenceColumn(field, mut column) = expr {
                            if local_names.contains(&column.name) {
                                column.relation = None;
                            }
                            Expr::OuterReferenceColumn(field, column)
                        } else {
                            expr
                        }
                    };
                    let expr = match expr {
                        Expr::ScalarSubquery(mut query) => {
                            query.outer_ref_columns =
                                query.outer_ref_columns.into_iter().map(strip).collect();
                            Expr::ScalarSubquery(query)
                        }
                        Expr::Exists(mut exists) => {
                            exists.subquery.outer_ref_columns = exists
                                .subquery
                                .outer_ref_columns
                                .into_iter()
                                .map(strip)
                                .collect();
                            Expr::Exists(exists)
                        }
                        Expr::InSubquery(mut query) => {
                            query.subquery.outer_ref_columns = query
                                .subquery
                                .outer_ref_columns
                                .into_iter()
                                .map(strip)
                                .collect();
                            Expr::InSubquery(query)
                        }
                        other => strip(other),
                    };
                    Ok(Transformed::yes(expr))
                })
            })
        })?
        .data;
    let plan = plan
        .transform_up(|plan| {
            if let LogicalPlan::SubqueryAlias(alias) = plan {
                return Ok(Transformed::yes(Arc::unwrap_or_clone(alias.input)));
            }
            let preserver = NamePreserver::new(&plan);
            let expressions = plan
                .expressions()
                .into_iter()
                .map(|expr| {
                    let saved_name = match preserver.save(&expr) {
                        SavedName::Saved { name, .. } => SavedName::Saved { relation: None, name },
                        other => other,
                    };
                    let expr = expr
                        .transform_up(|expr| match expr {
                            Expr::Column(mut column) => {
                                column.relation = None;
                                Ok(Transformed::yes(Expr::Column(column)))
                            }
                            Expr::Alias(mut alias) => {
                                alias.relation = None;
                                if matches!(alias.expr.as_ref(), Expr::Column(column) if column.name == alias.name) {
                                    Ok(Transformed::yes(*alias.expr))
                                } else {
                                    Ok(Transformed::yes(Expr::Alias(alias)))
                                }
                            }
                            expr => Ok(Transformed::no(expr)),
                        })?
                        .data;
                    Ok(saved_name.restore(expr))
                })
                .collect::<Result<Vec<_>>>()?;
            let inputs = plan.inputs().into_iter().cloned().collect();
            Ok(Transformed::yes(plan.with_new_exprs(expressions, inputs)?))
        })?
        .data;
    if !restore_qualifiers {
        return Ok(plan);
    }
    let mut plan = plan;
    // Merge projection-only lateral scopes before projection pruning sees their
    // outer references. Otherwise the enclosing join can discard an outer key.
    while let LogicalPlan::Projection(projection) = &plan {
        let LogicalPlan::Projection(inner) = projection.input.as_ref() else {
            break;
        };
        if inner.expr.iter().any(Expr::is_volatile) {
            break;
        }
        let exprs = projection
            .expr
            .iter()
            .map(|expr| {
                expr.clone()
                    .transform_up(|expr| {
                        if let Expr::Column(column) = &expr {
                            let index = projection.input.schema().index_of_column(column)?;
                            Ok(Transformed::yes(inner.expr[index].clone().unalias()))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    })
                    .map(|result| result.data)
            })
            .collect::<Result<Vec<_>>>()?;
        plan = LogicalPlan::Projection(Projection::try_new(exprs, Arc::clone(&inner.input))?);
    }
    if let LogicalPlan::Projection(projection) = plan {
        let expr = projection
            .expr
            .into_iter()
            .zip(output)
            .map(|(expr, column)| expr.unalias().alias_qualified(column.relation, column.name))
            .collect();
        return Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            projection.input,
        )?));
    }
    Ok(LogicalPlan::Projection(Projection::try_new(
        output
            .into_iter()
            .map(|column| {
                let expr = Expr::Column(datafusion_common::Column::new_unqualified(&column.name));
                if column.relation.is_some() {
                    expr.alias_qualified(column.relation, column.name)
                } else {
                    expr
                }
            })
            .collect(),
        Arc::new(plan),
    )?))
}
