use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, WindowFrame, WindowFunctionDefinition, lit,
};
use sail_logical_plan::sort::RequiredSortNode;

/// A correlated LIMIT applies independently to every equality key. Rank the
/// decorrelated input within each key, then put the correlation above the rank.
#[derive(Debug)]
pub struct DecorrelateLimit;

impl OptimizerRule for DecorrelateLimit {
    fn name(&self) -> &str {
        "decorrelate_limit"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let outer = limit.input.all_out_ref_exprs();
        if outer.is_empty() {
            return Ok(Transformed::no(plan));
        }
        let (SkipType::Literal(skip), FetchType::Literal(fetch)) =
            (limit.get_skip_type()?, limit.get_fetch_type()?)
        else {
            return Ok(Transformed::no(plan));
        };
        let mut keys = BTreeSet::new();
        let mut eligible = true;
        limit.input.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                for predicate in split_conjunction(&filter.predicate) {
                    if !predicate.contains_outer() {
                        continue;
                    }
                    let key = match predicate {
                        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                            match (binary.left.as_ref(), binary.right.as_ref()) {
                                (Expr::Column(column), right)
                                    if right.contains_outer() && !right.any_column_refs() =>
                                {
                                    Some(column)
                                }
                                (left, Expr::Column(column))
                                    if left.contains_outer() && !left.any_column_refs() =>
                                {
                                    Some(column)
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    if let Some(key) = key {
                        keys.insert(key.clone());
                    } else {
                        eligible = false;
                    }
                }
            }
            if matches!(
                node,
                LogicalPlan::Aggregate(_)
                    | LogicalPlan::Window(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Subquery(_)
            ) {
                eligible = false;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !eligible || keys.is_empty() {
            return Ok(Transformed::no(plan));
        }
        let output = plan.schema().columns();
        let mut order = None;
        let input = Arc::unwrap_or_clone(Arc::clone(&limit.input))
            .transform_down(|node| {
                if let LogicalPlan::Sort(sort) = node {
                    if order.is_none() {
                        order = Some(sort.expr);
                    }
                    Ok(Transformed::yes(Arc::unwrap_or_clone(sort.input)))
                } else if let LogicalPlan::Extension(extension) = &node
                    && let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>()
                    && sort.fetch().is_none_or(|top| {
                        fetch.is_some_and(|fetch| top >= skip.saturating_add(fetch))
                    })
                    && !sort.preserve_partitioning()
                {
                    // Order-sensitive aggregates protect their input sort with
                    // an extension node before optimization. The per-key rank
                    // carries that ordering when decorrelating a nested limit.
                    if order.is_none() {
                        order = Some(sort.sort_expr().to_vec());
                    }
                    Ok(Transformed::yes(extension.node.inputs()[0].clone()))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        let mut pull_up = PullUpCorrelatedExpr::new();
        let input = input.rewrite(&mut pull_up)?.data;
        if !pull_up.can_pull_up {
            return Ok(Transformed::no(plan));
        }
        let rank_name = config.alias_generator().next("__sail_limit_rank");
        let rank = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            params: WindowFunctionParams {
                args: vec![],
                partition_by: keys.into_iter().map(Expr::Column).collect(),
                order_by: order.unwrap_or_default(),
                window_frame: WindowFrame::new(None),
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        }))
        .alias(&rank_name);
        let rank_column = Expr::Column(Column::new_unqualified(rank_name));
        let mut predicate = rank_column.clone().gt(lit(skip as u64));
        if let Some(fetch) = fetch {
            predicate = predicate.and(rank_column.lt_eq(lit(skip.saturating_add(fetch) as u64)));
        }
        let Some(correlation) = conjunction(pull_up.join_filters) else {
            return Ok(Transformed::no(plan));
        };
        let correlation = correlation.transform_up(|expr| {
            if let Expr::Column(column) = &expr
                && let Some(reference) = outer.iter().find(|reference| matches!(reference, Expr::OuterReferenceColumn(_, c) if c == column)) {
                    return Ok(Transformed::yes(reference.clone()));
            }
            Ok(Transformed::no(expr))
        })?.data;
        let result = LogicalPlanBuilder::from(input)
            .window(vec![rank])?
            .filter(predicate)?
            .filter(correlation)?
            .project(output.into_iter().map(Expr::Column))?
            .build()?;
        Ok(Transformed::yes(result))
    }
}
