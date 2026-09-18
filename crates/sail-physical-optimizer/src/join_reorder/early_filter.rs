//! Redundant semijoin reductions for selective dimension joins.
//!
//! A predicate on dimension attributes cannot be copied to a fact's foreign key.
//! Instead, copy the *eligible keys*, retaining the original join and its residual
//! predicate. Semijoins preserve fact multiplicity even when dimension keys repeat.
//! Only direct column lineage and inner/semi equality joins are traversed; grouping
//! keys are safe because removing entire groups leaves retained aggregates unchanged.

use std::sync::Arc;

use datafusion::common::NullEquality;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};

// Bound plan growth: each reduction duplicates a small, filtered scan. This is a
// single pass over the original tree, never a fixed point over the added joins.
const MAX_REDUCTIONS: usize = 64;

pub(super) fn propagate(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut remaining = MAX_REDUCTIONS;
    visit(plan, config, &mut remaining)
}

fn visit(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    remaining: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| visit(Arc::clone(child), config, remaining))
        .collect::<Result<Vec<_>>>()?;
    let plan = replace_children_if_necessary(plan, children)?;
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if !matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    ) || *remaining == 0
    {
        return Ok(plan);
    }

    let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
    for source_side in 0..2 {
        // A semi join only requires its output side to have matches in the other
        // side. Filtering the non-output side by its own matches is redundant too.
        let source = &children[source_side];
        if !is_filtered_scan(source, false)? {
            continue;
        }
        let Some(rows) = row_count(source)? else {
            continue;
        };
        if rows > config.optimizer.hash_join_single_partition_threshold_rows {
            continue;
        }
        let mut source_keys = Vec::new();
        let mut target_keys = Vec::new();
        for (left, right) in join.on() {
            let (source, target) = if source_side == 0 {
                (left, right)
            } else {
                (right, left)
            };
            let (Some(source), Some(target)) = (
                source.downcast_ref::<Column>(),
                target.downcast_ref::<Column>(),
            ) else {
                continue;
            };
            source_keys.push(source.index());
            target_keys.push(target.index());
        }
        if source_keys.is_empty() {
            continue;
        }
        let expr: Vec<_> = source_keys
            .iter()
            .enumerate()
            .map(|(i, &index)| ProjectionExpr {
                expr: column(source, index),
                alias: format!("__early_join_key_{i}"),
            })
            .collect();
        let keys = Arc::new(ProjectionExec::try_new(expr, Arc::clone(source))?);
        let restriction = Restriction {
            keys,
            rows,
            null_equality: join.null_equality(),
        };
        children[1 - source_side] = push(
            Arc::clone(&children[1 - source_side]),
            &target_keys,
            &restriction,
            false,
            remaining,
        )?;
    }
    replace_children_if_necessary(plan, children)
}

/// Only duplicate deterministic filtered scans, not arbitrary subqueries or UDFs
/// with volatile evaluation. IS NOT NULL introduced by join planning is not enough
/// to justify an additional scan and hash table.
fn is_filtered_scan(plan: &Arc<dyn ExecutionPlan>, filtered: bool) -> Result<bool> {
    if plan.fetch().is_some() {
        return Ok(false);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        for expr in projection.expr() {
            if volatile(&expr.expr)? {
                return Ok(false);
            }
        }
        return is_filtered_scan(projection.input(), filtered);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if volatile(filter.predicate())? {
            return Ok(false);
        }
        let selective = filter.predicate().exists(|expr| {
            Ok(expr.is::<InListExpr>()
                || expr.downcast_ref::<BinaryExpr>().is_some_and(|binary| {
                    matches!(
                        binary.op(),
                        Operator::Eq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq
                    )
                }))
        })?;
        return is_filtered_scan(filter.input(), filtered || selective);
    }
    Ok(filtered && plan.is::<DataSourceExec>())
}

struct Restriction {
    keys: Arc<dyn ExecutionPlan>,
    rows: usize,
    null_equality: NullEquality,
}

fn push(
    plan: Arc<dyn ExecutionPlan>,
    keys: &[usize],
    restriction: &Restriction,
    crossed_boundary: bool,
    remaining: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if *remaining == 0 || plan.fetch().is_some() {
        return Ok(plan);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        for expr in projection.expr() {
            if volatile(&expr.expr)? {
                return Ok(plan);
            }
        }
        let mapped = keys
            .iter()
            .map(|&i| {
                projection
                    .expr()
                    .get(i)?
                    .expr
                    .downcast_ref::<Column>()
                    .map(Column::index)
            })
            .collect::<Option<Vec<_>>>();
        if let Some(mapped) = mapped {
            let child = push(
                Arc::clone(projection.input()),
                &mapped,
                restriction,
                crossed_boundary,
                remaining,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if !volatile(filter.predicate())? {
            let mapped = keys
                .iter()
                .map(|&i| filter.projection().as_ref().map_or(i, |p| p[i]))
                .collect::<Vec<_>>();
            let child = push(
                Arc::clone(filter.input()),
                &mapped,
                restriction,
                crossed_boundary,
                remaining,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        // Grouping sets can synthesize NULL keys and global aggregation can emit
        // a row on empty input. Neither allows this transformation.
        if !aggregate.group_expr().has_grouping_set() {
            let mapped = keys
                .iter()
                .map(|&i| {
                    aggregate
                        .group_expr()
                        .expr()
                        .get(i)?
                        .0
                        .downcast_ref::<Column>()
                        .map(Column::index)
                })
                .collect::<Option<Vec<_>>>();
            if let Some(mapped) = mapped {
                let child = push(
                    Arc::clone(aggregate.input()),
                    &mapped,
                    restriction,
                    true,
                    remaining,
                )?;
                return replace_children_if_necessary(plan, vec![child]);
            }
        }
    } else if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if matches!(
            join.join_type(),
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            let mapped = join_input_keys(join, keys);
            let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
            for (side, keys) in mapped.into_iter().enumerate() {
                if let Some(keys) = keys {
                    children[side] = push(
                        Arc::clone(&children[side]),
                        &keys,
                        restriction,
                        true,
                        remaining,
                    )?;
                }
            }
            return replace_children_if_necessary(plan, children);
        }
    } else if plan.is::<DataSourceExec>() && crossed_boundary {
        // Do not add a second join next to a dimension/fact join already at a
        // scan. Require a substantially larger target to amortize the extra work.
        if row_count(&plan)?.is_some_and(|rows| rows > restriction.rows.saturating_mul(4)) {
            let on = keys
                .iter()
                .enumerate()
                .map(|(i, &key)| (column(&restriction.keys, i), column(&plan, key)))
                .collect();
            *remaining -= 1;
            return Ok(Arc::new(HashJoinExec::try_new(
                // A cloned DataSourceExec shares its work-stealing file queue.
                // Each use must scan the entire eligible-key set independently.
                reset_plan_states(Arc::clone(&restriction.keys))?,
                plan,
                on,
                None,
                &JoinType::RightSemi,
                None,
                PartitionMode::Auto,
                restriction.null_equality,
                false,
            )?));
        }
    }
    // Outer/anti/mark joins, limits, windows, computed keys and unknown operators
    // are boundaries. Never infer equality through casts or aggregate results.
    Ok(plan)
}

/// Map each key to both inputs using column equality. Keeping the incoming null
/// equality is safe even across ordinary equality: the original join still rejects
/// null matches. Requiring the whole tuple on an input preserves composite keys.
fn join_input_keys(join: &HashJoinExec, keys: &[usize]) -> [Option<Vec<usize>>; 2] {
    let left_len = join.left().schema().fields().len();
    let output = keys
        .iter()
        .map(|&i| {
            let i = join.projection.as_ref().map_or(i, |p| p[i]);
            match join.join_type() {
                JoinType::LeftSemi => (0, i),
                JoinType::RightSemi => (1, i),
                _ if i < left_len => (0, i),
                _ => (1, i - left_len),
            }
        })
        .collect::<Vec<_>>();
    [0, 1].map(|side| {
        output
            .iter()
            .map(|&(origin, index)| {
                if origin == side {
                    return Some(index);
                }
                join.on().iter().find_map(|(left, right)| {
                    let (from, to) = if origin == 0 {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    let from = from.downcast_ref::<Column>()?;
                    let to = to.downcast_ref::<Column>()?;
                    (from.index() == index).then_some(to.index())
                })
            })
            .collect()
    })
}

fn column(plan: &Arc<dyn ExecutionPlan>, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(plan.schema().field(index).name(), index))
}

fn volatile(expr: &Arc<dyn PhysicalExpr>) -> Result<bool> {
    expr.exists(|expr| Ok(expr.is_volatile_node()))
}

fn row_count(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<usize>> {
    let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
    Ok(stats.num_rows.get_value().copied())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn join(join_type: JoinType, projection: Option<Vec<usize>>) -> Result<HashJoinExec> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("tenant", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ]))));
        HashJoinExec::try_new(
            Arc::clone(&input),
            Arc::clone(&input),
            vec![
                (column(&input, 0), column(&input, 0)),
                (column(&input, 1), column(&input, 1)),
            ],
            None,
            &join_type,
            projection,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false,
        )
    }

    #[test]
    fn composite_keys_follow_join_projection_and_equalities() -> Result<()> {
        let join = join(JoinType::Inner, Some(vec![4, 0, 5]))?;
        assert_eq!(
            join_input_keys(&join, &[0, 1]),
            [Some(vec![1, 0]), Some(vec![1, 0])]
        );
        // The non-key column cannot be inferred on the other input, even when
        // another component of the restriction has an equality there.
        assert_eq!(join_input_keys(&join, &[0, 2]), [None, Some(vec![1, 2])]);
        Ok(())
    }

    #[test]
    fn semi_join_output_indices_are_relative_to_the_preserved_side() -> Result<()> {
        for join_type in [JoinType::LeftSemi, JoinType::RightSemi] {
            let join = join(join_type, Some(vec![1, 0, 2]))?;
            assert_eq!(
                join_input_keys(&join, &[0, 1]),
                [Some(vec![1, 0]), Some(vec![1, 0])]
            );
            let expected = if join_type == JoinType::LeftSemi {
                [Some(vec![2]), None]
            } else {
                [None, Some(vec![2])]
            };
            assert_eq!(join_input_keys(&join, &[2]), expected);
        }
        Ok(())
    }
}
