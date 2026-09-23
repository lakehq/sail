//! Redundant semijoin reductions for selective dimension joins.
//!
//! A predicate on dimension attributes cannot be copied to a fact's foreign key.
//! Instead, copy the *eligible keys*, retaining the original join and its residual
//! predicate. Semijoins preserve fact multiplicity even when dimension keys repeat.
//! Only direct column lineage and inner/semi equality joins are traversed; grouping
//! keys are safe because removing entire groups leaves retained aggregates unchanged.
//!
//! A reduction is only generated below an aggregate or a semi join whose filter side
//! is more than a scan, which join enumeration never reorders across. Among inner
//! joins, enumeration can normally place the dimension next to a fact already, and a
//! copy of its keys would only repeat the probe of the original join.

use std::sync::Arc;

use datafusion::common::NullEquality;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};

// Bound plan growth: each reduction duplicates a small, filtered scan. This is a
// single pass over the original tree, never a fixed point over the added joins.
const MAX_REDUCTIONS: usize = 64;

mod benefit;

struct Context<'a> {
    remaining: usize,
    options: &'a super::JoinReorderOptions,
}

pub(super) fn propagate(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    options: &super::JoinReorderOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut context = Context {
        remaining: MAX_REDUCTIONS,
        options,
    };
    visit(plan, config, &mut context)
}

fn visit(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    context: &mut Context<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| visit(Arc::clone(child), config, context))
        .collect::<Result<Vec<_>>>()?;
    let plan = replace_children_if_necessary(plan, children)?;
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if !matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    ) || context.remaining == 0
    {
        return Ok(plan);
    }

    let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
    for source_side in 0..2 {
        // A semi join only requires its output side to have matches in the other
        // side. Filtering the non-output side by its own matches is redundant too.
        let source = &children[source_side];
        let Some(scan_rows) = filtered_scan_rows(source, false)? else {
            continue;
        };
        // Each reduction executes the source independently. A small filtered
        // result bounds the hash table, but does not make a large scan cheap.
        // TODO: This also caps the dimension itself. TPC-DS `item` exceeds the default
        //   (131072 rows) from SF100, which disables the `item` reductions of q64 there.
        let max_rows = config.optimizer.hash_join_single_partition_threshold_rows;
        if scan_rows > max_rows {
            continue;
        }
        let Some(rows) = row_count(source)? else {
            continue;
        };
        if rows > max_rows {
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
        let key_stats = StatisticsContext::new().compute(keys.as_ref(), &StatisticsArgs::new())?;
        // Match JoinSelection's byte-first collection threshold: the benefit
        // estimate does not account for hash-repartitioning the target scan.
        let can_collect = match key_stats.total_byte_size.get_value() {
            Some(&bytes) => bytes < config.optimizer.hash_join_single_partition_threshold,
            None => rows < max_rows,
        };
        if !can_collect {
            continue;
        }
        let restriction = Restriction {
            keys,
            rows,
            scan_rows,
            null_equality: join.null_equality(),
        };
        children[1 - source_side] = push(
            Arc::clone(&children[1 - source_side]),
            &target_keys,
            &restriction,
            0.0,
            context,
        )?;
    }
    replace_children_if_necessary(plan, children)
}

/// Return the pre-filter row count of a deterministic filtered file scan.
/// IS NOT NULL introduced by join planning is not enough to justify duplication.
/// Unknown scan sizes and sources without pre-filter statistics are ineligible.
fn filtered_scan_rows(plan: &Arc<dyn ExecutionPlan>, filtered: bool) -> Result<Option<usize>> {
    if plan.fetch().is_some() {
        return Ok(None);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        for expr in projection.expr() {
            if volatile(&expr.expr)? {
                return Ok(None);
            }
        }
        return filtered_scan_rows(projection.input(), filtered);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if volatile(filter.predicate())? {
            return Ok(None);
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
        return filtered_scan_rows(filter.input(), filtered || selective);
    }
    if !filtered {
        return Ok(None);
    }
    let Some(scan) = plan.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(None);
    };
    // FileScanConfig retains the input counts even when a predicate is pushed
    // into the file source; statistics() only marks those counts as inexact.
    Ok(config.statistics().num_rows.get_value().copied())
}

struct Restriction {
    keys: Arc<dyn ExecutionPlan>,
    rows: usize,
    scan_rows: usize,
    null_equality: NullEquality,
}

fn push(
    plan: Arc<dyn ExecutionPlan>,
    keys: &[usize],
    restriction: &Restriction,
    downstream_work: f64,
    context: &mut Context<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if context.remaining == 0 || plan.fetch().is_some() {
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
                downstream_work,
                context,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if !volatile(filter.predicate())? {
            // Preserve existing scan filters below the reduction. Besides avoiding
            // extra probes, this costs the candidate using the filtered target's
            // key domain (e.g. facts already restricted to the selected year).
            if is_scan_pipeline(filter.input(), true)? {
                return insert(plan, keys, restriction, downstream_work, context);
            }
            let mapped = keys
                .iter()
                .map(|&i| filter.projection().as_ref().map_or(i, |p| p[i]))
                .collect::<Vec<_>>();
            let child = push(
                Arc::clone(filter.input()),
                &mapped,
                restriction,
                downstream_work,
                context,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        for (expr, _) in aggregate.group_expr().expr() {
            if volatile(expr)? {
                return Ok(plan);
            }
        }
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
                    context.options.build_side_weight + context.options.output_weight,
                    context,
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
            // Credit starts below a reorder boundary. Until one is crossed, the inputs
            // of an inner join are relations that join enumeration can normally place
            // the dimension next to itself.
            let credited = downstream_work > 0.0 || reorder_boundary(join)?;
            let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
            for (side, keys) in mapped.into_iter().enumerate() {
                if let Some(keys) = keys {
                    let downstream_work = if credited {
                        context.options.output_weight
                            + if side == 0 {
                                context.options.build_side_weight
                            } else {
                                context.options.probe_side_weight
                            }
                    } else {
                        0.0
                    };
                    children[side] = push(
                        Arc::clone(&children[side]),
                        &keys,
                        restriction,
                        downstream_work,
                        context,
                    )?;
                }
            }
            return replace_children_if_necessary(plan, children);
        }
    } else if plan.is::<DataSourceExec>() {
        return insert(plan, keys, restriction, downstream_work, context);
    }
    // Outer/anti/mark joins, limits, windows, computed keys and unknown operators
    // are boundaries. Never infer equality through casts or aggregate results.
    Ok(plan)
}

fn insert(
    plan: Arc<dyn ExecutionPlan>,
    keys: &[usize],
    restriction: &Restriction,
    downstream_work: f64,
    context: &mut Context<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if downstream_work <= 0.0
        || !benefit::worthwhile(&plan, keys, restriction, downstream_work, context.options)?
    {
        return Ok(plan);
    }
    let on: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, &key)| (column(&restriction.keys, i), column(&plan, key)))
        .collect();
    context.remaining -= 1;
    // Hash semijoins enumerate duplicate build matches before discarding them.
    // Deduplicate only the copied keys; the original join retains multiplicity.
    let distinct_keys = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(
            (0..keys.len())
                .map(|i| {
                    (
                        column(&restriction.keys, i),
                        format!("__early_join_key_{i}"),
                    )
                })
                .collect(),
        ),
        vec![],
        vec![],
        // Each independent consumer needs its own scan queue.
        reset_plan_states(Arc::clone(&restriction.keys))?,
        restriction.keys.schema(),
    )?);
    Ok(Arc::new(HashJoinExec::try_new(
        distinct_keys,
        plan,
        on,
        None,
        &JoinType::RightSemi,
        None,
        PartitionMode::Auto,
        restriction.null_equality,
        false,
    )?))
}

/// Whether the inputs of `join` are out of reach of join enumeration. An inner join
/// is not: enumeration normally orders inner joins itself. No semi join is reordered,
/// but one that merely filters by a scan (e.g. a dimension join rewritten for
/// DISTINCT) is as cheap to probe as the reduction. This shape test is conservative:
/// only a filter side that is more than a projected and filtered scan, such as an
/// aggregate, counts.
fn reorder_boundary(join: &HashJoinExec) -> Result<bool> {
    Ok(match join.join_type() {
        JoinType::LeftSemi => !is_scan_pipeline(join.right(), false)?,
        JoinType::RightSemi => !is_scan_pipeline(join.left(), false)?,
        _ => false,
    })
}

/// `column_only` rejects computed projections, which keys cannot be mapped through.
fn is_scan_pipeline(plan: &Arc<dyn ExecutionPlan>, column_only: bool) -> Result<bool> {
    if plan.fetch().is_some() {
        return Ok(false);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let mut supported = true;
        for p in projection.expr() {
            supported &= p.expr.is::<Column>() || (!column_only && !volatile(&p.expr)?);
        }
        if supported {
            return is_scan_pipeline(projection.input(), column_only);
        }
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>()
        && !volatile(filter.predicate())?
    {
        return is_scan_pipeline(filter.input(), column_only);
    }
    Ok(plan.is::<DataSourceExec>())
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
    use datafusion::datasource::memory::MemorySourceConfig;
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
    fn only_a_semi_join_with_a_computed_filter_side_is_a_reorder_boundary() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let scan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let computed: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let boundary =
            |left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, join_type| {
                reorder_boundary(&HashJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    vec![(column(left, 0), column(right, 0))],
                    None,
                    &join_type,
                    None,
                    PartitionMode::Auto,
                    NullEquality::NullEqualsNothing,
                    false,
                )?)
            };
        assert!(boundary(&scan, &computed, JoinType::LeftSemi)?);
        assert!(!boundary(&computed, &scan, JoinType::LeftSemi)?);
        assert!(boundary(&computed, &scan, JoinType::RightSemi)?);
        assert!(!boundary(&scan, &computed, JoinType::RightSemi)?);
        assert!(!boundary(&computed, &computed, JoinType::Inner)?);
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
