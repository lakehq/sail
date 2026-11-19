// SPDX-License-Identifier: Apache-2.0

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

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{NullEquality, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use sail_physical_plan::scalar_subquery::reads_scalar_subquery;
use sail_physical_plan::shared_cte::{SharedCteRestriction, SharedCteScanExec};

// Bound plan growth: each reduction duplicates a small, filtered scan. This is a
// single pass over the original tree, never a fixed point over the added joins.
const MAX_REDUCTIONS: usize = 64;

mod benefit;

struct Context<'a> {
    remaining: usize,
    options: &'a super::JoinReorderOptions,
}

/// The restrictions of the references of a shared CTE (see [`SharedCteRestriction`]),
/// for the rule that optimizes the plan of the CTE. The query planner adds them to the
/// configuration options that it plans the CTE with, which each rule receives however
/// the rules of the session are wrapped.
#[derive(Debug, Clone)]
pub struct SharedCteRestrictions {
    /// The restrictions of each reference.
    references: Vec<Vec<SharedCteRestriction>>,
    /// Whether the rule added a reduction to the plan of the CTE for the restrictions.
    applied: Arc<AtomicBool>,
}

impl SharedCteRestrictions {
    pub fn new(references: Vec<Vec<SharedCteRestriction>>) -> Self {
        Self {
            references,
            applied: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns whether the rule restricted a plan to the keys of the references, for
    /// these restrictions or a copy of them.
    pub fn applied(&self) -> bool {
        self.applied.load(Ordering::Relaxed)
    }
}

impl ConfigExtension for SharedCteRestrictions {
    const PREFIX: &'static str = "sail_shared_cte_restrictions";
}

impl ExtensionOptions for SharedCteRestrictions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        plan_err!("the shared CTE restrictions have no option: {key}")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
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
    let plan = visit(plan, config, &mut context)?;
    match config.extensions.get::<SharedCteRestrictions>() {
        Some(restrictions) => restrict(plan, restrictions, config, &mut context),
        None => Ok(plan),
    }
}

/// Copies the keys of all the references of a shared CTE into the plan of the CTE, as
/// the keys of each reference are copied into a CTE that is in the place of its
/// reference. Each reference reads the rows of the keys of all the references, which
/// include the rows of its own keys, and the join that has its keys remains above it.
fn restrict(
    plan: Arc<dyn ExecutionPlan>,
    restrictions: &SharedCteRestrictions,
    config: &ConfigOptions,
    context: &mut Context<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some((columns, restriction)) = union(&restrictions.references, config)? else {
        return Ok(plan);
    };
    // The requirements of the output are above the plan of the CTE.
    let body = match plan.downcast_ref::<OutputRequirementExec>() {
        Some(requirements) => requirements.input(),
        None => Arc::clone(&plan),
    };
    let remaining = context.remaining;
    let body = push(body, &columns, &restriction, 0.0, context)?;
    if context.remaining == remaining {
        return Ok(plan);
    }
    restrictions.applied.store(true, Ordering::Relaxed);
    if plan.is::<OutputRequirementExec>() {
        replace_children_if_necessary(plan, vec![body])
    } else {
        Ok(body)
    }
}

/// Returns the columns of a shared CTE that every reference is restricted on, and the
/// restriction of these columns to the keys of all the references. A reference with
/// several restrictions reads no more than the rows of any one of them, so the first
/// columns whose keys are within the limits are taken.
fn union(
    references: &[Vec<SharedCteRestriction>],
    config: &ConfigOptions,
) -> Result<Option<(Vec<usize>, Restriction)>> {
    let Some((first, others)) = references.split_first() else {
        return Ok(None);
    };
    let types = |keys: &Arc<dyn ExecutionPlan>| {
        let schema = keys.schema();
        let types = schema.fields().iter().map(|x| x.data_type().clone());
        types.collect::<Vec<_>>()
    };
    'candidates: for candidate in first {
        let sources = others
            .iter()
            .map(|reference| {
                reference.iter().find(|x| {
                    x.columns == candidate.columns
                        && x.null_equality == candidate.null_equality
                        && types(&x.keys) == types(&candidate.keys)
                })
            })
            .collect::<Option<Vec<_>>>();
        let Some(sources) = sources else {
            continue;
        };
        let (mut keys, mut rows, mut scan_rows) = (vec![], 0usize, 0usize);
        for source in std::iter::once(candidate).chain(sources) {
            let (Some(source_scan_rows), Some(source_rows)) = (
                filtered_scan_rows(&source.keys, false)?,
                row_count(&source.keys)?,
            ) else {
                continue 'candidates;
            };
            keys.push(Arc::clone(&source.keys));
            rows = rows.saturating_add(source_rows);
            scan_rows = scan_rows.saturating_add(source_scan_rows);
        }
        // The keys of all the references are collected together, so the limits of a
        // collected join input apply to all of them, and so does the estimate of the
        // benefit, which has the rows and the statistics of all the keys. The rows are
        // at most the sum of the rows of the sources. The scan of each source is
        // within the limit of the rows that a reduction scans, which it met before its
        // keys reached the reference, and it is executed once here, as it is by a
        // reference that evaluates the CTE itself.
        let max_rows = config.optimizer.hash_join_single_partition_threshold_rows;
        let keys = UnionExec::try_new(keys)?;
        if rows > max_rows || !can_collect(&keys, rows, config)? {
            continue;
        }
        let restriction = Restriction {
            keys,
            rows,
            scan_rows,
            null_equality: candidate.null_equality,
        };
        return Ok(Some((candidate.columns.clone(), restriction)));
    }
    Ok(None)
}

/// Match JoinSelection's byte-first collection threshold: the benefit
/// estimate does not account for hash-repartitioning the target scan.
fn can_collect(keys: &Arc<dyn ExecutionPlan>, rows: usize, config: &ConfigOptions) -> Result<bool> {
    let key_stats = StatisticsContext::new().compute(keys.as_ref(), &StatisticsArgs::new())?;
    Ok(match key_stats.total_byte_size.get_value() {
        Some(&bytes) => bytes < config.optimizer.hash_join_single_partition_threshold,
        None => rows < config.optimizer.hash_join_single_partition_threshold_rows,
    })
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
        let keys: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(expr, Arc::clone(source))?);
        if !can_collect(&keys, rows, config)? {
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
    } else if let Some(scan) = plan.downcast_ref::<SharedCteScanExec>()
        && !restriction.keys.exists(reads_scalar_subquery)?
    {
        // The plan of a shared CTE is not in the place of its references, so the keys
        // cannot be copied into it for one of them. The reference records that it reads
        // only the rows of these keys, and when each reference is restricted, the query
        // planner has the keys of all of them copied into the plan of the CTE.
        // The plan of the CTE is not below the operator that provides the result of a
        // scalar subquery of the plan of the references, so keys that depend on such a
        // result are not recorded.
        let columns = keys
            .iter()
            .map(|&i| scan.projection().map_or(i, |projection| projection[i]))
            .collect();
        return Ok(Arc::new(scan.with_restriction(SharedCteRestriction {
            keys: Arc::clone(&restriction.keys),
            columns,
            null_equality: restriction.null_equality,
        })));
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
    use datafusion::common::internal_err;
    use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
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
    fn a_shared_cte_reference_records_the_restriction() -> Result<()> {
        use datafusion::physical_plan::{Partitioning, Statistics};
        use sail_physical_plan::shared_cte::{SharedCte, shared_cte_restrictions};

        let schema = |names: [&str; 2]| {
            Arc::new(Schema::new(vec![
                Field::new(names[0], DataType::Int64, true),
                Field::new(names[1], DataType::Int64, true),
            ]))
        };
        let cte = Arc::new(SharedCte::new(
            3,
            "c".to_string(),
            Partitioning::UnknownPartitioning(1),
        ));
        // The reference reads the columns of the CTE in the other order.
        let scan: Arc<dyn ExecutionPlan> = Arc::new(SharedCteScanExec::new(
            cte,
            schema(["key", "value"]),
            &Statistics::new_unknown(&schema(["value", "key"])),
            Some(vec![1, 0]),
        ));
        let reference = |scan: &Arc<dyn ExecutionPlan>, key: usize| -> Result<_> {
            Ok(Arc::new(ProjectionExec::try_new(
                vec![ProjectionExpr {
                    expr: column(scan, key),
                    alias: "k".to_string(),
                }],
                Arc::clone(scan),
            )?) as Arc<dyn ExecutionPlan>)
        };
        let keys: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
                "__early_join_key_0",
                DataType::Int64,
                true,
            )]))));
        let restriction = Restriction {
            keys: Arc::clone(&keys),
            rows: 1,
            scan_rows: 1,
            null_equality: NullEquality::NullEqualsNull,
        };
        let options = super::super::JoinReorderOptions::default();
        let mut context = Context {
            remaining: MAX_REDUCTIONS,
            options: &options,
        };
        assert!(shared_cte_restrictions([&reference(&scan, 0)?]).is_empty());
        // The keys reach the reference through a projection of its columns, and no join
        // is added, since the plan of the CTE is not there.
        let plan = push(reference(&scan, 0)?, &[0], &restriction, 0.0, &mut context)?;
        // The keys of a second join input reach the other column of the reference.
        let plan = plan.transform_up(|plan| match plan.is::<SharedCteScanExec>() {
            true => push(plan, &[1], &restriction, 0.0, &mut context).map(Transformed::yes),
            false => Ok(Transformed::no(plan)),
        })?;
        let restrictions = shared_cte_restrictions([&plan.data]);
        let [reference] = restrictions[&3].as_slice() else {
            return internal_err!("the CTE has one reference: {restrictions:?}");
        };
        let [first, second] = reference.as_slice() else {
            return internal_err!("the reference has two restrictions: {reference:?}");
        };
        // The restrictions are on the columns of the CTE.
        assert_eq!((&first.columns, &second.columns), (&vec![1], &vec![0]));
        assert!(Arc::ptr_eq(&first.keys, &keys));
        assert_eq!(first.null_equality, NullEquality::NullEqualsNull);
        assert_eq!(context.remaining, MAX_REDUCTIONS);
        assert!(plan.data.is::<ProjectionExec>());
        Ok(())
    }

    /// Returns the scan of a file with the given number of rows and two columns whose
    /// values are in the given range.
    fn file_scan(rows: usize, range: (i64, i64)) -> Arc<dyn ExecutionPlan> {
        use datafusion::common::stats::Precision;
        use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::object_store::ObjectStoreUrl;
        use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let column = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Int64(Some(range.0))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(range.1))),
            null_count: Precision::Exact(0),
            ..ColumnStatistics::new_unknown()
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(rows),
            total_byte_size: Precision::Exact(rows * 16),
            column_statistics: vec![column.clone(), column],
        };
        let source = Arc::new(ParquetSource::new(Arc::clone(&schema)));
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(PartitionedFile::new("file.parquet", 1024))
            .with_statistics(statistics)
            .build();
        DataSourceExec::from_data_source(config)
    }

    /// Returns the keys of a join input that reach a reference of a shared CTE: the
    /// given column of the rows of a file whose values in that column are less than 10.
    fn selected_keys(key: usize) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;

        let scan = file_scan(1000, (0, 999));
        let predicate = Arc::new(BinaryExpr::new(
            column(&scan, key),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
        ));
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);
        Ok(Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: column(&filter, key),
                alias: "__early_join_key_0".to_string(),
            }],
            filter,
        )?))
    }

    /// Returns the plan of a shared CTE that groups the rows of a large file by both
    /// columns, and returns the columns in the other order.
    fn grouped_facts() -> Result<Arc<dyn ExecutionPlan>> {
        let scan = file_scan(1_000_000, (0, 999));
        let group_by = PhysicalGroupBy::new_single(vec![
            (column(&scan, 0), "key".to_string()),
            (column(&scan, 1), "value".to_string()),
        ]);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            Arc::clone(&scan),
            scan.schema(),
        )?);
        Ok(Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: column(&aggregate, 1),
                    alias: "v".to_string(),
                },
                ProjectionExpr {
                    expr: column(&aggregate, 0),
                    alias: "k".to_string(),
                },
            ],
            aggregate,
        )?))
    }

    fn count<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
        let mut count = 0;
        plan.apply(|plan| {
            count += usize::from(plan.is::<T>());
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(count)
    }

    fn restrict_grouped_facts(
        references: Vec<Vec<SharedCteRestriction>>,
        requirements: bool,
    ) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
        use datafusion::physical_plan::Distribution;

        let mut body = grouped_facts()?;
        if requirements {
            let distribution = Distribution::UnspecifiedDistribution;
            body = Arc::new(OutputRequirementExec::new(body, None, distribution, None));
        }
        let restrictions = SharedCteRestrictions::new(references);
        let mut config = ConfigOptions::default();
        config.extensions.insert(restrictions.clone());
        let plan = propagate(body, &config, &super::super::JoinReorderOptions::default())?;
        Ok((plan, restrictions.applied()))
    }

    fn restriction(column: usize, null_equality: NullEquality) -> Result<SharedCteRestriction> {
        Ok(SharedCteRestriction {
            keys: selected_keys(0)?,
            columns: vec![column],
            null_equality,
        })
    }

    #[test]
    fn the_keys_of_all_references_restrict_the_plan_of_a_shared_cte() -> Result<()> {
        let equal = NullEquality::NullEqualsNothing;
        // Both references are restricted on the second column of the CTE, which is the
        // first grouping key, and the second reference also on the other column.
        let references = vec![
            vec![restriction(1, equal)?],
            vec![restriction(0, equal)?, restriction(1, equal)?],
        ];
        for requirements in [false, true] {
            let (plan, applied) = restrict_grouped_facts(references.clone(), requirements)?;
            assert!(applied);
            assert_eq!(plan.is::<OutputRequirementExec>(), requirements);
            // The scan of the file is reduced to the keys of both references, below the
            // aggregate, and the keys are those of the two filtered scans.
            let mut reductions = vec![];
            plan.apply(|plan| {
                if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
                    reductions.push((
                        *join.join_type(),
                        join.on().to_vec(),
                        join.right().is::<DataSourceExec>(),
                        count::<UnionExec>(join.left())?,
                        count::<FilterExec>(join.left())?,
                    ));
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let [(join_type, on, scan, unions, filters)] = reductions.as_slice() else {
                return internal_err!("the plan has one reduction: {reductions:?}");
            };
            assert_eq!(*join_type, JoinType::RightSemi);
            let [(_, target)] = on.as_slice() else {
                return internal_err!("the reduction has one key: {on:?}");
            };
            assert_eq!(target.downcast_ref::<Column>().map(Column::index), Some(0));
            assert_eq!((*scan, *unions, *filters), (true, 1, 2));
            assert_eq!(count::<AggregateExec>(&plan)?, 2);
        }
        Ok(())
    }

    #[test]
    fn a_shared_cte_is_not_restricted_without_common_keys() -> Result<()> {
        let equal = NullEquality::NullEqualsNothing;
        let unchanged = |references| -> Result<()> {
            let (plan, applied) = restrict_grouped_facts(references, true)?;
            assert!(!applied);
            assert_eq!(count::<HashJoinExec>(&plan)?, 0);
            Ok(())
        };
        // The references are restricted on different columns.
        unchanged(vec![
            vec![restriction(1, equal)?],
            vec![restriction(0, equal)?],
        ])?;
        // The keys are compared in different ways.
        unchanged(vec![
            vec![restriction(1, equal)?],
            vec![restriction(1, NullEquality::NullEqualsNull)?],
        ])?;
        // The keys have different types.
        let mut other = restriction(1, equal)?;
        other.keys = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "__early_join_key_0",
            DataType::Int32,
            true,
        )]))));
        unchanged(vec![vec![restriction(1, equal)?], vec![other]])?;
        // The keys are not those of a filtered scan.
        let mut other = restriction(1, equal)?;
        other.keys = Arc::new(EmptyExec::new(other.keys.schema()));
        unchanged(vec![vec![restriction(1, equal)?], vec![other]])?;
        // The CTE has no reference.
        unchanged(vec![])?;
        // The keys are too many to pay for the reduction.
        let mut many = restriction(1, equal)?;
        many.keys = Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: column(&file_scan(1, (0, 0)), 0),
                alias: "__early_join_key_0".to_string(),
            }],
            Arc::new(FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    column(&file_scan(1, (0, 0)), 0),
                    Operator::Lt,
                    column(&file_scan(1, (0, 0)), 1),
                )),
                file_scan(900_000, (0, 999)),
            )?),
        )?);
        unchanged(vec![vec![restriction(1, equal)?], vec![many]])?;
        Ok(())
    }

    /// Returns the restriction of the given column of the CTE to the first column of the
    /// rows of a file whose values in that column, which are in the given range, compare
    /// in the given way with the given value.
    fn compared(
        column_of_cte: usize,
        (rows, range): (usize, (i64, i64)),
        operator: Operator,
        value: i64,
    ) -> Result<SharedCteRestriction> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;

        let scan = file_scan(rows, range);
        let predicate = Arc::new(BinaryExpr::new(
            column(&scan, 0),
            operator,
            Arc::new(Literal::new(ScalarValue::Int64(Some(value)))),
        ));
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);
        let keys = Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: column(&filter, 0),
                alias: "__early_join_key_0".to_string(),
            }],
            filter,
        )?);
        Ok(SharedCteRestriction {
            keys,
            columns: vec![column_of_cte],
            null_equality: NullEquality::NullEqualsNothing,
        })
    }

    /// Returns a reference that is restricted on the second column of the CTE to the
    /// first column of the rows of a file whose values in that column, which are from 0
    /// to 999, compare in the given way with the given value.
    fn reference(rows: usize, operator: Operator, value: i64) -> Result<Vec<SharedCteRestriction>> {
        Ok(vec![compared(1, (rows, (0, 999)), operator, value)?])
    }

    #[test]
    fn the_scan_of_each_key_source_is_limited_on_its_own() -> Result<()> {
        // 60,000 rows: the scans of two references are below the row threshold of a
        // collected join input together. 73,049 rows (the TPC-DS date dimension): each
        // scan is below the threshold, and two or more together are not.
        for (rows, references) in [(60_000, 2), (73_049, 2), (73_049, 5)] {
            let references = (0..references)
                .map(|_| reference(rows, Operator::Lt, 10))
                .collect::<Result<Vec<_>>>()?;
            let sources = references.len();
            let (plan, applied) = restrict_grouped_facts(references, false)?;
            assert!(applied, "{sources} key scans of {rows} rows each");
            assert_eq!(count::<HashJoinExec>(&plan)?, 1);
            // The source of each reference is scanned once, apart from the file of the CTE.
            assert_eq!(count::<DataSourceExec>(&plan)?, sources + 1);
        }
        Ok(())
    }

    #[test]
    fn the_keys_of_all_references_together_are_limited() -> Result<()> {
        // Every row of each source has one of ten keys, so the keys pay for a reduction,
        // and the rows of all the sources are collected together.
        let all = |column: usize| compared(column, (40_000, (0, 9)), Operator::Lt, 10);
        let references = |count: usize| {
            let references = (0..count).map(|_| Ok(vec![all(1)?]));
            references.collect::<Result<Vec<_>>>()
        };
        // 120,000 rows are below the row threshold of a collected join input.
        let (plan, applied) = restrict_grouped_facts(references(3)?, false)?;
        assert!(applied);
        assert_eq!(count::<HashJoinExec>(&plan)?, 1);
        // 160,000 rows are not.
        let (plan, applied) = restrict_grouped_facts(references(4)?, false)?;
        assert!(!applied);
        assert_eq!(count::<HashJoinExec>(&plan)?, 0);
        // The references are restricted on the other column as well, with few keys.
        let both = || {
            Ok(vec![
                all(1)?,
                compared(0, (1000, (0, 999)), Operator::Lt, 10)?,
            ])
        };
        let references = (0..4).map(|_| both()).collect::<Result<Vec<_>>>()?;
        let (plan, applied) = restrict_grouped_facts(references, false)?;
        assert!(applied);
        assert_eq!(count::<HashJoinExec>(&plan)?, 1);
        assert_eq!(count::<DataSourceExec>(&plan)?, 5);
        Ok(())
    }

    #[test]
    fn the_keys_of_all_references_together_pay_for_the_reduction() -> Result<()> {
        // Each reference reads the rows of half of the keys of the CTE, which pays for a
        // reduction to its keys, and the references together read the rows of all keys.
        let low = || reference(1000, Operator::Lt, 500);
        let high = || reference(1000, Operator::GtEq, 500);
        let (plan, applied) = restrict_grouped_facts(vec![low()?, low()?], false)?;
        assert!(applied);
        assert_eq!(count::<HashJoinExec>(&plan)?, 1);
        let (plan, applied) = restrict_grouped_facts(vec![low()?, high()?], false)?;
        assert!(!applied);
        assert_eq!(count::<HashJoinExec>(&plan)?, 0);
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
