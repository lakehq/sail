//! Reuse dimension-join filters before fact joins and aggregates.
//!
//! This runs after native dynamic-filter pushdown. Equality lineage can expose
//! additional consumers below aggregates and semijoins without executing the
//! dimension scan again or adding another hash join.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::{JoinType, Operator, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use sail_physical_plan::optional_filter::OptionalFilterExpr;

#[derive(Debug, Default)]
pub struct PropagateJoinFilters;

impl PhysicalOptimizerRule for PropagateJoinFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Native producers enforce join type, partition routing and execution-mode
        // restrictions. In particular, cluster mode creates no join producers.
        if !config.optimizer.enable_join_dynamic_filter_pushdown {
            return Ok(plan);
        }
        plan.transform_up(|plan| {
            let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            if !supported_join(join)
                || join.partition_mode() != &PartitionMode::CollectLeft
                || plan.fetch().is_some()
                || volatile_node(plan.as_ref())?
            {
                return Ok(Transformed::no(plan));
            }
            let threshold = config.optimizer.hash_join_single_partition_threshold_rows;
            let Some(source_rows) = filtered_scan_rows(join.left(), false)? else {
                return Ok(Transformed::no(plan));
            };
            if source_rows > threshold {
                return Ok(Transformed::no(plan));
            }
            let Some(filter) = join.dynamic_expressions_produced().into_iter().next() else {
                return Ok(Transformed::no(plan));
            };
            if !filter.is::<DynamicFilterPhysicalExpr>()
                || join.on().is_empty()
                || join
                    .on()
                    .iter()
                    .any(|(left, right)| !left.is::<Column>() || !right.is::<Column>())
            {
                return Ok(Transformed::no(plan));
            }
            let keys = filter
                .children()
                .iter()
                .map(|expr| expr.downcast_ref::<Column>().map(Column::index))
                .collect::<Option<Vec<_>>>();
            let Some(keys) = keys else {
                return Ok(Transformed::no(plan));
            };
            if keys.is_empty() {
                return Ok(Transformed::no(plan));
            }
            // The producer itself is not an early-filtering opportunity: an
            // adjacent consumer would only repeat its membership lookup.
            let right = push(Arc::clone(join.right()), &keys, &filter, false, source_rows)?;
            if Arc::ptr_eq(&right, join.right()) {
                return Ok(Transformed::no(plan));
            }
            let left = Arc::clone(join.left());
            Ok(Transformed::yes(replace_children_if_necessary(
                plan,
                vec![left, right],
            )?))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "PropagateJoinFilters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn push(
    plan: Arc<dyn ExecutionPlan>,
    keys: &[usize],
    filter: &Arc<dyn PhysicalExpr>,
    crossed_boundary: bool,
    source_rows: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.fetch().is_some() || volatile_node(plan.as_ref())? {
        return Ok(plan);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
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
                filter,
                crossed_boundary,
                source_rows,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(existing) = plan.downcast_ref::<FilterExec>() {
        let mapped = keys
            .iter()
            .map(|&i| {
                existing
                    .projection()
                    .as_ref()
                    .map_or(Some(i), |p| p.get(i).copied())
            })
            .collect::<Option<Vec<_>>>();
        if let Some(mapped) = mapped {
            let expected = remap(filter, existing.input(), &mapped)?;
            if same_filter(existing.predicate(), &expected) {
                return Ok(plan);
            }
            let child = push(
                Arc::clone(existing.input()),
                &mapped,
                filter,
                crossed_boundary,
                source_rows,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        if aggregate.group_expr().is_empty()
            || aggregate.group_expr().has_grouping_set()
            || aggregate.limit_options().is_some()
            || aggregate
                .aggr_expr()
                .iter()
                .any(|expr| expr.fun().signature().volatility == Volatility::Volatile)
        {
            return Ok(plan);
        }
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
                filter,
                true,
                source_rows,
            )?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    } else if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if !supported_join(join) {
            return Ok(plan);
        }
        let mapped = join_input_keys(join, keys);
        let mut children = vec![Arc::clone(join.left()), Arc::clone(join.right())];
        for (side, mapped) in mapped.into_iter().enumerate() {
            if let Some(mapped) = mapped {
                children[side] = push(
                    Arc::clone(&children[side]),
                    &mapped,
                    filter,
                    crossed_boundary || is_semi(join),
                    source_rows,
                )?;
            }
        }
        return replace_children_if_necessary(plan, children);
    } else if let Some(input) = transparent_input(&plan) {
        let child = push(
            Arc::clone(input),
            keys,
            filter,
            crossed_boundary,
            source_rows,
        )?;
        return replace_children_if_necessary(plan, vec![child]);
    } else if crossed_boundary {
        return attach(plan, keys, filter, source_rows);
    }
    Ok(plan)
}

fn attach(
    plan: Arc<dyn ExecutionPlan>,
    keys: &[usize],
    filter: &Arc<dyn PhysicalExpr>,
    source_rows: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(scan) = plan.downcast_ref::<DataSourceExec>() else {
        return Ok(plan);
    };
    let Some(file) = scan.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(plan);
    };
    if !file
        .statistics()
        .num_rows
        .get_value()
        .is_some_and(|&rows| rows > source_rows)
    {
        return Ok(plan);
    }
    // Keep the full key tuple and its order. Rewriting the native expression's
    // children preserves the producing join's shared state and expression ID.
    let filter = remap(filter, &plan, keys)?;
    // An exact consumer works with the existing scan configuration. Sampling
    // bypasses membership work when this input has little observed reduction.
    let filter = Arc::new(OptionalFilterExpr::new(filter));
    Ok(Arc::new(FilterExec::try_new(filter, plan)?))
}

fn remap(
    filter: &Arc<dyn PhysicalExpr>,
    plan: &Arc<dyn ExecutionPlan>,
    keys: &[usize],
) -> Result<Arc<dyn PhysicalExpr>> {
    let schema = plan.schema();
    Arc::clone(filter).with_new_children(
        keys.iter()
            .map(|&index| {
                Arc::new(Column::new(schema.field(index).name(), index)) as Arc<dyn PhysicalExpr>
            })
            .collect(),
    )
}

fn same_filter(left: &Arc<dyn PhysicalExpr>, right: &Arc<dyn PhysicalExpr>) -> bool {
    let left = left
        .downcast_ref::<OptionalFilterExpr>()
        .map_or(left, OptionalFilterExpr::predicate);
    left.expression_id() == right.expression_id() && left.children() == right.children()
}

fn supported_join(join: &HashJoinExec) -> bool {
    matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    )
}

fn is_semi(join: &HashJoinExec) -> bool {
    matches!(join.join_type(), JoinType::LeftSemi | JoinType::RightSemi)
}

fn join_input_keys(join: &HashJoinExec, keys: &[usize]) -> [Option<Vec<usize>>; 2] {
    let left_len = join.left().schema().fields().len();
    let output = keys
        .iter()
        .map(|&i| {
            let i = join
                .projection
                .as_ref()
                .map_or(Some(i), |p| p.get(i).copied())?;
            Some(match join.join_type() {
                JoinType::LeftSemi => (0, i),
                JoinType::RightSemi => (1, i),
                _ if i < left_len => (0, i),
                _ => (1, i - left_len),
            })
        })
        .collect::<Option<Vec<_>>>();
    [0, 1].map(|side| {
        output
            .as_ref()?
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

fn transparent_input(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if let Some(exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
        Some(exec.input())
    } else if let Some(exec) = plan.downcast_ref::<RepartitionExec>() {
        Some(exec.input())
    } else {
        plan.downcast_ref::<CooperativeExec>()
            .map(CooperativeExec::input)
    }
}

fn filtered_scan_rows(plan: &Arc<dyn ExecutionPlan>, filtered: bool) -> Result<Option<usize>> {
    if plan.fetch().is_some() || volatile_node(plan.as_ref())? {
        return Ok(None);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        if projection
            .expr()
            .iter()
            .all(|expr| expr.expr.is::<Column>())
        {
            return filtered_scan_rows(projection.input(), filtered);
        }
        return Ok(None);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return filtered_scan_rows(filter.input(), filtered || selective(filter.predicate())?);
    }
    if let Some(input) = transparent_input(plan) {
        return filtered_scan_rows(input, filtered);
    }
    let Some(scan) = plan.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(file) = scan.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(None);
    };
    let filtered = filtered
        || file
            .file_source()
            .filter()
            .map(|expr| selective(&expr))
            .transpose()?
            .unwrap_or(false);
    Ok(filtered
        .then(|| file.statistics().num_rows.get_value().copied())
        .flatten())
}

fn selective(expr: &Arc<dyn PhysicalExpr>) -> Result<bool> {
    expr.exists(|expr| {
        Ok(expr.is::<InListExpr>()
            || expr.downcast_ref::<BinaryExpr>().is_some_and(|binary| {
                matches!(
                    binary.op(),
                    Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                )
            }))
    })
}

fn volatile_node(plan: &dyn ExecutionPlan) -> Result<bool> {
    let mut volatile = false;
    plan.apply_expressions(&mut |expr| {
        volatile |= expr.exists(|expr| Ok(expr.is_volatile_node()))?;
        Ok(if volatile {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(volatile)
}
