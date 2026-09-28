use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};

/// Prefer an aggregated filtering input when statistics cannot distinguish semi-join
/// build costs. Without distinct counts, an aggregate can inherit its input row
/// estimate, leaving a large fact input on the build side of a left semi join.
/// HashJoinExec concatenates that input, potentially overflowing Utf8 offsets.
///
/// Run after JoinSelection, but before dynamic filters and distribution enforcement.
/// This is a fallback for partitioned joins, not evidence that the aggregate is small
/// enough to broadcast. It is independent of multi-table inner join enumeration.
#[derive(Debug)]
pub struct SelectSemiJoinBuildSide;

impl PhysicalOptimizerRule for SelectSemiJoinBuildSide {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.join_reordering {
            return Ok(plan);
        }
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if join.join_type() != &JoinType::LeftSemi
                || join.partition_mode() != &PartitionMode::Partitioned
                || join.null_aware
            {
                return Ok(Transformed::no(node));
            }
            let keys = join
                .on()
                .iter()
                .map(|(_, right)| right.downcast_ref::<Column>().map(Column::index))
                .collect::<Option<Vec<_>>>();
            if !keys.is_some_and(|keys| grouped_on_keys(join.right(), &keys)) {
                return Ok(Transformed::no(node));
            }

            let statistics = StatisticsContext::new();
            let left = statistics.compute(join.left().as_ref(), &StatisticsArgs::new())?;
            let right = statistics.compute(join.right().as_ref(), &StatisticsArgs::new())?;
            // Match JoinSelection's byte-first comparison. Unequal byte estimates
            // already informed its decision; otherwise retain a smaller left input.
            let inconclusive = match (
                left.total_byte_size.get_value(),
                right.total_byte_size.get_value(),
            ) {
                (Some(left), Some(right)) => left == right,
                _ => match (left.num_rows.get_value(), right.num_rows.get_value()) {
                    (Some(left), Some(right)) => left == right,
                    _ => true,
                },
            };
            if !inconclusive {
                return Ok(Transformed::no(node));
            }

            Ok(Transformed::yes(
                join.swap_inputs(PartitionMode::Partitioned)?,
            ))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "SelectSemiJoinBuildSide"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Follow only direct column lineage to a completed ordinary aggregate whose
/// grouping columns are exactly the join keys. Windows append columns: a join on
/// a window result must not be mistaken for a join on an input grouping column.
fn grouped_on_keys(plan: &Arc<dyn ExecutionPlan>, keys: &[usize]) -> bool {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        let grouping = aggregate.group_expr();
        return matches!(
            aggregate.mode(),
            AggregateMode::Final
                | AggregateMode::FinalPartitioned
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned
        ) && grouping.is_single()
            && !grouping.expr().is_empty()
            && keys.iter().all(|&key| key < grouping.expr().len())
            && (0..grouping.expr().len()).all(|key| keys.contains(&key));
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let mapped = keys
            .iter()
            .map(|&key| {
                projection
                    .expr()
                    .get(key)?
                    .expr
                    .downcast_ref::<Column>()
                    .map(Column::index)
            })
            .collect::<Option<Vec<_>>>();
        return mapped.is_some_and(|keys| grouped_on_keys(projection.input(), &keys));
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return match filter.projection() {
            Some(projection) => {
                let mapped = keys
                    .iter()
                    .map(|&key| projection.get(key).copied())
                    .collect::<Option<Vec<_>>>();
                mapped.is_some_and(|keys| grouped_on_keys(filter.input(), &keys))
            }
            None => grouped_on_keys(filter.input(), keys),
        };
    }
    let input = if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        window.input()
    } else if let Some(window) = plan.downcast_ref::<WindowAggExec>() {
        window.input()
    } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
        sort.input()
    } else if let Some(sort) = plan.downcast_ref::<SortPreservingMergeExec>() {
        sort.input()
    } else {
        return false;
    };
    keys.iter().all(|&key| key < input.schema().fields().len()) && grouped_on_keys(input, keys)
}
