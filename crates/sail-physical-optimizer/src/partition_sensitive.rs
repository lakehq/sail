use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, replace_children_if_necessary,
};
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::monotonic_id::MonotonicIdExec;
use sail_physical_plan::seeded_random::SeededRandomExec;
use sail_physical_plan::spark_partition_id::SparkPartitionIdExec;

/// Keeps the input partitions of the operators whose output depends on the partition of each
/// row and on the order of the rows in the partition (monotonically increasing IDs, partition
/// IDs and random functions with a seed), as Spark computes them over the partitions of their
/// input.
///
/// `EnsureRequirements` adds round-robin repartitions below the filters and projections of
/// their input, which change the partitions and make the order of the rows in each partition
/// nondeterministic. These repartitions are removed down to the operator that defines the
/// partitions, also below the sorts within partitions that sort pushdown moves below these
/// operators. A round-robin repartition to the same number of partitions is added above the
/// operator instead, so that the operators above keep their parallelism, unless it would lose
/// the order of the rows that the operators above rely on. It is merged into a round-robin
/// repartition that directly follows it.
#[derive(Debug, Default)]
pub struct KeepPartitionSensitiveInputs;

impl PhysicalOptimizerRule for KeepPartitionSensitiveInputs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(input) = round_robin_input(&plan)
                && let Some(inner) = round_robin_input(input)
                && is_partition_sensitive(inner)
            {
                // The round-robin repartition added above the operator is merged into the
                // round-robin repartition that follows it.
                let input = Arc::clone(inner);
                return Ok(Transformed::yes(replace_children_if_necessary(
                    plan,
                    vec![input],
                )?));
            }
            if !is_partition_sensitive(&plan) {
                return Ok(Transformed::no(plan));
            }
            let children = plan.children();
            let [child] = children.as_slice() else {
                return Ok(Transformed::no(plan));
            };
            let child = remove_round_robin(Arc::clone(child))?;
            if !child.transformed {
                return Ok(Transformed::no(plan));
            }
            let partitions = plan.output_partitioning().partition_count();
            let ordering = plan.output_ordering().cloned();
            let plan = replace_children_if_necessary(Arc::clone(&plan), vec![child.data])?;
            if plan.output_partitioning().partition_count() == partitions {
                return Ok(Transformed::yes(plan));
            }
            let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
                Arc::clone(&plan),
                Partitioning::RoundRobinBatch(partitions),
            )?);
            if ordering.is_some() && repartition.output_ordering() != ordering.as_ref() {
                return Ok(Transformed::yes(plan));
            }
            Ok(Transformed::yes(repartition))
        })
        .data()
    }

    fn name(&self) -> &str {
        "KeepPartitionSensitiveInputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn is_partition_sensitive(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<SeededRandomExec>()
        || plan.is::<MonotonicIdExec>()
        || plan.is::<SparkPartitionIdExec>()
}

/// Returns the input of a round-robin repartition that does not preserve the order of rows.
fn round_robin_input(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    let repartition = plan.downcast_ref::<RepartitionExec>()?;
    (matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
        && !repartition.preserve_order())
    .then(|| repartition.input())
}

/// Removes the round-robin repartitions through the operators that keep each row in its
/// partition, such as the operator that applies a Python function to each partition.
///
/// This includes the probe side of a hash join whose build side is collected, when the join
/// keeps the order of the probe rows, as the streamed side of a broadcast hash join in Spark.
/// The partitions and the order of the rows are execution-dependent for other joins, such as
/// partitioned hash joins and nested loop joins, so their inputs are kept.
fn remove_round_robin(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(input) = round_robin_input(&plan) {
        let input = Arc::clone(input);
        return Ok(Transformed::yes(remove_round_robin(input)?.data));
    }
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && *join.partition_mode() == PartitionMode::CollectLeft
        && plan.maintains_input_order()[1]
    {
        let right = remove_round_robin(Arc::clone(join.right()))?;
        if !right.transformed {
            return Ok(Transformed::no(plan));
        }
        let left = Arc::clone(join.left());
        return Ok(Transformed::yes(replace_children_if_necessary(
            plan,
            vec![left, right.data],
        )?));
    }
    if !(plan.is::<FilterExec>()
        || plan.is::<ProjectionExec>()
        || plan.is::<CooperativeExec>()
        || plan.is::<UnnestExec>()
        || plan.is::<MapPartitionsExec>()
        || plan
            .downcast_ref::<SortExec>()
            .is_some_and(SortExec::preserve_partitioning)
        || is_partition_sensitive(&plan))
    {
        return Ok(Transformed::no(plan));
    }
    let children = plan.children();
    let [child] = children.as_slice() else {
        return Ok(Transformed::no(plan));
    };
    let child = remove_round_robin(Arc::clone(child))?;
    if !child.transformed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(replace_children_if_necessary(
        Arc::clone(&plan),
        vec![child.data],
    )?))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality, internal_datafusion_err};
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::union::UnionExec;

    use super::*;

    fn source(partitions: usize) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let inputs = (0..partitions)
            .map(|_| {
                Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>
            })
            .collect::<Vec<_>>();
        UnionExec::try_new(inputs)
    }

    fn round_robin(
        input: Arc<dyn ExecutionPlan>,
        partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::RoundRobinBatch(partitions),
        )?))
    }

    fn filter(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            col("a", &input.schema())?,
            datafusion::logical_expr::Operator::GtEq,
            lit(0_i64),
        ));
        Ok(Arc::new(FilterExec::try_new(predicate, input)?))
    }

    fn monotonic_id(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("id", DataType::Int64, false)));
        Ok(Arc::new(MonotonicIdExec::try_new(
            input,
            "id".to_string(),
            Arc::new(Schema::new(fields)),
        )?))
    }

    fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        mode: PartitionMode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let on = vec![(col("a", &left.schema())?, col("a", &right.schema())?)];
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            None,
            mode,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let plan = KeepPartitionSensitiveInputs.optimize(plan, &ConfigOptions::default())?;
        Ok(displayable(plan.as_ref()).indent(false).to_string())
    }

    #[test]
    fn round_robin_repartitions_move_into_the_repartition_above_partition_sensitive_operators()
    -> Result<()> {
        let plan = monotonic_id(filter(round_robin(source(2)?, 4)?)?)?;
        let plan = optimize(round_robin(plan, 8)?)?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(
            lines[0].starts_with(
                "RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=2"
            ),
            "{plan}"
        );
        assert!(lines[1].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[2].starts_with("FilterExec"), "{plan}");
        assert!(lines[3].starts_with("UnionExec"), "{plan}");
        assert_eq!(plan.matches("RepartitionExec").count(), 1, "{plan}");
        Ok(())
    }

    #[test]
    fn stacked_partition_sensitive_operators_share_the_input_partitions() -> Result<()> {
        let plan = monotonic_id(filter(round_robin(source(1)?, 4)?)?)?;
        let plan = optimize(monotonic_id(filter(plan)?)?)?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(
            lines[0].starts_with(
                "RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1"
            ),
            "{plan}"
        );
        assert!(lines[1].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[2].starts_with("FilterExec"), "{plan}");
        assert!(lines[3].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[4].starts_with("FilterExec"), "{plan}");
        assert!(lines[5].starts_with("PlaceholderRowExec"), "{plan}");
        Ok(())
    }

    #[test]
    fn round_robin_repartitions_between_partition_sensitive_operators_are_moved() -> Result<()> {
        let plan = monotonic_id(filter(source(2)?)?)?;
        let plan = optimize(monotonic_id(filter(round_robin(plan, 10)?)?)?)?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(
            lines[0].starts_with(
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2"
            ),
            "{plan}"
        );
        assert!(lines[1].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[2].starts_with("FilterExec"), "{plan}");
        assert!(lines[3].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[4].starts_with("FilterExec"), "{plan}");
        assert!(lines[5].starts_with("UnionExec"), "{plan}");
        Ok(())
    }

    #[test]
    fn round_robin_repartitions_below_sorts_within_partitions_are_removed() -> Result<()> {
        let input = filter(round_robin(source(3)?, 10)?)?;
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "a",
            &input.schema(),
        )?)])
        .ok_or_else(|| internal_datafusion_err!("empty ordering"))?;
        let sort = Arc::new(SortExec::new(order.clone(), input).with_preserve_partitioning(true));
        let plan = monotonic_id(sort)?;
        let plan = optimize(Arc::new(SortPreservingMergeExec::new(order, plan)))?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(lines[0].starts_with("SortPreservingMergeExec"), "{plan}");
        assert!(lines[1].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[2].starts_with("SortExec"), "{plan}");
        assert!(lines[3].starts_with("FilterExec"), "{plan}");
        assert!(lines[4].starts_with("UnionExec"), "{plan}");
        assert!(!plan.contains("RoundRobinBatch"), "{plan}");
        Ok(())
    }

    #[test]
    fn hash_repartitions_are_kept() -> Result<()> {
        let input = source(2)?;
        let key = col("a", &input.schema())?;
        let hash = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![key], 4),
        )?);
        let plan = optimize(monotonic_id(filter(hash)?)?)?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(lines[0].starts_with("MonotonicIdExec"), "{plan}");
        assert!(
            lines[2].starts_with("RepartitionExec: partitioning=Hash"),
            "{plan}"
        );
        Ok(())
    }

    #[test]
    fn round_robin_repartitions_below_the_probe_side_of_collect_left_joins_are_removed()
    -> Result<()> {
        for join_type in [
            JoinType::Inner,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::RightMark,
        ] {
            let probe = filter(round_robin(source(2)?, 4)?)?;
            let join = hash_join(source(1)?, probe, join_type, PartitionMode::CollectLeft)?;
            let plan = optimize(monotonic_id(join)?)?;
            let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
            assert!(
                lines[0].starts_with(
                    "RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2"
                ),
                "{plan}"
            );
            assert!(lines[1].starts_with("MonotonicIdExec"), "{plan}");
            assert!(
                lines[2].starts_with("HashJoinExec: mode=CollectLeft"),
                "{plan}"
            );
            assert!(lines[3].starts_with("PlaceholderRowExec"), "{plan}");
            assert!(lines[4].starts_with("FilterExec"), "{plan}");
            assert!(lines[5].starts_with("UnionExec"), "{plan}");
            assert_eq!(plan.matches("RepartitionExec").count(), 1, "{plan}");
        }
        Ok(())
    }

    #[test]
    fn round_robin_repartitions_below_joins_that_do_not_keep_the_probe_order_are_kept() -> Result<()>
    {
        for (join_type, mode) in [
            (JoinType::Left, PartitionMode::CollectLeft),
            (JoinType::Full, PartitionMode::CollectLeft),
            (JoinType::Inner, PartitionMode::Partitioned),
        ] {
            let build = round_robin(source(2)?, 4)?;
            let probe = filter(round_robin(source(2)?, 4)?)?;
            let join = hash_join(build, probe, join_type, mode)?;
            let plan = optimize(monotonic_id(join)?)?;
            let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
            assert!(lines[0].starts_with("MonotonicIdExec"), "{plan}");
            assert!(lines[1].starts_with("HashJoinExec"), "{plan}");
            assert_eq!(plan.matches("RoundRobinBatch(4)").count(), 2, "{plan}");
        }
        Ok(())
    }

    #[test]
    fn round_robin_repartitions_below_the_build_side_of_collect_left_joins_are_kept() -> Result<()>
    {
        let build = round_robin(source(2)?, 4)?;
        let join = hash_join(
            build,
            source(2)?,
            JoinType::Inner,
            PartitionMode::CollectLeft,
        )?;
        let plan = optimize(monotonic_id(join)?)?;
        let lines = plan.lines().map(str::trim_start).collect::<Vec<_>>();
        assert!(lines[0].starts_with("MonotonicIdExec"), "{plan}");
        assert!(lines[1].starts_with("HashJoinExec"), "{plan}");
        assert_eq!(plan.matches("RoundRobinBatch(4)").count(), 1, "{plan}");
        Ok(())
    }
}
