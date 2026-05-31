use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{
    with_new_children_if_necessary, ExecutionPlan, ExecutionPlanProperties,
};

/// Safety-net rule that ensures the build side (left child) of every
/// `HashJoinExec` in `CollectLeft` mode has exactly one output partition.
///
/// DataFusion's `EnforceDistribution` rule normally takes care of this,
/// but after join reordering or other plan transformations the invariant
/// can be violated. This rule wraps the left child in a
/// `CoalescePartitionsExec` when needed, and runs late in the optimizer
/// pipeline, before `EnforceBarrierPartitioning` and `SanityCheckPlan`.
#[derive(Debug, Default)]
pub struct RewriteCollectLeftHashJoin;

impl RewriteCollectLeftHashJoin {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RewriteCollectLeftHashJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            let Some(join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            if join.mode != PartitionMode::CollectLeft {
                return Ok(Transformed::no(node));
            }

            let left = join.left.clone();
            if left.output_partitioning().partition_count() == 1 {
                return Ok(Transformed::no(node));
            }

            // Wrap in CoalescePartitionsExec to merge into a single partition.
            let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(left));
            let new_children = vec![coalesced, join.right.clone()];
            let new_node = with_new_children_if_necessary(node, new_children)?;
            Ok(Transformed::yes(new_node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteCollectLeftHashJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    /// Build a plan with multiple partitions by unioning two EmptyExec.
    fn multi_partition_plan(s: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        let a = Arc::new(EmptyExec::new(s.clone())) as Arc<dyn ExecutionPlan>;
        let b = Arc::new(EmptyExec::new(s.clone())) as Arc<dyn ExecutionPlan>;
        UnionExec::try_new(vec![a, b]).unwrap()
    }

    #[test]
    fn test_adds_coalesce_when_left_has_multiple_partitions() {
        let s = schema();
        let left = multi_partition_plan(&s);
        let right = Arc::new(EmptyExec::new(s.clone())) as Arc<dyn ExecutionPlan>;

        assert!(left.output_partitioning().partition_count() > 1);

        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(
                    Arc::new(Column::new("id", 0)),
                    Arc::new(Column::new("id", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false, // null_aware
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let rule = RewriteCollectLeftHashJoin::new();
        let config = ConfigOptions::default();
        let result = rule.optimize(join, &config).unwrap();

        let new_join = result.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(
            new_join.children()[0]
                .output_partitioning()
                .partition_count(),
            1
        );
        assert!(new_join.children()[0]
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some());
    }

    #[test]
    fn test_no_change_when_left_has_single_partition() {
        let s = schema();
        let left = Arc::new(EmptyExec::new(s.clone())) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(s.clone())) as Arc<dyn ExecutionPlan>;

        assert_eq!(left.output_partitioning().partition_count(), 1);

        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(
                    Arc::new(Column::new("id", 0)),
                    Arc::new(Column::new("id", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false, // null_aware
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let rule = RewriteCollectLeftHashJoin::new();
        let config = ConfigOptions::default();
        let result = rule.optimize(join, &config).unwrap();

        let new_join = result.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(new_join.children()[0]
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_none());
    }

    #[test]
    fn test_no_change_for_partitioned_mode() {
        let s = schema();
        let left = multi_partition_plan(&s);
        let right = multi_partition_plan(&s);

        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(
                    Arc::new(Column::new("id", 0)),
                    Arc::new(Column::new("id", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false, // null_aware
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let rule = RewriteCollectLeftHashJoin::new();
        let config = ConfigOptions::default();
        let result = rule.optimize(join, &config).unwrap();

        let new_join = result.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(new_join.children()[0]
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_none());
    }
}
