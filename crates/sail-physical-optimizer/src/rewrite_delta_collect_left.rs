use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use sail_physical_plan::{contains_format_tag, FormatTag};

/// Rewrite `HashJoinExec(mode=CollectLeft)` into a distributed-safe form for join types that
/// require global coordination across probe partitions.
pub struct RewriteDeltaCollectLeft {}

impl RewriteDeltaCollectLeft {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RewriteDeltaCollectLeft {
    fn default() -> Self {
        Self::new()
    }
}

fn contains_delta_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    contains_format_tag(plan, FormatTag::Delta)
}

fn collect_left_requires_global_build_state(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark
            | JoinType::Full
    )
}

impl PhysicalOptimizerRule for RewriteDeltaCollectLeft {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !contains_delta_plan(&plan)? {
            return Ok(plan);
        }

        let result = plan.transform_up(|plan| {
            let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            if join.mode != PartitionMode::CollectLeft {
                return Ok(Transformed::no(plan));
            }
            if !collect_left_requires_global_build_state(join.join_type) {
                return Ok(Transformed::no(plan));
            }

            // Convert to a partitioned hash join with explicit repartitions on both sides, so each
            // output partition can be executed independently in the distributed engine.
            let partition_count = join.right.output_partitioning().partition_count();

            let (left_exprs, right_exprs): (Vec<_>, Vec<_>) = join
                .on
                .iter()
                .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
                .unzip();

            let left = Arc::new(RepartitionExec::try_new(
                Arc::clone(&join.left),
                Partitioning::Hash(left_exprs, partition_count),
            )?);
            let right = Arc::new(RepartitionExec::try_new(
                Arc::clone(&join.right),
                Partitioning::Hash(right_exprs, partition_count),
            )?);

            let rewritten = Arc::new(HashJoinExec::try_new(
                left,
                right,
                join.on.clone(),
                join.filter.clone(),
                &join.join_type,
                join.projection.clone(),
                PartitionMode::Partitioned,
                join.null_equality,
            )?);

            Ok(Transformed::yes(rewritten))
        })?;

        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteDeltaCollectLeft"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for RewriteDeltaCollectLeft {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
