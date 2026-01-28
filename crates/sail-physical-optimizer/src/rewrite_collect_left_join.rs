use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use sail_physical_plan::distributed_collect_left_join::DistributedCollectLeftJoinExec;

#[derive(Debug)]
pub struct RewriteCollectLeftJoinForDistributed;

impl RewriteCollectLeftJoinForDistributed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RewriteCollectLeftJoinForDistributed {
    fn default() -> Self {
        Self::new()
    }
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

impl PhysicalOptimizerRule for RewriteCollectLeftJoinForDistributed {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
            let rewritten = Arc::new(DistributedCollectLeftJoinExec::try_new(
                Arc::clone(&join.left),
                Arc::clone(&join.right),
                join.on.clone(),
                join.filter.clone(),
                join.join_type,
                join.projection.clone(),
                join.null_equality,
            )?);
            Ok(Transformed::yes(rewritten))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteCollectLeftJoinForDistributed"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
