use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_physical_expr::Partitioning;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

pub struct RewriteExplicitRepartition {}

/// Rewrites explicit repartition nodes to executable physical operators.
impl RewriteExplicitRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RewriteExplicitRepartition {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for RewriteExplicitRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|plan| {
            if let Some(node) = plan.downcast_ref::<ExplicitRepartitionExec>() {
                let partitioning = node.properties().output_partitioning().clone();
                let input = node.input().clone();
                let input_partition_count = input.output_partitioning().partition_count();
                match partitioning {
                    Partitioning::RoundRobinBatch(_) => Ok(Transformed::no(plan)),
                    Partitioning::Hash(_, _) => Ok(Transformed::yes(Arc::new(
                        RepartitionExec::try_new(input, partitioning)?,
                    ))),
                    Partitioning::UnknownPartitioning(n) if n >= input_partition_count => {
                        Ok(Transformed::yes(input))
                    }
                    Partitioning::UnknownPartitioning(1) => Ok(Transformed::yes(Arc::new(
                        CoalescePartitionsExec::new(input),
                    ))),
                    Partitioning::UnknownPartitioning(n) => {
                        Ok(Transformed::yes(Arc::new(CoalesceExec::new(input, n))))
                    }
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RewriteExplicitRepartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for RewriteExplicitRepartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion_physical_expr::Partitioning;
    use sail_physical_plan::coalesce::CoalesceExec;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;

    use super::RewriteExplicitRepartition;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn multi_partition_plan(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        let left = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        UnionExec::try_new(vec![left, right]).unwrap()
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        RewriteExplicitRepartition::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    fn test_rewrites_unknown_partitioning_to_coalesce_partitions_exec() {
        let input = multi_partition_plan(&schema());
        let plan = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::UnknownPartitioning(1),
        )) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan);

        assert!(optimized.is::<CoalescePartitionsExec>());
        assert_eq!(optimized.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_rewrites_unknown_partitioning_reduction_to_coalesce_exec() {
        let input = UnionExec::try_new(vec![
            Arc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>,
            Arc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>,
            Arc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>,
        ])
        .unwrap();
        let plan = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::UnknownPartitioning(2),
        )) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan);

        let coalesce = optimized.downcast_ref::<CoalesceExec>().unwrap();
        assert_eq!(coalesce.output_partitions(), 2);
        assert_eq!(optimized.output_partitioning().partition_count(), 2);
    }

    #[test]
    fn test_rewrites_unknown_partitioning_increase_to_input() {
        let input = multi_partition_plan(&schema());
        let plan = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::UnknownPartitioning(4),
        )) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan);

        assert!(optimized.is::<UnionExec>());
        assert!(!optimized.is::<CoalesceExec>());
        assert_eq!(optimized.output_partitioning().partition_count(), 2);
    }

    #[test]
    fn test_keeps_round_robin_partitioning_on_explicit_exec() {
        let input = multi_partition_plan(&schema());
        let plan = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::RoundRobinBatch(4),
        )) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan);

        assert!(
            optimized
                .downcast_ref::<ExplicitRepartitionExec>()
                .is_some()
        );
        assert_eq!(optimized.output_partitioning().partition_count(), 4);
    }
}
