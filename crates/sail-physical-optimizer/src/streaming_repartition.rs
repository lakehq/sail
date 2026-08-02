use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_physical_expr::Partitioning;
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

/// Removes round-robin repartitioning from streaming plans.
///
/// Round-robin repartitioning exists to buy parallelism, and for an unbounded
/// input it costs unbounded latency instead. [`RepartitionExec`] coalesces on
/// the producer side, holding rows until `batch_size` of them accumulate and
/// flushing the remainder only when the input ends. A stream does not end, so
/// at the default batch size a slow source delivers nothing downstream for a
/// very long time, while the query reports itself as running.
///
/// Streaming plans are not meant to be repartitioned in any case: the streaming
/// rewriter rejects a logical `Repartition` outright. This removes the ones the
/// physical planner adds on its own. A streaming source that is itself
/// multi-partition keeps its partitions, so real parallelism is unaffected.
///
/// A repartition is recognised as belonging to a streaming plan by its input
/// schema: every node above a streaming source carries the flow event fields.
#[derive(Debug)]
pub struct RemoveStreamingRoundRobinRepartition {}

impl RemoveStreamingRoundRobinRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RemoveStreamingRoundRobinRepartition {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for RemoveStreamingRoundRobinRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|plan| {
            if let Some(node) = plan.downcast_ref::<RepartitionExec>()
                && matches!(
                    node.properties().output_partitioning(),
                    Partitioning::RoundRobinBatch(_)
                )
                && is_flow_event_schema(node.input().schema().as_ref())
            {
                Ok(Transformed::yes(Arc::clone(node.input())))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "RemoveStreamingRoundRobinRepartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;

    use super::*;

    fn data_schema() -> Schema {
        Schema::new(vec![Field::new("value", DataType::Int64, false)])
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        #[expect(clippy::unwrap_used)]
        RemoveStreamingRoundRobinRepartition::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_removes_round_robin_over_a_flow_event_input() {
        let input = Arc::new(EmptyExec::new(Arc::new(to_flow_event_schema(
            &data_schema(),
        ))));
        let plan =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap());

        let optimized = optimize(plan);
        assert!(optimized.downcast_ref::<RepartitionExec>().is_none());
        assert!(optimized.downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_keeps_round_robin_over_a_batch_input() {
        let input = Arc::new(EmptyExec::new(Arc::new(data_schema())));
        let plan =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap());

        let optimized = optimize(plan);
        assert!(optimized.downcast_ref::<RepartitionExec>().is_some());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_keeps_hash_repartition_over_a_flow_event_input() {
        // Only the round-robin variant is removed: a hash repartition changes
        // semantics rather than merely adding parallelism, so if one ever
        // appears in a streaming plan it must not be silently dropped.
        let input = Arc::new(EmptyExec::new(Arc::new(to_flow_event_schema(
            &data_schema(),
        ))));
        let plan =
            Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![], 4)).unwrap());

        let optimized = optimize(plan);
        assert!(optimized.downcast_ref::<RepartitionExec>().is_some());
    }
}
