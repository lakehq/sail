use std::any::Any;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, Result};
use futures::{StreamExt, TryStreamExt};

/// A physical plan node that enforces a barrier between preconditions and the actual plan.
///
/// This node has `n` children (`n >= 1`), where the first `n - 1` children (possibly none)
/// are preconditions and the last child is the actual plan.
///
/// When `execute(partition, context)` is called, it exhausts partition `p` of each precondition
/// child sequentially before executing and returning partition `p` of the actual plan. This
/// guarantees that the side effects of the preconditions (e.g. catalog commands) are complete
/// before the actual plan starts producing output.
///
/// **Partition matching**: The `EnforceBarrierPartitioning` physical optimizer rule (which
/// runs at the end of the optimizer pipeline) ensures that all preconditions are wrapped with
/// `RepartitionExec` (round-robin) or `CoalescePartitionsExec` to match the partition count
/// of the actual plan. By wrapping the preconditions this way, the actual plan will not start
/// until all partitions of the preconditions are completed, even if we only call `execute()` for
/// one partition of the precondition.
///
/// **Distributed execution note**: In distributed processing, `BarrierExec` does not prevent
/// tasks in dependent stages from being _scheduled_. The barrier is only meaningful within a
/// single stage: the write node and `BarrierExec` belong to the same stage, so the actual write
/// only starts after preconditions for the corresponding partition have been exhausted.
#[derive(Debug, Clone)]
pub struct BarrierExec {
    preconditions: Vec<Arc<dyn ExecutionPlan>>,
    plan: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl BarrierExec {
    pub fn new(preconditions: Vec<Arc<dyn ExecutionPlan>>, plan: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(plan.properties().as_ref().clone());
        Self {
            preconditions,
            plan,
            properties,
        }
    }

    pub fn preconditions(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.preconditions
    }

    pub fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }
}

impl DisplayAs for BarrierExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BarrierExec")
    }
}

impl ExecutionPlan for BarrierExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Never repartition children based on input partitioning heuristics;
        // partition alignment is handled explicitly by `EnforceBarrierPartitioning`.
        vec![false; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.preconditions
            .iter()
            .chain(std::iter::once(&self.plan))
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = children.pop().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{} requires at least 1 child (the actual plan)",
                self.name()
            ))
        })?;
        Ok(Arc::new(Self::new(children, plan)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let num_partitions = self.properties.output_partitioning().partition_count();
        if partition >= num_partitions {
            return exec_err!(
                "{}: partition index {} out of range ({})",
                self.name(),
                partition,
                num_partitions
            );
        }
        // Collect precondition streams to exhaust before running the actual plan.
        let streams: Vec<SendableRecordBatchStream> = self
            .preconditions
            .iter()
            .map(|precondition| precondition.execute(partition, context.clone()))
            .collect::<Result<_>>()?;
        let plan = self.plan.clone();
        let schema = self.schema();
        // Exhaust each precondition stream sequentially, then run the actual plan.
        // We use a once-stream that resolves to the actual plan stream, then flatten.
        let outer = futures::stream::once(async move {
            for mut stream in streams {
                while let Some(batch) = stream.next().await {
                    // Discard the batch; we only care about side effects.
                    batch?;
                }
            }
            plan.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, outer)))
    }
}
