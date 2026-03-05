use std::any::Any;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{exec_err, internal_err, Result};
use futures::{StreamExt, TryStreamExt};

/// A physical plan node that enforces a barrier between preconditions and a main plan.
///
/// This node has `n` children (`n >= 1`), where the first `n - 1` children (possibly none)
/// are preconditions and the last child is the actual plan.
///
/// When `execute(partition, context)` is called, it exhausts partition `p` of each precondition
/// child sequentially before executing and returning partition `p` of the actual plan. This
/// guarantees that the side effects of the preconditions (e.g. catalog commands) are complete
/// before the actual plan starts producing output.
///
/// **Partition matching**: All children must have the same number of output partitions.
/// The `EnforceBarrierPartitioning` physical optimizer rule (which runs at the end of the
/// optimizer pipeline) ensures that any precondition whose partition count differs from the
/// actual plan is wrapped with a `RepartitionExec` (round-robin) or `CoalescePartitionsExec`
/// to bring all children to a common partition count.
///
/// **Distributed execution note**: In distributed processing, `BarrierExec` does not prevent
/// tasks in dependent stages from being _scheduled_. The barrier is only meaningful within a
/// single stage: the write node and `BarrierExec` belong to the same stage, so the actual write
/// only starts after preconditions for the corresponding partition have been exhausted.
#[derive(Debug, Clone)]
pub struct BarrierExec {
    /// All children: first `n - 1` are preconditions, last is the actual plan.
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: PlanProperties,
}

impl BarrierExec {
    pub fn new(preconditions: Vec<Arc<dyn ExecutionPlan>>, plan: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            plan.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        let mut children = preconditions;
        children.push(plan);
        Self {
            children,
            properties,
        }
    }

    pub fn preconditions(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.children[..self.children.len() - 1]
    }

    pub fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        self.children
            .last()
            .unwrap_or_else(|| unreachable!("BarrierExec must have at least one child"))
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Never repartition children based on input partitioning heuristics;
        // partition alignment is handled explicitly by `EnforceBarrierPartitioning`.
        vec![false; self.children.len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            return internal_err!("BarrierExec requires at least 1 child (the actual plan)");
        }
        let plan = children
            .pop()
            .unwrap_or_else(|| unreachable!("children is non-empty"));
        let preconditions = children;
        Ok(Arc::new(Self::new(preconditions, plan)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let num_partitions = self.properties.output_partitioning().partition_count();
        if partition >= num_partitions {
            return exec_err!(
                "BarrierExec: partition index {} out of range ({})",
                partition,
                num_partitions
            );
        }
        // Collect precondition streams to exhaust before running the actual plan.
        let precondition_streams: Vec<SendableRecordBatchStream> = self
            .preconditions()
            .iter()
            .map(|pre| pre.execute(partition, context.clone()))
            .collect::<Result<_>>()?;
        let main_plan = self.plan().clone();
        let schema = self.schema();
        // Exhaust each precondition stream sequentially, then run the actual plan.
        // We use a once-stream that resolves to the actual plan stream, then flatten.
        let outer = futures::stream::once(async move {
            for mut stream in precondition_streams {
                while let Some(batch) = stream.next().await {
                    // Discard the batch; we only care about side effects.
                    batch?;
                }
            }
            main_plan.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, outer)))
    }
}
