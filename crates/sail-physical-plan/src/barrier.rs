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
/// This node has `n` children (`n >= 1`), where the first `n - 1` children are preconditions
/// and the last child is the main (actual) plan. All children are expected to have the same
/// number of output partitions.
///
/// When `execute(partition, context)` is called, it exhausts partition `p` of each precondition
/// child sequentially before executing and returning partition `p` of the main plan. This
/// guarantees that the side effects of the preconditions (e.g. catalog commands) are complete
/// before the main plan starts producing output.
///
/// **Partition matching**: All children must have the same number of partitions. When
/// `CatalogCommandExec` (which produces a single partition) is a precondition and the main plan
/// has more partitions, the physical optimizer will inject a `RepartitionExec` on top of the
/// `CatalogCommandExec` so that the partition counts match.
///
/// **Distributed execution note**: In distributed processing, `BarrierExec` does not prevent
/// tasks in dependent stages from being _scheduled_. The barrier is only meaningful within a
/// single stage: the write node and `BarrierExec` belong to the same stage, so the actual write
/// only starts after preconditions for the corresponding partition have been exhausted.
#[derive(Debug, Clone)]
pub struct BarrierExec {
    /// All children: first `n - 1` are preconditions, last is the main plan.
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: PlanProperties,
}

impl BarrierExec {
    pub fn try_new(
        preconditions: Vec<Arc<dyn ExecutionPlan>>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        if preconditions.is_empty() {
            return internal_err!("BarrierExec requires at least one precondition");
        }
        let num_partitions = plan.output_partitioning().partition_count();
        for (i, pre) in preconditions.iter().enumerate() {
            let pre_partitions = pre.output_partitioning().partition_count();
            if pre_partitions != num_partitions {
                return internal_err!(
                    "BarrierExec precondition {i} has {pre_partitions} partitions \
                     but main plan has {num_partitions} partitions"
                );
            }
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            plan.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        let mut children = preconditions;
        children.push(plan);
        Ok(Self {
            children,
            properties,
        })
    }

    fn preconditions(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.children[..self.children.len() - 1]
    }

    fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        // Safety: children always has at least one element (enforced in try_new)
        self.children
            .last()
            .unwrap_or_else(|| unreachable!("BarrierExec must have children"))
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() < 2 {
            return internal_err!(
                "BarrierExec requires at least 2 children (one precondition and the main plan)"
            );
        }
        let plan = children
            .pop()
            .unwrap_or_else(|| unreachable!("children is non-empty"));
        let preconditions = children;
        Ok(Arc::new(Self::try_new(preconditions, plan)?))
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
        // Collect precondition streams to exhaust before running the main plan.
        let precondition_streams: Vec<SendableRecordBatchStream> = self
            .preconditions()
            .iter()
            .map(|pre| pre.execute(partition, context.clone()))
            .collect::<Result<_>>()?;
        let main_plan = self.plan().clone();
        let schema = self.schema();
        // Exhaust each precondition stream sequentially, then run the main plan.
        // We use a once-stream that resolves to the main plan stream, then flatten.
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
