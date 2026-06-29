use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, Result};

/// A wrapper around a `DataSinkExec` that overrides `minimum_parallel_output_files`
/// in the `TaskContext` session config to match the number of output partitions.
///
/// The output partition count is captured at **planning time** from
/// `physical_input.output_partitioning().partition_count()`. It is injected into
/// the `SessionConfig` at **execution time** by cloning the `TaskContext`, modifying
/// the config, and delegating to the inner plan. The downstream `row_count_demuxer()`
/// then reads the modified config value and creates the corresponding number of
/// parallel output streams.

#[derive(Debug, Clone)]
pub struct OutputFileCountExec {
    input: Arc<dyn ExecutionPlan>,
    num_output_partitions: usize,
    properties: Arc<PlanProperties>,
}

impl OutputFileCountExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, num_output_partitions: usize) -> Self {
        let properties = Arc::new(input.properties().as_ref().clone());
        Self {
            input,
            num_output_partitions,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn num_output_partitions(&self) -> usize {
        self.num_output_partitions
    }
}

impl DisplayAs for OutputFileCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "OutputFileCountExec: partitions={}",
            self.num_output_partitions
        )
    }
}

impl ExecutionPlan for OutputFileCountExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return internal_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(Self::new(child, self.num_output_partitions)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut session_config = context.session_config().clone();
        session_config
            .options_mut()
            .execution
            .minimum_parallel_output_files = std::cmp::max(1, self.num_output_partitions);

        let new_context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            session_config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));

        self.input.execute(partition, new_context)
    }
}
