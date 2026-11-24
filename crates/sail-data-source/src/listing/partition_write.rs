// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;

/// Lets listing sinks write the Spark Hive null marker as a partition value.
#[derive(Debug)]
pub struct HivePartitionWriteExec {
    input: Arc<dyn ExecutionPlan>,
}

impl HivePartitionWriteExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for HivePartitionWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for HivePartitionWriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.try_into().map_err(|_| {
            datafusion_common::internal_datafusion_err!("HivePartitionWriteExec expects one child")
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut config = context.session_config().clone();
        config
            .options_mut()
            .execution
            .listing_table_hive_default_partition_name
            .clear();
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        self.input.execute(partition, context)
    }
}
