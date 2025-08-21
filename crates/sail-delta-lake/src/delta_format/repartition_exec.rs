use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_physical_expr::expressions::col;

/// DeltaRepartitionExec is a wrapper that encapsulates the logic for
/// data repartitioning for Delta Lake writes.
#[derive(Debug)]
pub struct DeltaRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Internal RepartitionExec instance
    repartition_exec: Arc<RepartitionExec>,
    /// Original partition column names for display purposes
    partition_columns: Vec<String>,
}

impl DeltaRepartitionExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>, // Original partition column names
    ) -> DFResult<Self> {
        let partitioning = if partition_columns.is_empty() {
            // No partition columns, ensure some parallelism
            // TODO: Make partition count configurable
            Partitioning::RoundRobinBatch(4)
        } else {
            // Use the original partition column names (DeltaProjectExec has moved them to the end)
            let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = partition_columns
                .iter()
                .map(|name| col(name, &input.schema()))
                .collect::<DFResult<_>>()?;

            // TODO: Partition count should be configurable
            let num_partitions = 4;
            Partitioning::Hash(partition_exprs, num_partitions)
        };

        let repartition_exec = Arc::new(RepartitionExec::try_new(input.clone(), partitioning)?);

        Ok(Self {
            input,
            repartition_exec,
            partition_columns,
        })
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }
}

impl ExecutionPlan for DeltaRepartitionExec {
    fn name(&self) -> &'static str {
        "DeltaRepartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.repartition_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DeltaRepartitionExec should have exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(DeltaRepartitionExec::try_new(
            children[0].clone(),
            self.partition_columns.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.repartition_exec.execute(partition, context)
    }
}

impl DisplayAs for DeltaRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaRepartitionExec: partition_columns={:?}",
                    self.partition_columns
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaRepartitionExec")
            }
        }
    }
}
