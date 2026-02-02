use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{internal_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// A placeholder execution plan for stage inputs.
#[derive(Debug, Clone)]
pub struct StageInputExec<I> {
    input: I,
    properties: PlanProperties,
}

impl<I> StageInputExec<I> {
    pub fn new(input: I, properties: PlanProperties) -> Self {
        Self { input, properties }
    }

    pub fn input(&self) -> &I {
        &self.input
    }
}

impl<I> DisplayAs for StageInputExec<I>
where
    I: fmt::Display,
{
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StageInputExec: input={}, partitioning={}",
            self.input,
            self.properties.output_partitioning()
        )
    }
}

impl<I> ExecutionPlan for StageInputExec<I>
where
    I: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "StageInputExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} does not accept children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("{} should be resolved before execution", self.name())
    }
}
