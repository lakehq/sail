use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

#[allow(dead_code)]
impl ShuffleWriteExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            partitioning,
            ExecutionMode::Unbounded,
        );
        Self { plan, properties }
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleWriteExec")
    }
}

impl ExecutionPlan for ShuffleWriteExec {
    fn name(&self) -> &str {
        "ShuffleWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(child), true) => Ok(Arc::new(Self::new(
                child,
                self.properties.partitioning.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "ShuffleWriteExec does not accept children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}
