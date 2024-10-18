use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ShuffleReadExec {
    schema: SchemaRef,
    properties: PlanProperties,
}

#[allow(dead_code)]
impl ShuffleReadExec {
    pub fn new(schema: SchemaRef, partitioning: Partitioning) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            ExecutionMode::Unbounded,
        );
        Self { schema, properties }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleReadExec")
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
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
            return Err(DataFusionError::Internal(
                "ShuffleReadExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}
