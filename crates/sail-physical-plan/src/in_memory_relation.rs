use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, Result};

/// Physical execution node for reading cached in-memory data.
#[derive(Debug, Clone)]
pub struct InMemoryRelationExec {
    #[allow(dead_code)]
    schema: SchemaRef,
    properties: PlanProperties,
}

impl InMemoryRelationExec {
    /// Creates a new InMemoryRelationExec with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { schema, properties }
    }
}

impl DisplayAs for InMemoryRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemoryRelationExec")
    }
}

impl ExecutionPlan for InMemoryRelationExec {
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("InMemoryRelationExec should have no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: retrieve cached RecordBatches from worker-local cache store
        // and return them as a stream
        internal_err!("InMemoryRelationExec cache read not yet implemented")
    }
}
