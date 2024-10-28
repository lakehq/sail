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

use crate::id::JobId;
use crate::stream::ChannelName;

#[derive(Debug, Clone)]
pub enum ShuffleReadLocation {
    ThisWorker {
        channel: ChannelName,
    },
    OtherWorker {
        host: String,
        port: u16,
        channel: ChannelName,
    },
    Remote {
        uri: String,
    },
}

#[derive(Debug)]
pub struct ShuffleReadExec {
    job_id: JobId,
    /// The stage to read from.
    stage: usize,
    /// For each output partition, a list of locations to read from.
    locations: Vec<Vec<ShuffleReadLocation>>,
    properties: PlanProperties,
}

impl ShuffleReadExec {
    pub fn new(job_id: JobId, stage: usize, schema: SchemaRef, partitioning: Partitioning) -> Self {
        let partition_count = partitioning.partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            ExecutionMode::Unbounded,
        );
        Self {
            job_id,
            stage,
            locations: vec![vec![]; partition_count],
            properties,
        }
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn stage(&self) -> usize {
        self.stage
    }

    pub fn partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    pub fn locations(&self) -> &[Vec<ShuffleReadLocation>] {
        &self.locations
    }

    pub fn with_locations(self, locations: Vec<Vec<ShuffleReadLocation>>) -> Self {
        Self { locations, ..self }
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
