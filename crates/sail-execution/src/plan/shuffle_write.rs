use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};

use crate::id::JobId;
use crate::stream::ChannelName;

#[derive(Debug, Clone)]
pub enum ShuffleWriteLocation {
    Memory { channel: ChannelName },
    Disk { channel: ChannelName },
    Remote { uri: String },
}

#[derive(Debug)]
pub struct ShuffleWriteExec {
    job_id: JobId,
    /// The stage that this execution plan is part of.
    stage: usize,
    plan: Arc<dyn ExecutionPlan>,
    /// For each input partition, a list of locations to write to.
    locations: Vec<Vec<ShuffleWriteLocation>>,
    properties: PlanProperties,
}

impl ShuffleWriteExec {
    pub fn new(
        job_id: JobId,
        stage: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Self {
        let input_partition_count = plan.output_partitioning().partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            partitioning,
            ExecutionMode::Unbounded,
        );
        let locations = vec![vec![]; input_partition_count];
        Self {
            job_id,
            stage,
            plan,
            locations,
            properties,
        }
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn stage(&self) -> usize {
        self.stage
    }

    pub fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    pub fn partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    pub fn locations(&self) -> &[Vec<ShuffleWriteLocation>] {
        &self.locations
    }

    pub fn with_locations(self, locations: Vec<Vec<ShuffleWriteLocation>>) -> Self {
        Self { locations, ..self }
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
                self.job_id,
                self.stage,
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
