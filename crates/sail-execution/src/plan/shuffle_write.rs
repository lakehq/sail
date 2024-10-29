use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, exec_err, DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use futures::future::try_join_all;
use futures::StreamExt;

use crate::id::JobId;
use crate::stream::{TaskStreamWriter, TaskWriteLocation};

#[derive(Debug)]
pub struct ShuffleWriteExec {
    job_id: JobId,
    /// The stage that this execution plan is part of.
    stage: usize,
    plan: Arc<dyn ExecutionPlan>,
    /// For each input partition, a list of locations to write to.
    locations: Vec<Vec<TaskWriteLocation>>,
    properties: PlanProperties,
    writer: Option<Arc<dyn TaskStreamWriter>>,
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
            writer: None,
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

    pub fn locations(&self) -> &[Vec<TaskWriteLocation>] {
        &self.locations
    }

    pub fn with_locations(self, locations: Vec<Vec<TaskWriteLocation>>) -> Self {
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self
            .locations
            .get(partition)
            .ok_or_else(|| {
                exec_datafusion_err!("write locations for partition {partition} not found")
            })?
            .clone();
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| exec_datafusion_err!("writer not set"))?
            .clone();
        let partitioning = self.properties.output_partitioning().clone();
        if partitioning.partition_count() != locations.len() {
            return exec_err!(
                "partition count mismatch: partitioning has {} partitions, but {} locations were provided",
                partitioning.partition_count(),
                locations.len()
            );
        }
        let stream = self.plan.execute(partition, context)?;
        // TODO: support metrics in batch partitioner
        let partitioner = BatchPartitioner::try_new(partitioning, Default::default())?;
        let output_schema = Arc::new(Schema::empty());
        let output_data = RecordBatch::new_empty(output_schema.clone());
        let output = futures::stream::once(async move {
            shuffle_write(writer, stream, &locations, partitioner).await?;
            Ok(output_data)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }
}

async fn shuffle_write(
    writer: Arc<dyn TaskStreamWriter>,
    mut stream: SendableRecordBatchStream,
    locations: &[TaskWriteLocation],
    mut partitioner: BatchPartitioner,
) -> Result<()> {
    let schema = stream.schema();
    let mut partition_writers = {
        let futures = locations
            .iter()
            .map(|location| writer.open(location, schema.clone()));
        try_join_all(futures).await?
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mut partitions: Vec<Option<RecordBatch>> = vec![None; partition_writers.len()];
        partitioner.partition(batch, |p, batch| {
            partitions[p] = Some(batch);
            Ok(())
        })?;
        for p in 0..partitions.len() {
            if let Some(batch) = partitions[p].take() {
                partition_writers[p].write(&batch).await?;
            }
        }
    }
    partition_writers.into_iter().try_for_each(|w| w.close())?;
    Ok(())
}
