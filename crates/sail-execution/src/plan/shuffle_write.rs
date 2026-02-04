use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use futures::future::try_join_all;
use futures::StreamExt;

use crate::plan::ListListDisplay;
use crate::stream::writer::{TaskStreamSinkState, TaskStreamWriter, TaskWriteLocation};

#[derive(Debug, Clone)]
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    /// The partitioning scheme for the shuffle output.
    /// The partition count for the shuffle output can be different from the
    /// partition count of the input plan.
    shuffle_partitioning: Partitioning,
    /// For each input partition, a list of locations to write to.
    locations: Vec<Vec<TaskWriteLocation>>,
    properties: PlanProperties,
    writer: Arc<dyn TaskStreamWriter>,
}

impl ShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        locations: Vec<Vec<TaskWriteLocation>>,
        writer: Arc<dyn TaskStreamWriter>,
        partitioning: Partitioning,
    ) -> Self {
        let partitioning = match partitioning {
            Partitioning::Hash(expr, n) if expr.is_empty() => Partitioning::UnknownPartitioning(n),
            Partitioning::Hash(expr, n) => {
                // https://github.com/apache/arrow-datafusion/issues/5184
                Partitioning::Hash(
                    expr.into_iter()
                        .filter(|e| e.as_any().downcast_ref::<UnKnownColumn>().is_none())
                        .collect(),
                    n,
                )
            }
            _ => partitioning,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            // The shuffle write plan has the same number of partitions as the input plan.
            // For each partition that are executed, the data is further partitioned according to
            // the shuffle partitioning, resulting in multiple output streams.
            // These output streams are written to locations managed by the worker,
            // while the return value of `.execute()` is always an empty stream.
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            plan,
            shuffle_partitioning: partitioning,
            locations,
            properties,
            writer,
        }
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleWriteExec: partitioning={}, locations={}",
            self.shuffle_partitioning,
            ListListDisplay(&self.locations),
        )
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
            (Some(plan), true) => Ok(Arc::new(Self {
                plan,
                ..self.as_ref().clone()
            })),
            _ => plan_err!("ShuffleWriteExec should have one child"),
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
        let writer = self.writer.clone();
        if self.shuffle_partitioning.partition_count() != locations.len() {
            return internal_err!(
                "partition count mismatch: shuffle partitioning has {} partitions, but {} locations were provided",
                self.shuffle_partitioning.partition_count(),
                locations.len()
            );
        }
        let stream = self.plan.execute(partition, context)?;
        // TODO: Revisit this
        let shuffle_partitioning = match &self.shuffle_partitioning {
            Partitioning::UnknownPartitioning(size) => Partitioning::RoundRobinBatch(*size),
            shuffle_partitioning => shuffle_partitioning.clone(),
        };
        // TODO: Support metrics in batch partitioner
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let partitioner = BatchPartitioner::try_new(
            shuffle_partitioning,
            Default::default(),
            partition,
            num_input_partitions,
        )?;
        let empty = RecordBatch::new_empty(self.schema());
        let output = futures::stream::once(async move {
            shuffle_write(writer, stream, &locations, partitioner).await?;
            Ok(empty)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
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
    let mut partition_sinks = {
        let futures = locations
            .iter()
            .map(|location| writer.open(location, schema.clone()));
        try_join_all(futures)
            .await?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>()
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mut partitions: Vec<Option<RecordBatch>> = vec![None; partition_sinks.len()];
        partitioner.partition(batch, |p, batch| {
            partitions[p] = Some(batch);
            Ok(())
        })?;
        let mut active = 0;
        for p in 0..partitions.len() {
            let Some(sink) = partition_sinks[p].as_mut() else {
                continue;
            };
            // We should update the number of active sinks here,
            // even if the current batch does not have data for this partition.
            active += 1;
            if let Some(batch) = partitions[p].take() {
                match sink.write(Ok(batch)).await {
                    TaskStreamSinkState::Ok => {}
                    TaskStreamSinkState::Error(e) => {
                        return Err(e);
                    }
                    TaskStreamSinkState::Closed => {
                        partition_sinks[p] = None;
                        // This sink is closed when writing this batch,
                        // so we should not consider it active anymore.
                        active -= 1;
                    }
                }
            }
        }
        if active == 0 {
            break;
        }
    }
    // TODO: Ensure the sinks are cleaned up properly when an error causes an early return
    //   of this function. We need to consider this for sinks that handle remote data.
    let futures = partition_sinks
        .into_iter()
        .filter_map(|s| s.map(|x| x.close()));
    try_join_all(futures).await?;
    Ok(())
}
