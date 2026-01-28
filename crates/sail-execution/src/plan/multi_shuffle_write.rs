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

use crate::stream::writer::{TaskStreamSinkState, TaskStreamWriter, TaskWriteLocation};

#[derive(Debug, Clone)]
pub struct MultiShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    outputs: Vec<MultiShuffleWriteOutput>,
    properties: PlanProperties,
    writer: Arc<dyn TaskStreamWriter>,
}

#[derive(Debug, Clone)]
struct MultiShuffleWriteOutput {
    shuffle_partitioning: Partitioning,
    locations: Vec<Vec<TaskWriteLocation>>,
}

impl MultiShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        outputs: Vec<(Partitioning, Vec<Vec<TaskWriteLocation>>)>,
        writer: Arc<dyn TaskStreamWriter>,
    ) -> Self {
        let outputs = outputs
            .into_iter()
            .map(|(partitioning, locations)| {
                let partitioning = match partitioning {
                    Partitioning::Hash(expr, n) if expr.is_empty() => {
                        Partitioning::UnknownPartitioning(n)
                    }
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
                MultiShuffleWriteOutput {
                    shuffle_partitioning: partitioning,
                    locations,
                }
            })
            .collect::<Vec<_>>();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            plan,
            outputs,
            properties,
            writer,
        }
    }
}

impl DisplayAs for MultiShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiShuffleWriteExec: outputs={}, locations={:?}",
            self.outputs.len(),
            self.outputs
                .iter()
                .map(|output| output.locations.clone())
                .collect::<Vec<_>>()
        )
    }
}

impl ExecutionPlan for MultiShuffleWriteExec {
    fn name(&self) -> &str {
        "MultiShuffleWriteExec"
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
            _ => plan_err!("MultiShuffleWriteExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.outputs.is_empty() {
            return internal_err!("multi shuffle write has no outputs");
        }
        let outputs = self
            .outputs
            .iter()
            .map(|output| {
                output.locations.get(partition).cloned().ok_or_else(|| {
                    exec_datafusion_err!("write locations for partition {partition} not found")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let writer = self.writer.clone();
        let stream = self.plan.execute(partition, context)?;
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let mut partitioners = self
            .outputs
            .iter()
            .map(|output| {
                let shuffle_partitioning = match &output.shuffle_partitioning {
                    Partitioning::UnknownPartitioning(size) => {
                        Partitioning::RoundRobinBatch(*size)
                    }
                    shuffle_partitioning => shuffle_partitioning.clone(),
                };
                BatchPartitioner::try_new(
                    shuffle_partitioning,
                    Default::default(),
                    partition,
                    num_input_partitions,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let output_schema = Arc::new(Schema::empty());
        let output_data = RecordBatch::new_empty(output_schema.clone());
        let output = futures::stream::once(async move {
            multi_shuffle_write(writer, stream, &outputs, &mut partitioners).await?;
            Ok(output_data)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn multi_shuffle_write(
    writer: Arc<dyn TaskStreamWriter>,
    mut stream: SendableRecordBatchStream,
    outputs: &[Vec<TaskWriteLocation>],
    partitioners: &mut [BatchPartitioner],
) -> Result<()> {
    if partitioners.len() != outputs.len() {
        return internal_err!(
            "partitioner count mismatch: expected {}, got {}",
            outputs.len(),
            partitioners.len()
        );
    }
    let schema = stream.schema();
    let mut all_sinks = Vec::with_capacity(outputs.len());
    for locations in outputs {
        let futures = locations
            .iter()
            .map(|location| writer.open(location, schema.clone()));
        let sinks = try_join_all(futures)
            .await?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        all_sinks.push(sinks);
    }
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for (output_idx, partitioner) in partitioners.iter_mut().enumerate() {
            let sinks = &mut all_sinks[output_idx];
            let mut partitions: Vec<Option<RecordBatch>> = vec![None; sinks.len()];
            partitioner.partition(batch.clone(), |p, batch| {
                partitions[p] = Some(batch);
                Ok(())
            })?;
            let mut active = 0;
            for p in 0..partitions.len() {
                let Some(sink) = sinks[p].as_mut() else {
                    continue;
                };
                active += 1;
                if let Some(batch) = partitions[p].take() {
                    match sink.write(Ok(batch)).await {
                        TaskStreamSinkState::Ok => {}
                        TaskStreamSinkState::Error(e) => {
                            return Err(e);
                        }
                        TaskStreamSinkState::Closed => {
                            sinks[p] = None;
                            active -= 1;
                        }
                    }
                }
            }
            if active == 0 {
                return Ok(());
            }
        }
    }
    for sinks in all_sinks {
        for sink in sinks {
            if let Some(sink) = sink {
                sink.close().await?;
            }
        }
    }
    Ok(())
}
