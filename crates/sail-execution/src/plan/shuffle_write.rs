use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;
use sail_physical_plan::repartition::RowRoundRobinPartitioner;

use crate::stream::writer::{TaskStreamWriteState, TaskStreamWriter};

enum ShufflePartitioner {
    Batch(BatchPartitioner),
    RoundRobin(RowRoundRobinPartitioner),
}

/// The partitioning scheme for shuffle output.
///
/// This captures only the schemes supported by shuffle writes. In particular,
/// row-level partitioning is only valid for round-robin distribution.
#[derive(Debug, Clone)]
pub(crate) enum ShufflePartitioning {
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    RoundRobinBatch(usize),
    RoundRobinRow(usize),
}

impl ShufflePartitioning {
    fn normalize(self) -> Self {
        match self {
            // An empty hash distribution is equivalent to batch-level
            // round-robin distribution.
            Self::Hash(expressions, partitions) if expressions.is_empty() => {
                Self::RoundRobinBatch(partitions)
            }
            Self::Hash(expressions, partitions) => {
                // https://github.com/apache/arrow-datafusion/issues/5184
                Self::Hash(
                    expressions
                        .into_iter()
                        .filter(|expression| !expression.is::<UnKnownColumn>())
                        .collect(),
                    partitions,
                )
            }
            partitioning => partitioning,
        }
    }

    fn partition_count(&self) -> usize {
        match self {
            Self::Hash(_, partitions)
            | Self::RoundRobinBatch(partitions)
            | Self::RoundRobinRow(partitions) => *partitions,
        }
    }
}

impl Display for ShufflePartitioning {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Hash(expressions, partitions) => {
                write!(
                    f,
                    "{}",
                    Partitioning::Hash(expressions.clone(), *partitions)
                )
            }
            Self::RoundRobinBatch(partitions) => {
                write!(f, "{}", Partitioning::RoundRobinBatch(*partitions))
            }
            Self::RoundRobinRow(partitions) => write!(f, "RoundRobinRow({partitions})"),
        }
    }
}

impl ShufflePartitioner {
    fn partition<F>(&mut self, batch: RecordBatch, f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        match self {
            Self::Batch(partitioner) => partitioner.partition(batch, f),
            Self::RoundRobin(partitioner) => partitioner.partition(batch, f),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    /// The partitioning scheme for the shuffle output.
    /// The partition count for the shuffle output can be different from the
    /// partition count of the input plan.
    partitioning: ShufflePartitioning,
    properties: Arc<PlanProperties>,
    writer: Arc<dyn TaskStreamWriter>,
}

impl ShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        writer: Arc<dyn TaskStreamWriter>,
        partitioning: ShufflePartitioning,
    ) -> Self {
        let partitioning = partitioning.normalize();
        let properties = Arc::new(PlanProperties::new(
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
        ));
        Self {
            plan,
            partitioning,
            properties,
            writer,
        }
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleWriteExec: partitioning={}", self.partitioning,)
    }
}

impl ExecutionPlan for ShuffleWriteExec {
    fn name(&self) -> &str {
        "ShuffleWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let writer = self.writer.clone();
        let stream = self.plan.execute(partition, context)?;
        // TODO: Support metrics in batch partitioner
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let partitioner = match &self.partitioning {
            ShufflePartitioning::Hash(expressions, partitions) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::Hash(expressions.clone(), *partitions),
                    Default::default(),
                    partition,
                    num_input_partitions,
                )?)
            }
            ShufflePartitioning::RoundRobinBatch(partitions) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::RoundRobinBatch(*partitions),
                    Default::default(),
                    partition,
                    num_input_partitions,
                )?)
            }
            ShufflePartitioning::RoundRobinRow(partitions) => ShufflePartitioner::RoundRobin(
                RowRoundRobinPartitioner::new(*partitions, partition, num_input_partitions)?,
            ),
        };
        let empty = RecordBatch::new_empty(self.schema());
        let channels = self.partitioning.partition_count();
        let output = futures::stream::once(async move {
            shuffle_write(writer, stream, partition, channels, partitioner).await?;
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
    partition: usize,
    channels: usize,
    mut partitioner: ShufflePartitioner,
) -> Result<()> {
    let mut sink = writer.open(partition).await?;
    let result = async {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let mut partitions: Vec<Option<RecordBatch>> = vec![None; channels];
            partitioner.partition(batch, |p, batch| {
                partitions[p] = Some(batch);
                Ok(())
            })?;
            for (channel, partition) in partitions.iter_mut().enumerate() {
                if let Some(batch) = partition.take()
                    && sink.write(channel, batch).await? == TaskStreamWriteState::Closed
                {
                    return Ok::<_, datafusion::error::DataFusionError>(false);
                }
            }
        }
        Ok(true)
    }
    .await;
    match result {
        Ok(true) => sink.commit().await,
        Ok(false) => {
            // TODO: model successful early-stop separately from error-triggered aborts
            sink.abort().await
        }
        Err(error) => {
            let _ = sink.abort().await;
            Err(error)
        }
    }
}
