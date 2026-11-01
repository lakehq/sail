use std::fmt::{Display, Formatter};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{Result, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, RangePartitioning,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, apply_expression_roots,
};
use futures::StreamExt;
use sail_physical_plan::repartition::RowRoundRobinPartitioner;
use sail_system_store::types::{TaskOutputPartitionStatistics, TaskOutputStatistics};

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
    Range(RangePartitioning),
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
            Self::Hash(_, partitions) => *partitions,
            Self::Range(partitioning) => partitioning.partition_count(),
            Self::RoundRobinBatch(partitions) | Self::RoundRobinRow(partitions) => *partitions,
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
            Self::Range(partitioning) => write!(f, "{partitioning}"),
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
    // Each prepared task plan is executed once, for its TaskKey's partition.
    output_statistics: Arc<OnceLock<TaskOutputStatistics>>,
}

impl ShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        writer: Arc<dyn TaskStreamWriter>,
        partitioning: ShufflePartitioning,
        output_statistics: Arc<OnceLock<TaskOutputStatistics>>,
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
            output_statistics,
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

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match &self.partitioning {
            ShufflePartitioning::Hash(expressions, _) => apply_expression_roots(expressions, f),
            ShufflePartitioning::Range(range) => {
                apply_expression_roots(range.ordering().iter().map(|sort_expr| &sort_expr.expr), f)
            }
            ShufflePartitioning::RoundRobinBatch(_) | ShufflePartitioning::RoundRobinRow(_) => {
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("ShuffleWriteExec should have one child");
        }
        let plan = Arc::clone(&children[0]);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                plan,
                ..self.as_ref().clone()
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(Self::new(
                plan,
                Arc::clone(&self.writer),
                self.partitioning.clone(),
                self.output_statistics.clone(),
            ))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
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
            ShufflePartitioning::Range(partitioning) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::Range(partitioning.clone()),
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
        let output_statistics = self.output_statistics.clone();
        let output = futures::stream::once(async move {
            let statistics =
                shuffle_write(writer, stream, partition, channels, partitioner).await?;
            let _ = output_statistics.set(statistics);
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
) -> Result<TaskOutputStatistics> {
    let mut sink = writer.open(partition).await?;
    let mut statistics = TaskOutputStatistics {
        complete: false,
        partitions: vec![TaskOutputPartitionStatistics::default(); channels],
    };
    let result = async {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let mut partitions: Vec<Option<RecordBatch>> = vec![None; channels];
            partitioner.partition(batch, |p, batch| {
                statistics.partitions[p].num_rows += batch.num_rows() as u64;
                statistics.partitions[p].batch_bytes += get_record_batch_memory_size(&batch) as u64;
                partitions[p] = Some(batch);
                Ok(())
            })?;
            if sink.write(partitions).await? == TaskStreamWriteState::Closed {
                return Ok::<_, datafusion::error::DataFusionError>(false);
            }
        }
        Ok(true)
    }
    .await;
    match result {
        Ok(true) => {
            let completions = sink.commit().await?;
            if completions.len() != channels {
                return datafusion::common::internal_err!(
                    "shuffle commit returned an invalid channel count"
                );
            }
            statistics.complete = completions.iter().all(|x| x.complete);
            for (partition, completion) in statistics.partitions.iter_mut().zip(completions) {
                partition.stored_bytes = completion.stored_bytes;
            }
            Ok(statistics)
        }
        Ok(false) => {
            sink.abort().await?;
            Ok(statistics)
        }
        Err(error) => {
            let _ = sink.abort().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DataFusionError;

    use super::*;
    use crate::stream::writer::{TaskStreamSink, TaskStreamSinkCompletion};

    #[derive(Debug, Clone)]
    struct TestWriter {
        state: TaskStreamWriteState,
        fail_commit: bool,
        completions: Vec<TaskStreamSinkCompletion>,
        aborted: Arc<AtomicBool>,
    }

    impl TestWriter {
        fn new(channels: usize) -> Self {
            Self {
                state: TaskStreamWriteState::Active,
                fail_commit: false,
                completions: vec![
                    TaskStreamSinkCompletion {
                        complete: true,
                        stored_bytes: None
                    };
                    channels
                ],
                aborted: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[tonic::async_trait]
    impl TaskStreamWriter for TestWriter {
        async fn open(&self, _partition: usize) -> Result<Box<dyn TaskStreamSink>> {
            Ok(Box::new(self.clone()))
        }
    }

    #[tonic::async_trait]
    impl TaskStreamSink for TestWriter {
        async fn write(
            &mut self,
            _batches: Vec<Option<RecordBatch>>,
        ) -> Result<TaskStreamWriteState> {
            Ok(self.state)
        }

        async fn commit(self: Box<Self>) -> Result<Vec<TaskStreamSinkCompletion>> {
            if self.fail_commit {
                return datafusion::common::exec_err!("commit failed");
            }
            Ok(self.completions)
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            self.aborted.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch() -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?)
    }

    async fn run(
        writer: TestWriter,
        batches: Vec<Result<RecordBatch>>,
    ) -> Result<TaskOutputStatistics> {
        let channels = writer.completions.len();
        shuffle_write(
            Arc::new(writer),
            Box::pin(RecordBatchStreamAdapter::new(
                schema(),
                futures::stream::iter(batches),
            )),
            0,
            channels,
            ShufflePartitioner::RoundRobin(RowRoundRobinPartitioner::new(channels, 0, 1)?),
        )
        .await
    }

    #[tokio::test]
    async fn records_every_channel_including_empty_channels() -> Result<()> {
        let stats = run(
            TestWriter::new(5),
            vec![Ok(RecordBatch::new_empty(schema())), batch()],
        )
        .await?;
        assert!(stats.complete);
        assert_eq!(
            stats
                .partitions
                .iter()
                .map(|p| p.num_rows)
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 0, 0]
        );
        assert!(stats.partitions[..3].iter().all(|p| p.batch_bytes > 0));
        assert!(stats.partitions[3..].iter().all(|p| p.batch_bytes == 0));
        assert!(stats.partitions.iter().all(|p| p.stored_bytes.is_none()));
        let empty = run(TestWriter::new(2), vec![]).await?;
        assert!(empty.complete);
        assert!(empty.partitions.iter().all(|p| p.num_rows == 0));
        Ok(())
    }

    #[tokio::test]
    async fn early_close_is_an_incomplete_prefix() -> Result<()> {
        let mut writer = TestWriter::new(2);
        writer.state = TaskStreamWriteState::Closed;
        let aborted = writer.aborted.clone();
        // The error must never be polled after the consumer stops reading.
        let stats = run(
            writer,
            vec![batch(), Err(DataFusionError::Execution("unread".into()))],
        )
        .await?;
        assert!(!stats.complete);
        assert_eq!(stats.partitions.iter().map(|p| p.num_rows).sum::<u64>(), 3);
        assert!(aborted.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_channel_prevents_complete_output() -> Result<()> {
        let mut writer = TestWriter::new(2);
        writer.completions[1].complete = false;
        assert!(!run(writer, vec![batch()]).await?.complete);
        Ok(())
    }

    #[tokio::test]
    async fn errors_do_not_publish_output_statistics() {
        let mut writer = TestWriter::new(2);
        writer.fail_commit = true;
        assert!(run(writer, vec![batch()]).await.is_err());
        let writer = TestWriter::new(2);
        let aborted = writer.aborted.clone();
        assert!(
            run(
                writer,
                vec![Err(DataFusionError::Execution("read failed".into()))]
            )
            .await
            .is_err()
        );
        assert!(aborted.load(Ordering::SeqCst));
    }
}
