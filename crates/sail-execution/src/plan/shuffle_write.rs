use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MutableArrayData, make_array};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
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

use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState, TaskStreamWriter};

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
        let batch_size = context.session_config().batch_size();
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
        // Partitioned batches can be slices sharing the whole input batch's buffers,
        // so they are copied into full batches before being queued. Unbounded input
        // is not coalesced so that partial batches flow promptly.
        let coalescers = (!self.plan.properties().boundedness.is_unbounded()).then(|| {
            // The same limit as DataFusion's coalescer for passing large batches through.
            let pass_through_rows = batch_size / 2;
            Coalescers {
                channels: (0..channels)
                    .map(|_| {
                        BatchCoalescer::new(stream.schema(), batch_size)
                            .with_biggest_coalesce_batch_size(Some(pass_through_rows))
                    })
                    .collect(),
                pass_through_rows,
            }
        });
        let output = futures::stream::once(async move {
            shuffle_write(writer, stream, partition, channels, partitioner, coalescers).await?;
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
    mut coalescers: Option<Coalescers>,
) -> Result<()> {
    let mut sink = writer.open(partition).await?;
    let result = async {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let mut partitions: Vec<Option<RecordBatch>> = vec![None; channels];
            partitioner.partition(batch, |p, batch| {
                partitions[p] = Some(batch);
                Ok(())
            })?;
            let active = match &mut coalescers {
                Some(coalescers) => {
                    coalescers.push(partitions)?;
                    write_completed(sink.as_mut(), &mut coalescers.channels).await?
                }
                None => sink.write(partitions).await? == TaskStreamWriteState::Active,
            };
            if !active {
                return Ok::<_, datafusion::error::DataFusionError>(false);
            }
        }
        if let Some(coalescers) = &mut coalescers {
            for coalescer in coalescers.channels.iter_mut() {
                coalescer.finish_buffered_batch()?;
            }
            return write_completed(sink.as_mut(), &mut coalescers.channels).await;
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

/// One batch coalescer per output channel.
struct Coalescers {
    channels: Vec<BatchCoalescer>,
    pass_through_rows: usize,
}

impl Coalescers {
    /// Adds the partitioned batches of one input batch to their channels.
    ///
    /// Partitioned batches are slices of one batch holding every channel's rows, so
    /// the columns the coalescer would not copy are copied here before they are queued.
    fn push(&mut self, partitions: Vec<Option<RecordBatch>>) -> Result<()> {
        for (coalescer, batch) in self.channels.iter_mut().zip(partitions) {
            let Some(batch) = batch else {
                continue;
            };
            // A large batch can be passed through without copying any of its columns.
            let batch = if batch.num_rows() > self.pass_through_rows {
                copy_columns(&batch, |_| true)?
            } else {
                copy_columns(&batch, |data_type| !copied_by_coalescer(data_type))?
            };
            coalescer.push_batch(batch)?;
        }
        Ok(())
    }
}

// The coalescer keeps columns of other types as slices of their input until it
// finishes a batch, and passes on a batch made of one slice without copying it.
/// Returns whether the coalescer copies rows of this type as they arrive.
fn copied_by_coalescer(data_type: &DataType) -> bool {
    data_type.is_primitive() || matches!(data_type, DataType::Utf8View | DataType::BinaryView)
}

/// Copies the columns whose type matches, so they no longer share the input's buffers.
fn copy_columns(batch: &RecordBatch, copy: impl Fn(&DataType) -> bool) -> Result<RecordBatch> {
    if !batch
        .columns()
        .iter()
        .any(|column| copy(column.data_type()))
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            if copy(column.data_type()) {
                copy_array(column)
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<_>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

fn copy_array(array: &ArrayRef) -> Result<ArrayRef> {
    let data = array.to_data();
    let mut copy = MutableArrayData::new(vec![&data], false, data.len());
    copy.try_extend(0, 0, data.len())?;
    Ok(make_array(copy.freeze()))
}

/// Writes the completed batches of all channels, returning `false` once every channel is closed.
async fn write_completed(
    sink: &mut dyn TaskStreamSink,
    coalescers: &mut [BatchCoalescer],
) -> Result<bool> {
    loop {
        let batches: Vec<Option<RecordBatch>> = coalescers
            .iter_mut()
            .map(|coalescer| coalescer.next_completed_batch())
            .collect();
        if batches.iter().all(Option::is_none) {
            return Ok(true);
        }
        if sink.write(batches).await? == TaskStreamWriteState::Closed {
            return Ok(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array, ListArray, StringArray, UInt32Array};
    use datafusion::arrow::compute::{concat_batches, take_record_batch};
    use datafusion::arrow::datatypes::{Field, Int64Type, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use tokio::sync::Mutex;

    use super::*;

    const CHANNELS: usize = 32;
    const ROWS: i64 = 8192;

    #[derive(Debug, Default)]
    struct Capture {
        channels: Vec<Vec<RecordBatch>>,
        writes: usize,
        committed: bool,
        aborted: bool,
    }

    /// Records everything written, reporting `state` after each write.
    #[derive(Debug)]
    struct CaptureWriter {
        capture: Arc<Mutex<Capture>>,
        state: TaskStreamWriteState,
    }

    struct CaptureSink {
        capture: Arc<Mutex<Capture>>,
        state: TaskStreamWriteState,
    }

    #[tonic::async_trait]
    impl TaskStreamWriter for CaptureWriter {
        async fn open(&self, _partition: usize) -> Result<Box<dyn TaskStreamSink>> {
            Ok(Box::new(CaptureSink {
                capture: Arc::clone(&self.capture),
                state: self.state,
            }))
        }
    }

    #[tonic::async_trait]
    impl TaskStreamSink for CaptureSink {
        async fn write(
            &mut self,
            batches: Vec<Option<RecordBatch>>,
        ) -> Result<TaskStreamWriteState> {
            let mut capture = self.capture.lock().await;
            capture.writes += 1;
            capture.channels.resize(batches.len(), vec![]);
            for (channel, batch) in batches.into_iter().enumerate() {
                capture.channels[channel].extend(batch);
            }
            Ok(self.state)
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            self.capture.lock().await.committed = true;
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            self.capture.lock().await.aborted = true;
            Ok(())
        }
    }

    /// Streams its batches as they are, without splitting them to the configured batch size.
    #[derive(Debug)]
    struct StreamSource(Vec<RecordBatch>);

    impl PartitionStream for StreamSource {
        fn schema(&self) -> &SchemaRef {
            self.0[0].schema_ref()
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let batches = self.0.clone().into_iter().map(Ok);
            Box::pin(RecordBatchStreamAdapter::new(
                self.0[0].schema(),
                futures::stream::iter(batches),
            ))
        }
    }

    fn input(batches: usize) -> Result<Vec<RecordBatch>> {
        input_with_rows(batches, ROWS)
    }

    fn input_with_rows(batches: usize, rows: i64) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        (0..batches as i64)
            .map(|i| {
                let values = Int64Array::from_iter_values(i * rows..(i + 1) * rows);
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(values)],
                )?)
            })
            .collect()
    }

    /// Batches with string, boolean, and list columns, which the coalescer does not copy as they arrive.
    fn nested_input(batches: usize, rows: i64) -> Result<Vec<RecordBatch>> {
        let item = Arc::new(Field::new_list_field(DataType::Int64, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("l", DataType::List(item), false),
        ]));
        (0..batches as i64)
            .map(|i| {
                let values = i * rows..(i + 1) * rows;
                let s = StringArray::from_iter_values(values.clone().map(|x| format!("{x:024}")));
                let b = BooleanArray::from_iter(values.clone().map(|x| Some(x % 3 == 0)));
                let l = ListArray::from_iter_primitive::<Int64Type, _, _>(
                    values.map(|x| Some(vec![Some(x), Some(x + 1), Some(x + 2)])),
                );
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(s), Arc::new(b), Arc::new(l)],
                )?)
            })
            .collect()
    }

    fn hash(schema: &Schema, column: &str, channels: usize) -> Result<ShufflePartitioning> {
        Ok(ShufflePartitioning::Hash(
            vec![col(column, schema)?],
            channels,
        ))
    }

    async fn shuffle(plan: Arc<dyn ExecutionPlan>, state: TaskStreamWriteState) -> Result<Capture> {
        let partitioning = hash(&plan.schema(), "a", CHANNELS)?;
        shuffle_with(plan, partitioning, state).await
    }

    async fn shuffle_with(
        plan: Arc<dyn ExecutionPlan>,
        partitioning: ShufflePartitioning,
        state: TaskStreamWriteState,
    ) -> Result<Capture> {
        let capture = Arc::new(Mutex::new(Capture::default()));
        let writer = Arc::new(CaptureWriter {
            capture: Arc::clone(&capture),
            state,
        });
        let exec = ShuffleWriteExec::new(plan, writer, partitioning);
        collect(exec.execute(0, Arc::new(TaskContext::default()))?).await?;
        let capture = std::mem::take(&mut *capture.lock().await);
        Ok(capture)
    }

    fn stream(batches: &[RecordBatch], unbounded: bool) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            batches[0].schema(),
            vec![Arc::new(StreamSource(batches.to_vec()))],
            None,
            Vec::new(),
            unbounded,
            None,
        )?))
    }

    /// Rows routed by `BatchPartitioner` on its own, one list of batches per channel.
    fn expected(
        input: &[RecordBatch],
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        channels: usize,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let mut partitioner = BatchPartitioner::try_new(
            Partitioning::Hash(expressions, channels),
            Default::default(),
            0,
            1,
        )?;
        let mut expected = vec![vec![]; channels];
        for batch in input {
            partitioner.partition(batch.clone(), |p, batch| {
                expected[p].push(batch);
                Ok(())
            })?;
        }
        Ok(expected)
    }

    /// Checks that a channel's batches keep at most twice the memory of a copy of their rows.
    ///
    /// A consumer that falls behind holds on to the batches of one channel
    /// while the others are released, so this must hold for each channel.
    fn assert_compact(schema: &SchemaRef, channel: usize, output: &[RecordBatch]) -> Result<()> {
        let retained: usize = output.iter().map(|b| b.get_array_memory_size()).sum();
        let rows = concat_batches(schema, output)?;
        let indices = UInt32Array::from_iter_values(0..rows.num_rows() as u32);
        // Concatenating a single batch returns it unchanged, so `take` makes the copy.
        let compact = take_record_batch(&rows, &indices)?.get_array_memory_size();
        assert!(
            retained <= 2 * compact,
            "channel {channel} retains {retained} bytes for {compact} bytes of rows"
        );
        Ok(())
    }

    fn memory(batches: &[RecordBatch]) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[batches.to_vec()],
            batches[0].schema(),
            None,
        )?)
    }

    #[tokio::test]
    async fn hash_shuffle_output_does_not_retain_input_batches() -> Result<()> {
        let input = input(64)?;
        let capture = shuffle(memory(&input)?, TaskStreamWriteState::Active).await?;
        for (channel, output) in capture.channels.iter().enumerate() {
            assert_compact(&input[0].schema(), channel, output)?;
        }
        Ok(())
    }

    /// Each channel's only batch holds part of the single input batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_one_input_batch_does_not_retain_it() -> Result<()> {
        let input = nested_input(1, ROWS)?;
        let schema = input[0].schema();
        let partitioning = hash(&schema, "s", CHANNELS)?;
        let capture =
            shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
        let expected = expected(&input, vec![col("s", &schema)?], CHANNELS)?;
        for (channel, (output, expected)) in capture.channels.iter().zip(expected).enumerate() {
            assert_eq!(output.len(), 1);
            assert_eq!(
                concat_batches(&schema, output)?,
                concat_batches(&schema, &expected)?
            );
            assert_compact(&schema, channel, output)?;
        }
        Ok(())
    }

    /// Each channel receives more rows from one input batch than fit in an output batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_large_input_batches_does_not_retain_them() -> Result<()> {
        for (input, column) in [
            (input_with_rows(4, 8 * ROWS)?, "a"),
            (nested_input(4, 8 * ROWS)?, "s"),
        ] {
            let schema = input[0].schema();
            let partitioning = hash(&schema, column, 4)?;
            let capture = shuffle_with(
                stream(&input, false)?,
                partitioning,
                TaskStreamWriteState::Active,
            )
            .await?;
            let expected = expected(&input, vec![col(column, &schema)?], 4)?;
            for (channel, (output, expected)) in capture.channels.iter().zip(expected).enumerate() {
                assert_eq!(
                    concat_batches(&schema, output)?,
                    concat_batches(&schema, &expected)?
                );
                assert_compact(&schema, channel, output)?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn hash_shuffle_output_matches_batch_partitioner() -> Result<()> {
        let input = input(64)?;
        let schema = input[0].schema();
        let capture = shuffle(memory(&input)?, TaskStreamWriteState::Active).await?;
        assert!(capture.committed && !capture.aborted);

        let expected = expected(&input, vec![col("a", &schema)?], CHANNELS)?;

        let batch_size = TaskContext::default().session_config().batch_size();
        for (channel, (actual, expected)) in capture.channels.iter().zip(expected).enumerate() {
            assert_eq!(
                concat_batches(&schema, actual)?,
                concat_batches(&schema, &expected)?,
                "channel {channel}"
            );
            // Every batch is full except the residual flushed at the end.
            let (last, full) = actual.split_last().unzip();
            assert!(full.is_some_and(|full| full.iter().all(|b| b.num_rows() == batch_size)));
            assert!(last.is_some_and(|last| last.num_rows() > 0 && last.num_rows() <= batch_size));
        }
        Ok(())
    }

    #[tokio::test]
    async fn hash_shuffle_stops_when_all_channels_close() -> Result<()> {
        let input = input(64)?;
        let capture = shuffle(memory(&input)?, TaskStreamWriteState::Closed).await?;
        assert_eq!(capture.writes, 1);
        assert!(capture.aborted && !capture.committed);
        Ok(())
    }

    #[tokio::test]
    async fn hash_shuffle_does_not_coalesce_unbounded_input() -> Result<()> {
        let input = input(4)?;
        let capture = shuffle(stream(&input, true)?, TaskStreamWriteState::Active).await?;
        assert_eq!(capture.writes, input.len());
        assert!(capture.committed);
        Ok(())
    }
}
