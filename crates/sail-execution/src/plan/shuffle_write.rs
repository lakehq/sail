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
use sail_function::aggregate::array_agg::compact_payload;
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
                batch_rows: batch_size,
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
            return write_buffered(sink.as_mut(), &mut coalescers.channels).await;
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
    batch_rows: usize,
    pass_through_rows: usize,
}

impl Coalescers {
    /// Adds the partitioned batches of one input batch to their channels.
    ///
    /// Partitioned batches are slices of one batch holding every channel's rows, so
    /// the columns the coalescer would not copy are copied here before they are queued.
    fn push(&mut self, partitions: Vec<Option<RecordBatch>>) -> Result<()> {
        for (coalescer, batch) in self.channels.iter_mut().zip(partitions) {
            if let Some(batch) = batch {
                push_batch(coalescer, batch, self.batch_rows, self.pass_through_rows)?;
            }
        }
        Ok(())
    }
}

/// Adds a batch to the coalescer, copying the columns it would keep sharing the input's buffers.
fn push_batch(
    coalescer: &mut BatchCoalescer,
    batch: RecordBatch,
    batch_rows: usize,
    pass_through_rows: usize,
) -> Result<()> {
    let rows = batch.num_rows();
    let buffered = coalescer.get_buffered_rows();
    if rows > pass_through_rows
        && buffered > 0
        && buffered <= pass_through_rows
        && rows > batch_rows - buffered
    {
        // The coalescer merges a large batch into a small buffered one rather than passing
        // it through, and the full batches after the first would then share one copy.
        // So the rows of each output batch are copied on their own, with passing through
        // turned off to keep the same batch boundaries.
        coalescer.set_biggest_coalesce_batch_size(None);
        let mut offset = 0;
        while offset < rows {
            let len = (batch_rows - coalescer.get_buffered_rows()).min(rows - offset);
            push_copy(coalescer, batch.slice(offset, len), pass_through_rows)?;
            offset += len;
        }
        coalescer.set_biggest_coalesce_batch_size(Some(pass_through_rows));
        return Ok(());
    }
    push_copy(coalescer, batch, pass_through_rows)
}

fn push_copy(
    coalescer: &mut BatchCoalescer,
    batch: RecordBatch,
    pass_through_rows: usize,
) -> Result<()> {
    // A large batch can be passed through without copying any of its columns.
    let batch = if batch.num_rows() > pass_through_rows {
        copy_columns(&batch, |_| true)?
    } else {
        copy_columns(&batch, |data_type| !copied_by_coalescer(data_type))?
    };
    Ok(coalescer.push_batch(batch)?)
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
    // The copy still shares view payloads and dictionary values with the input.
    compact_payload(make_array(copy.freeze()))
}

/// Writes the batch still buffered in each channel, returning `false` if every channel is closed.
async fn write_buffered(
    sink: &mut dyn TaskStreamSink,
    coalescers: &mut [BatchCoalescer],
) -> Result<bool> {
    let batches = coalescers
        .iter_mut()
        .map(|coalescer| {
            coalescer.finish_buffered_batch()?;
            // The buffered batch can be a slice of the last batch pushed, which shrinking
            // cannot release.
            coalescer
                .next_completed_batch()
                .map(|batch| copy_columns(&batch, |_| true))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    if batches.iter().all(Option::is_none) {
        return Ok(true);
    }
    Ok(sink.write(batches).await? == TaskStreamWriteState::Active)
}

/// Writes the completed batches of all channels, returning `false` once every channel is closed.
async fn write_completed(
    sink: &mut dyn TaskStreamSink,
    coalescers: &mut [BatchCoalescer],
) -> Result<bool> {
    loop {
        let batches: Vec<Option<RecordBatch>> = coalescers
            .iter_mut()
            .map(|coalescer| {
                coalescer
                    .next_completed_batch()
                    .map(shrink_to_fit)
                    .transpose()
            })
            .collect::<Result<_>>()?;
        if batches.iter().all(Option::is_none) {
            return Ok(true);
        }
        if sink.write(batches).await? == TaskStreamWriteState::Closed {
            return Ok(false);
        }
    }
}

/// Releases the unused capacity of a completed batch.
///
/// The coalescer reserves a full batch for each column, and copies view strings
/// into blocks that grow from batch to batch.
fn shrink_to_fit(batch: RecordBatch) -> Result<RecordBatch> {
    let (schema, mut columns, rows) = batch.into_parts();
    for column in columns.iter_mut() {
        // A column shared with another batch cannot be reallocated.
        if let Some(column) = Arc::get_mut(column) {
            column.shrink_to_fit();
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use datafusion::arrow::array::{
        ArrayData, BooleanArray, Int64Array, ListArray, ListBuilder, StringArray, StringViewArray,
        StringViewBuilder, UInt32Array,
    };
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

    /// Batches with string, boolean, and list columns, including views with strings too long to inline.
    fn nested_input(batches: usize, rows: i64) -> Result<Vec<RecordBatch>> {
        let item = Arc::new(Field::new_list_field(DataType::Int64, true));
        let view_item = Arc::new(Field::new_list_field(DataType::Utf8View, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("l", DataType::List(item), false),
            Field::new("v", DataType::Utf8View, false),
            Field::new("lv", DataType::List(view_item), false),
        ]));
        (0..batches as i64)
            .map(|i| {
                let values = i * rows..(i + 1) * rows;
                let s = StringArray::from_iter_values(values.clone().map(|x| format!("{x:024}")));
                let b = BooleanArray::from_iter(values.clone().map(|x| Some(x % 3 == 0)));
                let l = ListArray::from_iter_primitive::<Int64Type, _, _>(
                    values
                        .clone()
                        .map(|x| Some(vec![Some(x), Some(x + 1), Some(x + 2)])),
                );
                let v =
                    StringViewArray::from_iter_values(values.clone().map(|x| format!("{x:032}")));
                let mut lv = ListBuilder::new(StringViewBuilder::new());
                for x in values {
                    lv.values().append_value(format!("{x:020}"));
                    lv.values().append_value(format!("{:020}", x + 1));
                    lv.append(true);
                }
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(s),
                        Arc::new(b),
                        Arc::new(l),
                        Arc::new(v),
                        Arc::new(lv.finish()),
                    ],
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

    /// Checks that a channel's batches share no memory with the input and that each column
    /// keeps at most twice the memory of a compacted copy of its rows.
    ///
    /// A consumer that falls behind holds on to the batches of one channel
    /// while the others are released, so this must hold for each channel.
    fn assert_compact(input: &[RecordBatch], channel: usize, output: &[RecordBatch]) -> Result<()> {
        let input_ranges = batch_ranges(input);
        let shared = batch_ranges(output)
            .into_iter()
            .filter(|out| {
                input_ranges
                    .iter()
                    .any(|r| out.start < r.end && r.start < out.end)
            })
            .count();
        assert_eq!(
            shared, 0,
            "channel {channel} shares {shared} buffers with the input"
        );

        for (i, batch) in output.iter().enumerate() {
            let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
            let copy = take_record_batch(batch, &indices)?;
            for (field, (column, copy)) in batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns().iter().zip(copy.columns()))
            {
                let retained = column.get_array_memory_size();
                // `take` still shares view payloads, so they are compacted separately.
                let compact = compact_payload(Arc::clone(copy))?.get_array_memory_size();
                assert!(
                    retained <= 2 * compact,
                    "column {} of batch {i} of channel {channel} retains {retained} bytes for {compact} bytes of rows",
                    field.name()
                );
            }
        }
        Ok(())
    }

    /// Shuffles the input by `column` and checks the output of every channel.
    async fn assert_shuffle_compact(
        plan: Arc<dyn ExecutionPlan>,
        input: &[RecordBatch],
        column: &str,
        channels: usize,
    ) -> Result<Capture> {
        let schema = input[0].schema();
        let partitioning = hash(&schema, column, channels)?;
        let capture = shuffle_with(plan, partitioning, TaskStreamWriteState::Active).await?;
        let expected = expected(input, vec![col(column, &schema)?], channels)?;
        for (channel, (output, expected)) in capture.channels.iter().zip(expected).enumerate() {
            assert_eq!(
                concat_batches(&schema, output)?,
                concat_batches(&schema, &expected)?
            );
            assert_compact(input, channel, output)?;
        }
        Ok(capture)
    }

    /// Returns the memory ranges of every buffer in the batches, including child arrays.
    fn batch_ranges(batches: &[RecordBatch]) -> Vec<Range<usize>> {
        fn add(data: &ArrayData, ranges: &mut Vec<Range<usize>>) {
            let nulls = data.nulls().map(|nulls| nulls.buffer());
            for buffer in data.buffers().iter().chain(nulls) {
                if !buffer.is_empty() {
                    let start = buffer.as_ptr() as usize;
                    ranges.push(start..start + buffer.len());
                }
            }
            for child in data.child_data() {
                add(child, ranges);
            }
        }
        let mut ranges = vec![];
        for column in batches.iter().flat_map(|batch| batch.columns()) {
            add(&column.to_data(), &mut ranges);
        }
        ranges
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
            assert_compact(&input, channel, output)?;
        }
        Ok(())
    }

    /// Each channel's only batch holds part of the single input batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_one_input_batch_does_not_retain_it() -> Result<()> {
        for (input, column) in [(input(1)?, "a"), (nested_input(1, ROWS)?, "s")] {
            let capture = assert_shuffle_compact(memory(&input)?, &input, column, CHANNELS).await?;
            assert!(capture.channels.iter().all(|output| output.len() == 1));
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
            assert_shuffle_compact(stream(&input, false)?, &input, column, 4).await?;
        }
        Ok(())
    }

    /// Each channel buffers a few rows before receiving several output batches of rows at once.
    #[tokio::test]
    async fn hash_shuffle_output_of_large_input_batch_after_small_one_does_not_retain_it()
    -> Result<()> {
        for (small, large, column) in [
            (
                input_with_rows(1, 256)?,
                input_with_rows(1, 16 * ROWS)?,
                "a",
            ),
            (nested_input(1, 256)?, nested_input(1, 16 * ROWS)?, "s"),
        ] {
            let input = [small, large].concat();
            assert_shuffle_compact(stream(&input, false)?, &input, column, 4).await?;
        }
        Ok(())
    }

    /// The coalescer copies view strings into blocks that grow from batch to batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_many_string_batches_is_compact() -> Result<()> {
        let input = nested_input(32, ROWS)?;
        assert_shuffle_compact(memory(&input)?, &input, "s", 4).await?;
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
