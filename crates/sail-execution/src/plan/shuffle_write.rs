use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
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

use crate::plan::shuffle_compact::{
    contains_dictionary, copy, dictionary_capacities, dictionary_sizes, oversized, trim,
};
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
        let slices = matches!(
            self.partitioning,
            ShufflePartitioning::Hash(..) | ShufflePartitioning::Range(_)
        );
        let coalescers = (!self.plan.properties().boundedness.is_unbounded())
            .then(|| Coalescers::new(stream.schema(), channels, batch_size, slices));
        let output = futures::stream::once(async move {
            shuffle_write(
                writer,
                stream,
                partition,
                channels,
                partitioner,
                slices,
                coalescers,
            )
            .await?;
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
    slices: bool,
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
                None => {
                    let partitions = compact_unbounded(partitions, slices)?;
                    sink.write(partitions).await? == TaskStreamWriteState::Active
                }
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
    channels: Vec<Channel>,
    /// Whether partitioned batches are slices of one batch holding every channel's rows,
    /// rather than whole input batches or batches taken for one channel.
    slices: bool,
    batch_rows: usize,
    pass_through_rows: usize,
    /// How many values each dictionary in the schema can hold before its keys overflow.
    dictionary_capacities: Vec<usize>,
}

impl Coalescers {
    fn new(schema: SchemaRef, channels: usize, batch_rows: usize, slices: bool) -> Self {
        // The same limit as DataFusion's coalescer for passing large batches through.
        let pass_through_rows = batch_rows / 2;
        let dictionary_capacities = dictionary_capacities(&schema);
        Self {
            channels: (0..channels)
                .map(|_| Channel {
                    coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_rows)
                        .with_biggest_coalesce_batch_size(Some(pass_through_rows)),
                    dictionary_values: vec![0; dictionary_capacities.len()],
                })
                .collect(),
            slices,
            batch_rows,
            pass_through_rows,
            dictionary_capacities,
        }
    }

    /// Adds the partitioned batches of one input batch to their channels.
    fn push(&mut self, partitions: Vec<Option<RecordBatch>>) -> Result<()> {
        for (channel, batch) in self.channels.iter_mut().zip(partitions) {
            if let Some(batch) = batch {
                channel.push_partition(
                    batch,
                    self.slices,
                    self.batch_rows,
                    self.pass_through_rows,
                    &self.dictionary_capacities,
                )?;
            }
        }
        Ok(())
    }
}

struct Channel {
    coalescer: BatchCoalescer,
    /// The number of values of each dictionary in the buffered rows.
    dictionary_values: Vec<usize>,
}

impl Channel {
    /// Adds a partitioned batch, copying the columns the coalescer would otherwise keep
    /// sharing buffers with rows that are not in its output.
    fn push_partition(
        &mut self,
        batch: RecordBatch,
        slices: bool,
        batch_rows: usize,
        pass_through_rows: usize,
        dictionary_capacities: &[usize],
    ) -> Result<()> {
        let rows = batch.num_rows();
        let buffered = self.coalescer.get_buffered_rows();
        let large = rows > pass_through_rows;
        if large && (buffered == 0 || buffered > pass_through_rows) {
            // The coalescer passes the batch on as it is.
            let batch = if slices {
                map_columns(batch, |column| copy(&column))?
            } else {
                map_columns(batch, compact_input)?
            };
            return self.push(batch, dictionary_capacities, None);
        }
        if large && rows > batch_rows - buffered {
            // The coalescer merges a large batch into a small buffered one rather than passing
            // it through, and the full batches after the first would then share its buffers.
            // So the rows of each output batch are copied on their own, with passing through
            // turned off to keep the same batch boundaries.
            self.coalescer.set_biggest_coalesce_batch_size(None);
            let mut offset = 0;
            while offset < rows {
                let len = (batch_rows - self.coalescer.get_buffered_rows()).min(rows - offset);
                let part = map_columns(batch.slice(offset, len), copy_unless_coalesced)?;
                self.push(part, dictionary_capacities, None)?;
                offset += len;
            }
            self.coalescer
                .set_biggest_coalesce_batch_size(Some(pass_through_rows));
            return Ok(());
        }
        let batch = if slices {
            map_columns(batch, copy_unless_coalesced)?
        } else {
            map_columns(batch, |column| {
                if copied_by_coalescer(column.data_type()) {
                    Ok(column)
                } else {
                    compact_input(column)
                }
            })?
        };
        self.push(batch, dictionary_capacities, Some(pass_through_rows))
    }

    /// Adds a batch to the coalescer, first finishing the buffered batch if concatenating
    /// their dictionaries could overflow the keys.
    ///
    /// Arrow panics on such an overflow for values it cannot merge. A batch's dictionaries
    /// have been compacted to the values its keys use, so they always fit on their own.
    ///
    /// `pass_through_rows` is set for a batch with columns left for the coalescer to copy,
    /// which must not be passed through when finishing early empties the buffer.
    fn push(
        &mut self,
        batch: RecordBatch,
        dictionary_capacities: &[usize],
        pass_through_rows: Option<usize>,
    ) -> Result<()> {
        if dictionary_capacities.is_empty() {
            return Ok(self.coalescer.push_batch(batch)?);
        }
        let incoming = dictionary_sizes(&batch);
        let overflows = self
            .dictionary_values
            .iter()
            .zip(&incoming)
            .zip(dictionary_capacities)
            .any(|((buffered, incoming), capacity)| buffered + incoming > *capacity);
        let finished = overflows && self.coalescer.get_buffered_rows() > 0;
        if finished {
            self.coalescer.finish_buffered_batch()?;
            self.dictionary_values.fill(0);
        }
        let held = pass_through_rows.filter(|_| finished);
        if held.is_some() {
            self.coalescer.set_biggest_coalesce_batch_size(None);
        }
        let expected = self.coalescer.get_buffered_rows() + batch.num_rows();
        self.coalescer.push_batch(batch)?;
        if held.is_some() {
            self.coalescer.set_biggest_coalesce_batch_size(held);
        }
        let buffered = self.coalescer.get_buffered_rows();
        if buffered == 0 {
            self.dictionary_values.fill(0);
        } else if buffered < expected {
            // A batch was completed, and only the rest of this one is still buffered.
            self.dictionary_values = incoming;
        } else {
            for (values, incoming) in self.dictionary_values.iter_mut().zip(incoming) {
                *values += incoming;
            }
        }
        Ok(())
    }
}

// The coalescer keeps columns of other types as slices of their input until it
// finishes a batch, and passes on a batch made of one slice without copying it.
// It copies view strings only when their buffers look mostly unused, and completed
// batches are compacted for the rest.
/// Returns whether the coalescer copies rows of this type as they arrive.
fn copied_by_coalescer(data_type: &DataType) -> bool {
    data_type.is_primitive() || matches!(data_type, DataType::Utf8View | DataType::BinaryView)
}

fn copy_unless_coalesced(column: ArrayRef) -> Result<ArrayRef> {
    if copied_by_coalescer(column.data_type()) {
        Ok(column)
    } else {
        copy(&column)
    }
}

/// Copies a column of a whole input batch if it holds much more memory than its rows need,
/// and compacts its dictionaries so that they hold only the values its keys use.
///
/// An input batch can be a slice of a larger batch from upstream.
fn compact_input(column: ArrayRef) -> Result<ArrayRef> {
    if oversized(&column) {
        copy(&column)
    } else if contains_dictionary(column.data_type()) {
        trim(column, false)
    } else {
        Ok(column)
    }
}

fn map_columns(
    batch: RecordBatch,
    f: impl Fn(ArrayRef) -> Result<ArrayRef>,
) -> Result<RecordBatch> {
    let (schema, columns, rows) = batch.into_parts();
    let columns = columns.into_iter().map(f).collect::<Result<_>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
}

/// Writes the batch still buffered in each channel, returning `false` if every channel is closed.
async fn write_buffered(sink: &mut dyn TaskStreamSink, channels: &mut [Channel]) -> Result<bool> {
    let batches = channels
        .iter_mut()
        .map(|channel| {
            channel.coalescer.finish_buffered_batch()?;
            // The buffered batch can be a slice of the last batch pushed, which shrinking
            // cannot release.
            let Some(batch) = channel.coalescer.next_completed_batch() else {
                return Ok(None);
            };
            let batch = map_columns(batch, copy_unless_coalesced)?;
            compact_completed(batch).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    if batches.iter().all(Option::is_none) {
        return Ok(true);
    }
    Ok(sink.write(batches).await? == TaskStreamWriteState::Active)
}

/// Writes the completed batches of all channels, returning `false` once every channel is closed.
async fn write_completed(sink: &mut dyn TaskStreamSink, channels: &mut [Channel]) -> Result<bool> {
    loop {
        let batches: Vec<Option<RecordBatch>> = channels
            .iter_mut()
            .map(|channel| {
                channel
                    .coalescer
                    .next_completed_batch()
                    .map(compact_completed)
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

/// Releases the memory a completed batch holds but does not use.
///
/// A completed batch can end inside a partitioned batch and still hold the view strings,
/// dictionary values, and list view values of rows in the next one.
fn compact_completed(batch: RecordBatch) -> Result<RecordBatch> {
    map_columns(batch, |mut column| {
        // The coalescer reserves a full batch for each column, and copies view strings
        // into blocks that grow from batch to batch. A column shared with another
        // batch cannot be reallocated.
        if let Some(column) = Arc::get_mut(&mut column) {
            column.shrink_to_fit();
        }
        trim(column, false)
    })
}

/// Copies or compacts each partitioned batch of unbounded input, which is queued as it is.
fn compact_unbounded(
    partitions: Vec<Option<RecordBatch>>,
    slices: bool,
) -> Result<Vec<Option<RecordBatch>>> {
    partitions
        .into_iter()
        .map(|batch| {
            batch
                .map(|batch| {
                    map_columns(batch, |column| {
                        if slices || oversized(&column) {
                            copy(&column)
                        } else {
                            trim(column, false)
                        }
                    })
                })
                .transpose()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::panic::AssertUnwindSafe;

    use datafusion::arrow::array::{
        Array, ArrayData, AsArray, BooleanArray, ByteView, DictionaryArray, FixedSizeBinaryArray,
        FixedSizeListArray, GenericListViewArray, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeListArray, ListArray, ListBuilder, ListViewArray, MapArray, OffsetSizeTrait, RunArray,
        StringArray, StringViewArray, StringViewBuilder, StructArray, UInt32Array, UnionArray,
        make_array,
    };
    use datafusion::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use datafusion::arrow::compute::{
        cast, concat, concat_batches, sort_to_indices, take, take_record_batch,
    };
    use datafusion::arrow::datatypes::{
        Field, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef, UnionFields,
    };
    use datafusion::common::ScalarValue;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{PhysicalSortExpr, SplitPoint};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion::prelude::SessionConfig;
    use futures::FutureExt;
    use sail_function::aggregate::array_agg::compact_payload;
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

    /// Batches with string, boolean, and list columns, and view strings too long to inline.
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

    /// Batches whose rows all hash to one channel, with `long` rows of 16 KiB strings at the end.
    fn skewed_input(batches: &[(usize, usize)]) -> Result<Vec<RecordBatch>> {
        let item = Arc::new(Field::new_list_field(DataType::Utf8View, true));
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let string_item = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let run_end_encoded = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8View, true)),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8View, false),
            Field::new("lv", DataType::List(item), false),
            Field::new("d", dictionary, false),
            Field::new("lw", DataType::ListView(string_item.clone()), false),
            Field::new("r", run_end_encoded, false),
        ]));
        batches
            .iter()
            .map(|&(rows, long)| {
                let values: Vec<String> = (0..rows)
                    .map(|row| {
                        if row < rows - long {
                            format!("{row}")
                        } else {
                            format!("{row:0>16384}")
                        }
                    })
                    .collect();
                let k = Int64Array::from_iter_values(std::iter::repeat_n(0, rows));
                let v = StringViewArray::from_iter_values(&values);
                let mut lv = ListBuilder::new(StringViewBuilder::new());
                for value in &values {
                    lv.values().append_value(value);
                    lv.append(true);
                }
                // Few distinct values, so concatenating keeps every dictionary's values.
                let d = (0..rows)
                    .map(|row| {
                        if row < rows - long {
                            format!("{}", row % 4)
                        } else {
                            format!("{:0>16384}", row % 4)
                        }
                    })
                    .collect::<Vec<_>>();
                let d: DictionaryArray<Int32Type> = d.iter().map(String::as_str).collect();
                // One item per list, so only the lengths of the items are skewed.
                let lw = ListViewArray::try_new(
                    string_item.clone(),
                    (0..rows as i32).collect::<Vec<_>>().into(),
                    vec![1; rows].into(),
                    Arc::new(StringArray::from_iter_values(&values)),
                    None,
                )?;
                let run_ends = Int32Array::from_iter_values(1..=rows as i32);
                let r = RunArray::<Int32Type>::try_new(&run_ends, &v)?;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(k),
                        Arc::new(v),
                        Arc::new(lv.finish()),
                        Arc::new(d),
                        Arc::new(lw),
                        Arc::new(r),
                    ],
                )?)
            })
            .collect()
    }

    /// Batches that each have their own dictionary of view strings with narrow keys.
    fn dictionary_input(batches: usize) -> Result<Vec<RecordBatch>> {
        let dictionary =
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8View));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("d", dictionary, false),
        ]));
        (0..batches as i64)
            .map(|i| {
                let a = Int64Array::from_iter_values(i * ROWS..(i + 1) * ROWS);
                let keys = Int16Array::from_iter_values((0..ROWS).map(|row| (row % 2000) as i16));
                let values =
                    StringViewArray::from_iter_values((0..2000).map(|v| format!("{i}-{v:020}")));
                let d = DictionaryArray::<Int16Type>::try_new(keys, Arc::new(values))?;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(a), Arc::new(d)],
                )?)
            })
            .collect()
    }

    /// Batches whose list views overlap within a small part of their values, and whose
    /// null lists point at most of the rest.
    fn shared_list_input(batches: usize) -> Result<Vec<RecordBatch>> {
        fn lists<O: OffsetSizeTrait>(field: &Arc<Field>) -> Result<ArrayRef> {
            let rows = ROWS as usize;
            let nulls = NullBuffer::from_iter((0..rows).map(|row| row % 10 != 0));
            let offsets: Vec<O> = (0..rows)
                .map(|row| O::usize_as(if row % 10 == 0 { 1000 } else { (row % 8) * 8 }))
                .collect();
            let sizes: Vec<O> = (0..rows)
                .map(|row| O::usize_as(if row % 10 == 0 { 60000 } else { 16 }))
                .collect();
            let values = Int64Array::from_iter_values(0..65536);
            Ok(Arc::new(GenericListViewArray::<O>::try_new(
                Arc::clone(field),
                offsets.into(),
                sizes.into(),
                Arc::new(values),
                Some(nulls),
            )?))
        }
        let item = Arc::new(Field::new_list_field(DataType::Int64, false));
        let nested = Field::new("lw", DataType::ListView(item.clone()), true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("lw", DataType::ListView(item.clone()), true),
            Field::new("llw", DataType::LargeListView(item.clone()), true),
            Field::new("s", DataType::Struct(vec![nested.clone()].into()), false),
        ]));
        (0..batches as i64)
            .map(|i| {
                let a = Int64Array::from_iter_values(i * ROWS..(i + 1) * ROWS);
                let s = StructArray::try_new(
                    vec![nested.clone()].into(),
                    vec![lists::<i32>(&item)?],
                    None,
                )?;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(a),
                        lists::<i32>(&item)?,
                        lists::<i64>(&item)?,
                        Arc::new(s),
                    ],
                )?)
            })
            .collect()
    }

    /// Batches whose view strings all point at a few long strings in a much larger buffer.
    fn shared_string_input(batches: usize) -> Result<Vec<RecordBatch>> {
        let item = Arc::new(Field::new("v", DataType::Utf8View, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("v", DataType::Utf8View, false),
            Field::new("s", DataType::Struct(vec![item.clone()].into()), false),
        ]));
        (0..batches as i64)
            .map(|i| {
                let strings =
                    StringViewArray::from_iter_values((0..1000).map(|v| format!("{i}-{v:01024}")));
                let indices = UInt32Array::from_iter_values((0..ROWS as u32).map(|row| row % 4));
                let v = take(&strings, &indices, None)?;
                let a = Int64Array::from_iter_values(i * ROWS..(i + 1) * ROWS);
                let s =
                    StructArray::try_new(vec![item.clone()].into(), vec![Arc::clone(&v)], None)?;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(a), v, Arc::new(s)],
                )?)
            })
            .collect()
    }

    /// Small batches that each have their own dictionary, with keys too narrow to hold
    /// the values of two of them.
    fn narrow_dictionary_input(values: &DataType) -> Result<Vec<RecordBatch>> {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(values.clone()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("d", dictionary, false),
        ]));
        (0..10i64)
            .map(|i| {
                let a = Int64Array::from_iter_values(i * 100..(i + 1) * 100);
                let keys = Int8Array::from_iter_values(0..100);
                let strings = StringArray::from_iter_values((0..100).map(|v| format!("{i}-{v}")));
                let values = cast(&strings, values)?;
                let d = DictionaryArray::<Int8Type>::try_new(keys, values)?;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(a), Arc::new(d)],
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

    /// Checks that each column of a channel's batches keeps at most twice the memory of a
    /// compacted copy of its rows.
    ///
    /// A consumer that falls behind holds on to the batches of one channel
    /// while the others are released, so this must hold for each channel.
    fn assert_compact(channel: usize, output: &[RecordBatch]) -> Result<()> {
        for (i, batch) in output.iter().enumerate() {
            let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
            let copy = take_record_batch(batch, &indices)?;
            for (field, (column, copy)) in batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns().iter().zip(copy.columns()))
            {
                assert_uses_values(field.name(), &column.to_data());
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

    /// Checks that the view strings and list view values nested in a column are used.
    ///
    /// A `take` keeps all of them, so the compacted copy cannot be relied on for these.
    /// Bytes or values that several rows share are counted once.
    fn assert_uses_values(name: &str, data: &ArrayData) {
        let array = make_array(data.clone());
        match data.data_type() {
            DataType::Utf8View => {
                let views = array.as_string_view();
                let held: usize = views.data_buffers().iter().map(Buffer::capacity).sum();
                let ranges = views
                    .views()
                    .iter()
                    .enumerate()
                    .filter(|&(i, &view)| view as u32 > 12 && views.is_valid(i))
                    .map(|(_, &view)| {
                        let view = ByteView::from(view);
                        let start = view.offset as usize;
                        (
                            view.buffer_index as usize,
                            start,
                            start + view.length as usize,
                        )
                    })
                    .collect();
                let used = covered(ranges);
                assert!(
                    held <= 2 * used,
                    "column {name} holds {held} bytes for {used} used"
                );
            }
            // Unused values can be arbitrarily large, so none are allowed.
            DataType::ListView(_) => assert_uses_list_values::<i32>(name, &array),
            DataType::LargeListView(_) => assert_uses_list_values::<i64>(name, &array),
            _ => {}
        }
        for child in data.child_data() {
            assert_uses_values(name, child);
        }
    }

    fn assert_uses_list_values<O: OffsetSizeTrait>(name: &str, array: &ArrayRef) {
        let lists = array.as_list_view::<O>();
        let ranges = (0..lists.len())
            .filter(|&i| lists.is_valid(i))
            .map(|i| {
                let start = lists.offsets()[i].as_usize();
                (0, start, start + lists.sizes()[i].as_usize())
            })
            .collect();
        let used = covered(ranges);
        let held = lists.values().len();
        assert!(
            held <= used,
            "column {name} holds {held} values for {used} used"
        );
    }

    /// Checks that every list view in the data holds at most `limit` values.
    fn assert_list_values_at_most(data: &ArrayData, limit: usize) {
        if matches!(
            data.data_type(),
            DataType::ListView(_) | DataType::LargeListView(_)
        ) {
            let values = data.child_data()[0].len();
            assert!(values <= limit, "list view holds {values} values");
        }
        for child in data.child_data() {
            assert_list_values_at_most(child, limit);
        }
    }

    /// Returns how much the `(buffer, start, end)` ranges cover, counting overlaps once.
    fn covered(mut ranges: Vec<(usize, usize, usize)>) -> usize {
        ranges.sort_unstable();
        let mut total = 0;
        // The buffer and end of the ranges seen so far.
        let mut reach: Option<(usize, usize)> = None;
        for (buffer, start, end) in ranges {
            let reached = reach.and_then(|(b, e)| (b == buffer).then_some(e));
            total += end.saturating_sub(start.max(reached.unwrap_or(0)));
            reach = Some((buffer, end.max(reached.unwrap_or(0))));
        }
        total
    }

    /// Checks that a channel's batches share no memory with the input.
    ///
    /// This only holds when every channel receives a small part of each input batch,
    /// since the coalescer otherwise keeps sharing view payloads it mostly uses.
    fn assert_not_shared(input: &[RecordBatch], channel: usize, output: &[RecordBatch]) {
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
    }

    /// Shuffles the input by `column` and checks the output of every channel.
    async fn assert_shuffle_compact(
        plan: Arc<dyn ExecutionPlan>,
        input: &[RecordBatch],
        column: &str,
        channels: usize,
        skewed: bool,
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
            assert_compact(channel, output)?;
            if !skewed {
                assert_not_shared(input, channel, output);
            }
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
            assert_compact(channel, output)?;
            assert_not_shared(&input, channel, output);
        }
        Ok(())
    }

    /// Each channel's only batch holds part of the single input batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_one_input_batch_does_not_retain_it() -> Result<()> {
        for (input, column) in [(input(1)?, "a"), (nested_input(1, ROWS)?, "s")] {
            let capture =
                assert_shuffle_compact(memory(&input)?, &input, column, CHANNELS, false).await?;
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
            assert_shuffle_compact(stream(&input, false)?, &input, column, 4, false).await?;
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
            assert_shuffle_compact(stream(&input, false)?, &input, column, 4, false).await?;
        }
        Ok(())
    }

    /// The coalescer copies view strings into blocks that grow from batch to batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_many_string_batches_is_compact() -> Result<()> {
        let input = nested_input(32, ROWS)?;
        assert_shuffle_compact(memory(&input)?, &input, "s", 4, false).await?;
        Ok(())
    }

    /// The first output batch ends inside an input batch whose long strings all come later.
    #[tokio::test]
    async fn hash_shuffle_output_ending_inside_a_partitioned_batch_does_not_retain_the_rest()
    -> Result<()> {
        let input = skewed_input(&[(4000, 0), (200, 0), (4096, 32)])?;
        let capture = assert_shuffle_compact(stream(&input, false)?, &input, "k", 2, true).await?;
        assert!(capture.channels.iter().any(|output| output.len() == 2));
        Ok(())
    }

    /// Concatenating the dictionaries of many input batches would overflow their keys.
    #[tokio::test]
    async fn hash_shuffle_output_of_batches_with_their_own_dictionaries_is_compact() -> Result<()> {
        // Each output batch takes rows from 32 input batches, each with 2000 values.
        let input = dictionary_input(40)?;
        let schema = input[0].schema();
        let partitioning = hash(&schema, "a", CHANNELS)?;
        let capture =
            shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
        let expected = expected(&input, vec![col("a", &schema)?], CHANNELS)?;
        // Concatenating the expected batches would overflow too, so the values are decoded.
        let decode = |batches: &[RecordBatch]| -> Result<Vec<ArrayRef>> {
            let columns = batches
                .iter()
                .map(|batch| {
                    let d = cast(batch.column(1), &DataType::Utf8View)?;
                    Ok(vec![Arc::clone(batch.column(0)), d])
                })
                .collect::<Result<Vec<_>>>()?;
            (0..2)
                .map(|i| {
                    let parts: Vec<&dyn Array> = columns.iter().map(|c| c[i].as_ref()).collect();
                    Ok(concat(&parts)?)
                })
                .collect()
        };
        for (channel, (output, expected)) in capture.channels.iter().zip(expected).enumerate() {
            assert_eq!(decode(output)?, decode(&expected)?);
            assert_compact(channel, output)?;
            assert_not_shared(&input, channel, output);
        }
        Ok(())
    }

    /// The coalescer merges a large whole input batch into a small buffered one.
    #[tokio::test]
    async fn round_robin_shuffle_output_of_large_input_batch_after_small_one_does_not_retain_it()
    -> Result<()> {
        let input = [nested_input(1, 100)?, nested_input(1, 4 * ROWS)?].concat();
        let schema = input[0].schema();
        let partitioning = ShufflePartitioning::RoundRobinBatch(1);
        let capture = shuffle_with(
            stream(&input, false)?,
            partitioning,
            TaskStreamWriteState::Active,
        )
        .await?;
        let output = &capture.channels[0];
        assert_eq!(
            concat_batches(&schema, output)?,
            concat_batches(&schema, &input)?
        );
        assert_compact(0, output)?;
        Ok(())
    }

    /// Row round-robin partitioning takes each channel's rows into new batches, which
    /// still share the input's view strings.
    #[tokio::test]
    async fn row_round_robin_shuffle_output_does_not_retain_input_batches() -> Result<()> {
        let input = nested_input(8, ROWS)?;
        let partitioning = ShufflePartitioning::RoundRobinRow(4);
        let capture =
            shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
        let rows: usize = capture
            .channels
            .iter()
            .flatten()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 8 * ROWS as usize);
        for (channel, output) in capture.channels.iter().enumerate() {
            assert_compact(channel, output)?;
        }
        Ok(())
    }

    /// Round-robin partitioning passes on whole input batches, which need no copy.
    #[tokio::test]
    async fn round_robin_shuffle_passes_large_input_batches_through() -> Result<()> {
        let input = input(8)?;
        let partitioning = ShufflePartitioning::RoundRobinBatch(4);
        let capture =
            shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
        let input_ranges = batch_ranges(&input);
        for output in &capture.channels {
            assert_eq!(output.len(), 2);
            assert!(
                batch_ranges(output)
                    .iter()
                    .all(|range| input_ranges.contains(range))
            );
        }
        Ok(())
    }

    /// Copying lists that share values one at a time would multiply the values.
    /// Every valid list uses some of the first 72 values of its input batch, and an output
    /// batch holds rows of at most all four.
    #[tokio::test]
    async fn shuffle_output_of_lists_sharing_values_holds_them_once() -> Result<()> {
        let input = shared_list_input(4)?;
        let schema = input[0].schema();
        for partitioning in [
            hash(&schema, "a", 4)?,
            ShufflePartitioning::RoundRobinRow(4),
            ShufflePartitioning::RoundRobinBatch(2),
        ] {
            let capture =
                shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
            let rows: usize = capture
                .channels
                .iter()
                .flatten()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 4 * ROWS as usize);
            for (channel, output) in capture.channels.iter().enumerate() {
                assert_compact(channel, output)?;
                for batch in output {
                    for column in batch.columns() {
                        assert_list_values_at_most(&column.to_data(), 4 * 72);
                    }
                }
            }
        }
        // The values each valid list sees are unchanged.
        let partitioning = hash(&schema, "a", 4)?;
        let capture =
            shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active).await?;
        let expected = expected(&input, vec![col("a", &schema)?], 4)?;
        let decode = |batches: &[RecordBatch]| -> Result<Vec<Option<Vec<i64>>>> {
            let lists = concat_batches(&schema, batches)?;
            let lists = lists.column(2).as_list_view::<i64>();
            Ok((0..lists.len())
                .map(|i| {
                    lists
                        .is_valid(i)
                        .then(|| lists.value(i).as_primitive::<Int64Type>().values().to_vec())
                })
                .collect())
        };
        for (output, expected) in capture.channels.iter().zip(expected) {
            assert_eq!(decode(output)?, decode(&expected)?);
        }
        Ok(())
    }

    /// Rows that point at the same long strings make a buffer look used when it is not.
    #[tokio::test]
    async fn hash_shuffle_output_of_strings_shared_by_rows_is_compact() -> Result<()> {
        let input = shared_string_input(4)?;
        assert_shuffle_compact(memory(&input)?, &input, "a", 4, true).await?;
        Ok(())
    }

    /// Concatenating the dictionaries of two input batches would overflow their keys.
    #[tokio::test]
    async fn shuffle_output_of_dictionaries_with_narrow_keys_does_not_overflow() -> Result<()> {
        // Arrow merges string values, and panics on overflow for values it cannot merge.
        for values in [DataType::Utf8, DataType::Utf8View] {
            let input = narrow_dictionary_input(&values)?;
            let schema = input[0].schema();
            for partitioning in [
                hash(&schema, "a", 1)?,
                ShufflePartitioning::RoundRobinBatch(1),
                ShufflePartitioning::RoundRobinRow(1),
            ] {
                let capture =
                    shuffle_with(memory(&input)?, partitioning, TaskStreamWriteState::Active)
                        .await?;
                let decode = |batches: &[RecordBatch]| -> Result<Vec<ArrayRef>> {
                    batches
                        .iter()
                        .map(|batch| Ok(cast(batch.column(1), &DataType::Utf8)?))
                        .collect()
                };
                let parts = decode(&capture.channels[0])?;
                let expected = decode(&input)?;
                let concat_all = |parts: &[ArrayRef]| -> Result<ArrayRef> {
                    let parts: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
                    Ok(concat(&parts)?)
                };
                assert_eq!(
                    concat_all(&parts)?.to_data(),
                    concat_all(&expected)?.to_data()
                );
            }
        }
        Ok(())
    }

    /// Input batches can be slices of a much larger batch from upstream.
    #[tokio::test]
    async fn round_robin_shuffle_output_of_sliced_input_does_not_retain_it() -> Result<()> {
        let large = nested_input(1, 16 * ROWS)?.remove(0);
        let small = nested_input(1, 8 * 1000)?.remove(0);
        let input: Vec<RecordBatch> = (0..8)
            .map(|i| large.slice(i * ROWS as usize, ROWS as usize))
            .chain((0..8).map(|i| small.slice(i * 1000, 1000)))
            .collect();
        for unbounded in [false, true] {
            let capture = shuffle_with(
                stream(&input, unbounded)?,
                ShufflePartitioning::RoundRobinBatch(4),
                TaskStreamWriteState::Active,
            )
            .await?;
            let rows: usize = capture
                .channels
                .iter()
                .flatten()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 8 * (ROWS as usize + 1000));
            for (channel, output) in capture.channels.iter().enumerate() {
                assert_compact(channel, output)?;
            }
        }
        Ok(())
    }

    /// Unbounded input is queued without coalescing, as slices of each input batch.
    #[tokio::test]
    async fn hash_shuffle_output_of_unbounded_input_does_not_retain_it() -> Result<()> {
        let input = nested_input(4, ROWS)?;
        let capture =
            assert_shuffle_compact(stream(&input, true)?, &input, "s", CHANNELS, false).await?;
        assert_eq!(capture.writes, input.len());
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

    /// The column types that the shuffle matrix checks.
    const MATRIX_KINDS: &[&str] = &[
        "int64",
        "boolean",
        "utf8",
        "large_binary",
        "utf8_view",
        "binary_view",
        "fixed_size_binary",
        "dictionary",
        "narrow_dictionary",
        "narrow_view_dictionary",
        "list",
        "nullable_list",
        "large_list",
        "list_view",
        "shared_list_view",
        "large_list_view",
        "fixed_size_list",
        "struct",
        "map",
        "dense_union",
        "sparse_union",
        "run_end",
        "nested_list",
        "list_of_dictionary",
        "struct_of_list_view",
        "list_view_of_narrow_dictionary",
    ];

    const MATRIX_BATCH_ROWS: usize = 512;

    /// Returns a string for `id` that views hold out of line, and a large one if `heavy`.
    fn matrix_text(id: usize, heavy: bool) -> String {
        if heavy {
            format!("{id:0>1000}")
        } else {
            format!("value-{id:020}")
        }
    }

    fn matrix_strings(count: usize, base: usize, heavy: &dyn Fn(usize) -> bool) -> StringArray {
        (0..count)
            .map(|j| Some(matrix_text(base + j, heavy(j))))
            .collect()
    }

    fn matrix_views(count: usize, base: usize, heavy: &dyn Fn(usize) -> bool) -> Result<ArrayRef> {
        Ok(cast(
            &matrix_strings(count, base, heavy),
            &DataType::Utf8View,
        )?)
    }

    fn matrix_list(values: ArrayRef, rows: usize, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
        let per_row = values.len() / rows;
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
        Ok(Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::from_lengths(std::iter::repeat_n(per_row, rows)),
            values,
            nulls,
        )?))
    }

    /// Returns list views of two values each, starting `step` values apart.
    fn matrix_list_view<O: OffsetSizeTrait>(
        values: ArrayRef,
        rows: usize,
        step: usize,
    ) -> Result<ArrayRef> {
        let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
        Ok(Arc::new(GenericListViewArray::<O>::try_new(
            field,
            (0..rows).map(|i| O::usize_as(i * step)).collect(),
            std::iter::repeat_n(O::usize_as(2), rows).collect(),
            values,
            None,
        )?))
    }

    /// Returns a dictionary with Int8 keys over 100 values, which differ from batch to batch.
    fn matrix_narrow_dictionary(count: usize, base: usize, views: bool) -> Result<ArrayRef> {
        let keys = Int8Array::from_iter_values((0..count).map(|i| (i % 100) as i8));
        let values: ArrayRef = if views {
            matrix_views(100, base, &|_| false)?
        } else {
            Arc::new(matrix_strings(100, base, &|_| false))
        };
        Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys, values,
        )?))
    }

    /// Returns a column of `kind` whose rows hold large values where `heavy` is set.
    fn matrix_column(
        kind: &str,
        rows: usize,
        base: usize,
        heavy: &dyn Fn(usize) -> bool,
    ) -> Result<ArrayRef> {
        let pairs = |j: usize| heavy(j / 2);
        Ok(match kind {
            "int64" => Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (base + i) as i64),
            )),
            "boolean" => Arc::new(BooleanArray::from_iter(
                (0..rows).map(|i| Some((base + i).is_multiple_of(3))),
            )),
            "utf8" => Arc::new(matrix_strings(rows, base, heavy)),
            "large_binary" => cast(&matrix_strings(rows, base, heavy), &DataType::LargeBinary)?,
            "utf8_view" => matrix_views(rows, base, heavy)?,
            "binary_view" => cast(&matrix_strings(rows, base, heavy), &DataType::BinaryView)?,
            "fixed_size_binary" => Arc::new(FixedSizeBinaryArray::try_from_iter(
                (0..rows).map(|i| (base + i).to_le_bytes()),
            )?),
            "dictionary" => Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..rows as i32),
                Arc::new(matrix_strings(rows, base, heavy)),
            )?),
            "narrow_dictionary" => matrix_narrow_dictionary(rows, base, false)?,
            "narrow_view_dictionary" => matrix_narrow_dictionary(rows, base, true)?,
            "list" => matrix_list(matrix_views(2 * rows, 2 * base, &pairs)?, rows, None)?,
            "nullable_list" => matrix_list(
                matrix_views(2 * rows, 2 * base, &pairs)?,
                rows,
                // Values under null rows are a known limit, so these are small.
                Some(NullBuffer::from_iter(
                    (0..rows).map(|i| i % 5 != 0 || heavy(i)),
                )),
            )?,
            "large_list" => {
                let values = Int64Array::from_iter_values((0..3 * rows).map(|j| j as i64));
                Arc::new(LargeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Int64, true)),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(3, rows)),
                    Arc::new(values),
                    None,
                )?)
            }
            "list_view" => {
                matrix_list_view::<i32>(matrix_views(2 * rows, 2 * base, &pairs)?, rows, 2)?
            }
            "shared_list_view" => {
                matrix_list_view::<i32>(matrix_views(rows + 1, base, heavy)?, rows, 1)?
            }
            "large_list_view" => {
                matrix_list_view::<i64>(matrix_views(2 * rows, 2 * base, &pairs)?, rows, 2)?
            }
            "fixed_size_list" => {
                let values = matrix_views(2 * rows, 2 * base, &pairs)?;
                Arc::new(FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Utf8View, true)),
                    2,
                    values,
                    None,
                )?)
            }
            "struct" => Arc::new(StructArray::try_from(vec![
                ("a", matrix_views(rows, base, heavy)?),
                (
                    "b",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                ("c", Arc::new(matrix_strings(rows, base, heavy)) as ArrayRef),
            ])?),
            "map" => {
                let keys: ArrayRef = Arc::new(StringArray::from_iter_values(
                    (0..2 * rows).map(|j| format!("key-{}", j % 2)),
                ));
                let values = matrix_views(2 * rows, 2 * base, &pairs)?;
                let entries = StructArray::try_new(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8View, true),
                    ]
                    .into(),
                    vec![keys, values],
                    None,
                )?;
                Arc::new(MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(2, rows)),
                    entries,
                    None,
                    false,
                )?)
            }
            "dense_union" | "sparse_union" => {
                let fields = UnionFields::try_new(
                    [0, 1],
                    [
                        Field::new("i", DataType::Int64, true),
                        Field::new("s", DataType::Utf8View, true),
                    ],
                )?;
                let type_ids = (0..rows).map(|i| (i % 2) as i8).collect();
                if kind == "dense_union" {
                    let ints =
                        Int64Array::from_iter_values((0..rows.div_ceil(2)).map(|j| j as i64));
                    let strings = matrix_views(rows / 2, base, &|j| heavy(2 * j + 1))?;
                    let offsets = (0..rows).map(|i| (i / 2) as i32).collect();
                    Arc::new(UnionArray::try_new(
                        fields,
                        type_ids,
                        Some(offsets),
                        vec![Arc::new(ints), strings],
                    )?)
                } else {
                    let ints = Int64Array::from_iter_values((0..rows).map(|j| j as i64));
                    let strings = matrix_views(rows, base, heavy)?;
                    Arc::new(UnionArray::try_new(
                        fields,
                        type_ids,
                        None,
                        vec![Arc::new(ints), strings],
                    )?)
                }
            }
            "run_end" => {
                let runs = rows.div_ceil(4);
                let run_ends =
                    Int32Array::from_iter_values((1..=runs).map(|r| (4 * r).min(rows) as i32));
                let values = matrix_views(runs, base, &|r| heavy(4 * r))?;
                Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values)?)
            }
            "nested_list" => {
                let inner = matrix_list(
                    matrix_views(4 * rows, 4 * base, &|j| heavy(j / 4))?,
                    2 * rows,
                    None,
                )?;
                matrix_list(inner, rows, None)?
            }
            "list_of_dictionary" => {
                let dictionary = DictionaryArray::<Int16Type>::try_new(
                    Int16Array::from_iter_values(0..2 * rows as i16),
                    Arc::new(matrix_strings(2 * rows, 2 * base, &pairs)),
                )?;
                matrix_list(Arc::new(dictionary), rows, None)?
            }
            "struct_of_list_view" => Arc::new(StructArray::try_from(vec![(
                "l",
                matrix_list_view::<i32>(matrix_views(2 * rows, 2 * base, &pairs)?, rows, 2)?,
            )])?),
            "list_view_of_narrow_dictionary" => {
                matrix_list_view::<i32>(matrix_narrow_dictionary(2 * rows, base, true)?, rows, 2)?
            }
            _ => unreachable!("unknown column kind {kind}"),
        })
    }

    /// Returns batches of the given sizes with a unique key and a column of `kind`.
    ///
    /// Skewed batches have a few large rows. Sliced and tail batches are slices of a batch
    /// whose rows outside the slice are large, covering a quarter or most of it.
    fn matrix_input(kind: &str, shape: &str, sizes: &[usize]) -> Result<Vec<RecordBatch>> {
        let mut base = 0;
        let mut batches = vec![];
        for &rows in sizes {
            let (full, heavy): (usize, Box<dyn Fn(usize) -> bool>) = match shape {
                "whole" => (rows, Box::new(|_| false)),
                "skewed" => (rows, Box::new(|i| i.is_multiple_of(97))),
                "tail" => (rows + rows / 8, Box::new(move |i| i >= rows)),
                _ => (4 * rows, Box::new(move |i| i < rows || i >= 2 * rows)),
            };
            let keys = Int64Array::from_iter_values((base..base + full).map(|k| k as i64));
            let column = matrix_column(kind, full, base, heavy.as_ref())?;
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", column.data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(keys), column])?;
            batches.push(match shape {
                "sliced" => batch.slice(rows, rows),
                "tail" => batch.slice(0, rows),
                _ => batch,
            });
            base += full;
        }
        Ok(batches)
    }

    /// Returns a type with its dictionaries replaced by their value types.
    fn decoded(data_type: &DataType) -> DataType {
        let field = |field: &Arc<Field>| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(decoded(field.data_type())),
            )
        };
        match data_type {
            DataType::Dictionary(_, values) => decoded(values),
            DataType::List(f) => DataType::List(field(f)),
            DataType::LargeList(f) => DataType::LargeList(field(f)),
            DataType::ListView(f) => DataType::ListView(field(f)),
            DataType::LargeListView(f) => DataType::LargeListView(field(f)),
            other => other.clone(),
        }
    }

    /// Concatenates batches in key order, decoding dictionaries so that they can be merged.
    fn matrix_rows(batches: &[RecordBatch]) -> Result<RecordBatch> {
        let schema = batches[0].schema();
        let decoded_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_data_type(decoded(f.data_type())))
                .collect::<Vec<_>>(),
        ));
        let batches = batches
            .iter()
            .map(|batch| {
                let columns = batch
                    .columns()
                    .iter()
                    .zip(decoded_schema.fields())
                    .map(|(column, field)| cast(column, field.data_type()))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(RecordBatch::try_new(Arc::clone(&decoded_schema), columns)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let all = concat_batches(&decoded_schema, &batches)?;
        let indices = sort_to_indices(all.column(0), None, None)?;
        Ok(take_record_batch(&all, &indices)?)
    }

    async fn check_matrix_case(
        input: &[RecordBatch],
        scheme: &str,
        channels: usize,
        unbounded: bool,
    ) -> Result<()> {
        let schema = input[0].schema();
        let key = col("k", &schema)?;
        let partitioning = match scheme {
            "hash" => ShufflePartitioning::Hash(vec![key], channels),
            "range" => {
                let mut keys: Vec<i64> = input
                    .iter()
                    .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
                    .collect();
                keys.sort_unstable();
                let points = (1..channels)
                    .map(|i| {
                        SplitPoint::new(vec![ScalarValue::Int64(Some(
                            keys[i * keys.len() / channels],
                        ))])
                    })
                    .collect();
                ShufflePartitioning::Range(RangePartitioning::try_new(
                    [PhysicalSortExpr::new_default(key)].into(),
                    points,
                )?)
            }
            "round_robin_batch" => ShufflePartitioning::RoundRobinBatch(channels),
            _ => ShufflePartitioning::RoundRobinRow(channels),
        };
        let capture = Arc::new(Mutex::new(Capture::default()));
        let writer = Arc::new(CaptureWriter {
            capture: Arc::clone(&capture),
            state: TaskStreamWriteState::Active,
        });
        let exec = ShuffleWriteExec::new(stream(input, unbounded)?, writer, partitioning);
        let context = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(MATRIX_BATCH_ROWS));
        collect(exec.execute(0, Arc::new(context))?).await?;
        let capture = std::mem::take(&mut *capture.lock().await);
        let output: Vec<RecordBatch> = capture.channels.iter().flatten().cloned().collect();
        if matrix_rows(&output)? != matrix_rows(input)? {
            return plan_err!("output rows differ from the input");
        }
        for (channel, output) in capture.channels.iter().enumerate() {
            assert_compact(channel, output)?;
        }
        Ok(())
    }

    /// Checks every column type through every partitioning scheme and coalescing path.
    #[tokio::test]
    async fn shuffle_output_is_compact_for_every_type_and_path() -> Result<()> {
        let sizes: [&[usize]; 3] = [
            &[60; 10],
            &[60, 300, 90, 700, 40, 1500, 200, 520, 30, 260],
            &[1500, 2600, 1100],
        ];
        let mut failures = vec![];
        for kind in MATRIX_KINDS {
            for shape in ["whole", "skewed", "sliced", "tail"] {
                for (s, sizes) in sizes.iter().enumerate() {
                    let input = matrix_input(kind, shape, sizes)?;
                    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
                        for channels in [1, 3] {
                            for unbounded in [false, true] {
                                let case = check_matrix_case(&input, scheme, channels, unbounded);
                                let error = match AssertUnwindSafe(case).catch_unwind().await {
                                    Ok(Ok(())) => continue,
                                    Ok(Err(error)) => error.to_string(),
                                    Err(panic) => panic
                                        .downcast_ref::<String>()
                                        .cloned()
                                        .or_else(|| {
                                            panic.downcast_ref::<&str>().map(|s| s.to_string())
                                        })
                                        .unwrap_or_default(),
                                };
                                failures.push(format!(
                                    "{kind} {shape} sizes{s} {scheme}({channels}) unbounded={unbounded}: {}",
                                    error.lines().next().unwrap_or_default()
                                ));
                            }
                        }
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} cases failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
        Ok(())
    }
}
