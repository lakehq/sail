use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{Schema, UInt64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, RangePartitioning,
};
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::{REPARTITION_RANDOM_STATE, RangeExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, apply_expression_roots,
};
use futures::StreamExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::shuffle::ShuffleWriteConfig;

use crate::plan::shuffle_buffer::{Round, ShuffleBuffer};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState, TaskStreamWriter};

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

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L940-L1010
/// Computes `value % divisor` without division in the hot loop when `divisor`
/// is fixed for many values.
///
/// Hash repartitioning computes a remainder for every row. Integer division is
/// relatively expensive, so this precomputes the strength-reduced form of the
/// divisor: powers of two use a bit mask, and other divisors use a reciprocal
/// multiply to recover the quotient and therefore the remainder. This is the
/// same invariant-divisor optimization compilers use for `%` by a constant.
#[derive(Debug, Clone, Copy)]
enum StrengthReducedU64 {
    PowerOfTwo { mask: u64 },
    Reciprocal { divisor: u64, reciprocal: u128 },
}

impl StrengthReducedU64 {
    fn new(divisor: u64) -> Self {
        debug_assert!(divisor > 0);

        if divisor.is_power_of_two() {
            Self::PowerOfTwo { mask: divisor - 1 }
        } else {
            Self::Reciprocal {
                divisor,
                // ceil(2^128 / divisor), computed without representing 2^128
                reciprocal: u128::MAX / u128::from(divisor) + 1,
            }
        }
    }

    fn partition_indices(self, hash_buffer: &[u64], indices: &mut [Vec<u32>]) {
        match self {
            Self::PowerOfTwo { mask } => {
                for (index, hash) in hash_buffer.iter().enumerate() {
                    indices[(*hash & mask) as usize].push(index as u32);
                }
            }
            Self::Reciprocal {
                divisor,
                reciprocal,
            } => {
                for (index, hash) in hash_buffer.iter().enumerate() {
                    let quotient = Self::quotient(*hash, reciprocal);
                    let partition = *hash - quotient * divisor;
                    indices[partition as usize].push(index as u32);
                }
            }
        }
    }

    #[cfg(test)]
    fn remainder(self, value: u64) -> u64 {
        match self {
            Self::PowerOfTwo { mask } => value & mask,
            Self::Reciprocal {
                divisor,
                reciprocal,
            } => value - Self::quotient(value, reciprocal) * divisor,
        }
    }

    #[inline]
    fn quotient(value: u64, reciprocal: u128) -> u64 {
        let reciprocal_low = reciprocal as u64;
        let reciprocal_high = (reciprocal >> 64) as u64;
        let low_product = u128::from(value) * u128::from(reciprocal_low);
        let high_product = u128::from(value) * u128::from(reciprocal_high);
        let carry = ((high_product & u128::from(u64::MAX)) + (low_product >> 64)) >> 64;

        ((high_product >> 64) + carry) as u64
    }
}
// SPDX-SnippetEnd

/// Assigns the rows of the input to output channels, in the same way as DataFusion's
/// `BatchPartitioner` and as `RowRoundRobinPartitioner`, without moving any row.
enum Router {
    /// Each batch goes to one channel as a whole, one channel after the other.
    Batch {
        channels: usize,
        next: usize,
        /// The hash expressions of a single channel, which are still evaluated.
        expressions: Vec<Arc<dyn PhysicalExpr>>,
    },
    Hash {
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        channels: usize,
        partition_reducer: StrengthReducedU64,
        hash_buffer: Vec<u64>,
    },
    Range(RangeExpr),
    RoundRobinRow {
        channels: usize,
        next: usize,
    },
}

impl Router {
    fn try_new(
        partitioning: &ShufflePartitioning,
        partition: usize,
        num_input_partitions: usize,
    ) -> Result<Self> {
        let router = match partitioning {
            ShufflePartitioning::Hash(_, 0) => {
                return internal_err!("Hash repartition requires at least one partition");
            }
            ShufflePartitioning::Hash(exprs, channels) => Self::Hash {
                exprs: exprs.clone(),
                channels: *channels,
                partition_reducer: StrengthReducedU64::new(*channels as u64),
                hash_buffer: vec![],
            },
            ShufflePartitioning::Range(range) if range.split_points().is_empty() => Self::Batch {
                channels: 1,
                next: 0,
                expressions: vec![],
            },
            ShufflePartitioning::Range(range) => {
                let keys = range
                    .ordering()
                    .iter()
                    .map(|sort_expr| Arc::clone(&sort_expr.expr))
                    .collect();
                // TODO: Uncomment after DataFusion 56, and pass the input schema to the router.
                // Self::Range(RangeExpr::try_new_with_schema(keys, range, schema)?)
                Self::Range(RangeExpr::try_new(keys, range)?)
            }
            // SPDX-SnippetBegin
            // SPDX-License-Identifier: Apache-2.0
            // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L1056-L1069
            ShufflePartitioning::RoundRobinBatch(channels) => Self::Batch {
                channels: *channels,
                next: (partition * channels) / num_input_partitions,
                expressions: vec![],
            },
            // SPDX-SnippetEnd
            ShufflePartitioning::RoundRobinRow(channels) => {
                if *channels == 0 {
                    return internal_err!(
                        "round-robin repartition requires at least one output partition"
                    );
                }
                if num_input_partitions == 0 {
                    return internal_err!(
                        "round-robin repartition requires at least one input partition"
                    );
                }
                Self::RoundRobinRow {
                    channels: *channels,
                    next: (partition * channels) / num_input_partitions,
                }
            }
        };
        // With one channel there is nothing to compute for the rows.
        Ok(match router {
            Self::Hash {
                exprs, channels: 1, ..
            } => Self::Batch {
                channels: 1,
                next: 0,
                expressions: exprs,
            },
            Self::RoundRobinRow { channels: 1, .. } => Self::Batch {
                channels: 1,
                next: 0,
                expressions: vec![],
            },
            router => router,
        })
    }

    /// Holds a batch in the buffer by the channels of its rows, and returns what is to
    /// be written. `indices` has the rows of each channel, and is left empty.
    fn push(
        &mut self,
        batch: RecordBatch,
        indices: &mut Vec<Vec<u32>>,
        buffer: &mut ShuffleBuffer,
        flush: bool,
    ) -> Result<Vec<Round>> {
        match self {
            // SPDX-SnippetBegin
            // SPDX-License-Identifier: Apache-2.0
            // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L1241-L1248
            Self::Batch {
                channels,
                next,
                expressions,
            } => {
                evaluate_expressions_to_arrays(expressions.iter(), &batch)?;
                let channel = *next;
                *next = (*next + 1) % *channels;
                return buffer.push_batch(channel, batch, flush);
            }
            // SPDX-SnippetEnd

            // SPDX-SnippetBegin
            // SPDX-License-Identifier: Apache-2.0
            // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L1249-L1274
            Self::Hash {
                exprs,
                channels,
                partition_reducer,
                hash_buffer,
            } => {
                let arrays = evaluate_expressions_to_arrays(exprs.as_slice(), &batch)?;

                hash_buffer.clear();
                hash_buffer.resize(batch.num_rows(), 0);

                create_hashes(
                    &arrays,
                    REPARTITION_RANDOM_STATE.random_state(),
                    hash_buffer,
                )?;

                indices.resize(*channels, vec![]);

                partition_reducer.partition_indices(hash_buffer, indices);
            }
            // SPDX-SnippetEnd
            Self::Range(expression) => {
                let channels = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
                let channels = channels.as_primitive::<UInt64Type>();
                indices.resize(expression.split_points().len() + 1, vec![]);
                for (row, channel) in channels.values().iter().enumerate() {
                    indices[*channel as usize].push(row as u32);
                }
            }
            Self::RoundRobinRow { channels, next } => {
                indices.resize(*channels, vec![]);
                let mut channel = *next;
                for row in 0..batch.num_rows() {
                    indices[channel].push(row as u32);
                    channel += 1;
                    if channel == *channels {
                        channel = 0;
                    }
                }
                *next = channel;
            }
        }
        buffer.push_rows(batch, indices, flush)
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
        let buffer_size = context.extension::<ShuffleWriteConfig>()?.buffer_size();
        let stream = self.plan.execute(partition, context)?;
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let router = Router::try_new(&self.partitioning, partition, num_input_partitions)?;
        let empty = RecordBatch::new_empty(self.schema());
        let channels = self.partitioning.partition_count();
        let buffer = ShuffleBuffer::try_new(
            &stream.schema(),
            channels,
            batch_size,
            buffer_size,
            matches!(router, Router::Batch { .. }),
        )?;
        // The rows of unbounded input are written as they arrive.
        let unbounded = self.plan.properties().boundedness.is_unbounded();
        let output = futures::stream::once(async move {
            shuffle_write(
                writer, stream, partition, channels, router, buffer, unbounded,
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
    mut router: Router,
    mut buffer: ShuffleBuffer,
    unbounded: bool,
) -> Result<()> {
    let mut sink = writer.open(partition).await?;
    let result = async {
        let mut rows = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let rounds = router.push(batch, &mut rows, &mut buffer, unbounded)?;
            if !write_rounds(sink.as_mut(), rounds, channels).await? {
                return Ok::<_, datafusion::error::DataFusionError>(false);
            }
        }
        write_rounds(sink.as_mut(), buffer.flush()?, channels).await
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

/// Writes each round to the sink, returning `false` once every channel is closed.
async fn write_rounds(
    sink: &mut dyn TaskStreamSink,
    rounds: Vec<Round>,
    channels: usize,
) -> Result<bool> {
    for round in rounds {
        let mut batches = vec![None; channels];
        for (channel, batch) in round {
            batches[channel] = Some(batch);
        }
        if sink.write(batches).await? == TaskStreamWriteState::Closed {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::panic::AssertUnwindSafe;

    use datafusion::arrow::array::{
        Array, ArrayData, ArrayRef, AsArray, BooleanArray, ByteView, DictionaryArray,
        FixedSizeBinaryArray, FixedSizeListArray, GenericListViewArray, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeListArray, ListArray, ListBuilder, ListViewArray, MapArray,
        NullArray, OffsetSizeTrait, RunArray, StringArray, StringViewArray, StringViewBuilder,
        StructArray, UInt32Array, UnionArray, make_array,
    };
    use datafusion::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use datafusion::arrow::compute::{
        cast, concat, concat_batches, sort_to_indices, take, take_record_batch,
    };
    use datafusion::arrow::datatypes::{
        DataType, Field, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef, UnionFields,
    };
    use datafusion::arrow::record_batch::RecordBatchOptions;
    use datafusion::common::ScalarValue;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{PhysicalSortExpr, SplitPoint};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::repartition::BatchPartitioner;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion::prelude::SessionConfig;
    use futures::FutureExt;
    use sail_function::aggregate::array_agg::compact_payload;
    use tokio::sync::Mutex;

    use super::*;

    const CHANNELS: usize = 32;
    const ROWS: i64 = 8192;

    /// The bytes that the shuffle writes of these tests hold before they write every channel.
    const BUFFER_SIZE: usize = 32 * 1024 * 1024;

    fn session_config() -> SessionConfig {
        SessionConfig::new().with_extension(Arc::new(ShuffleWriteConfig::new(BUFFER_SIZE)))
    }

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
        let context = TaskContext::default().with_session_config(session_config());
        collect(exec.execute(0, Arc::new(context))?).await?;
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
    /// since a channel otherwise keeps sharing view payloads that it mostly uses.
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

    /// Each channel is written with the rows of many input batches at once.
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

    /// A large whole input batch arrives while a small one is held.
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

    /// Each batch of unbounded input is written before the next one is read.
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
            assert!(!actual.is_empty());
            assert!(
                actual
                    .iter()
                    .all(|b| b.num_rows() > 0 && b.num_rows() <= batch_size)
            );
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
            .with_session_config(session_config().with_batch_size(MATRIX_BATCH_ROWS));
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

    /// Checks every column type through every partitioning scheme and path of the write.
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

    /// Rows that point at the same long strings are not given a copy of them each.
    #[tokio::test]
    async fn hash_shuffle_output_of_strings_shared_by_rows_holds_them_once() -> Result<()> {
        let input = shared_string_input(4)?;
        let capture = assert_shuffle_compact(memory(&input)?, &input, "a", CHANNELS, true).await?;
        let strings = |batches: &[RecordBatch]| -> usize {
            batches
                .iter()
                .flat_map(|batch| batch.column(1).as_string_view().data_buffers())
                .map(Buffer::len)
                .sum()
        };
        let output: Vec<RecordBatch> = capture.channels.into_iter().flatten().collect();
        assert!(strings(&output) <= strings(&input));
        Ok(())
    }

    /// The strings of every input batch are in one buffer that is larger than the budget
    /// of the write, which does not make each batch fill the budget.
    #[tokio::test]
    async fn hash_shuffle_holds_input_batches_that_share_a_large_buffer() -> Result<()> {
        // Twice the megabytes of the budget.
        const BATCHES: usize = 2 * BUFFER_SIZE / (BATCH_ROWS * LEN);
        const BATCH_ROWS: usize = 1024;
        const LEN: usize = 1024;
        let text: String = (0..BATCHES * BATCH_ROWS)
            .map(|i| format!("{i:0>LEN$}"))
            .collect();
        let mut strings = StringViewBuilder::new();
        let block = strings.append_block(Buffer::from(text.into_bytes()));
        for i in 0..BATCHES * BATCH_ROWS {
            strings.try_append_view(block, (i * LEN) as u32, LEN as u32)?;
        }
        let strings: ArrayRef = Arc::new(strings.finish());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("v", DataType::Utf8View, false),
        ]));
        let input = (0..BATCHES)
            .map(|i| {
                let rows = i * BATCH_ROWS..(i + 1) * BATCH_ROWS;
                let a = Int64Array::from_iter_values(rows.map(|row| row as i64));
                let v = strings.slice(i * BATCH_ROWS, BATCH_ROWS);
                Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(a), v])?)
            })
            .collect::<Result<Vec<_>>>()?;
        let capture = assert_shuffle_compact(stream(&input, false)?, &input, "a", 4, false).await?;
        // The batches are held by the megabyte of strings that each of them uses.
        assert!(
            (2..BATCHES / 2).contains(&capture.writes),
            "{} writes",
            capture.writes
        );
        Ok(())
    }

    /// The rows are held up to the buffer size of the session.
    #[tokio::test]
    async fn hash_shuffle_holds_rows_up_to_the_buffer_size_of_the_session() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let input = (0..8)
            .map(|i| {
                let a = Int64Array::from_iter_values(i * 1000..(i + 1) * 1000);
                Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?)
            })
            .collect::<Result<Vec<_>>>()?;
        // The rows of each input batch take 8000 bytes.
        for (buffer_size, writes) in [(BUFFER_SIZE, 1), (0, 1), (16000, 4), (1, 8)] {
            let capture = Arc::new(Mutex::new(Capture::default()));
            let writer = Arc::new(CaptureWriter {
                capture: Arc::clone(&capture),
                state: TaskStreamWriteState::Active,
            });
            let partitioning = hash(&schema, "a", CHANNELS)?;
            let exec = ShuffleWriteExec::new(stream(&input, false)?, writer, partitioning);
            let config =
                SessionConfig::new().with_extension(Arc::new(ShuffleWriteConfig::new(buffer_size)));
            let context = TaskContext::default().with_session_config(config);
            collect(exec.execute(0, Arc::new(context))?).await?;
            assert_eq!(capture.lock().await.writes, writes, "{buffer_size}");
            // The buffer size is required of the session.
            assert!(exec.execute(0, Arc::new(TaskContext::default())).is_err());
        }
        Ok(())
    }

    /// An input batch larger than the budget of the write is written apart from the rows
    /// held before it. The string offsets of an output batch then overflow only if those
    /// of an input batch do.
    #[tokio::test]
    async fn hash_shuffle_writes_an_input_batch_above_the_budget_apart() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = |rows: usize, len: usize| -> Result<RecordBatch> {
            let k = Int64Array::from_iter_values(std::iter::repeat_n(0, rows));
            let s = StringArray::from_iter_values((0..rows).map(|row| format!("{row:0>len$}")));
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(k), Arc::new(s)],
            )?)
        };
        // 1 MiB of strings, more of them than the budget, and 1 MiB, all for one channel.
        let len = BUFFER_SIZE / 3000 + 1024;
        let input = vec![batch(1000, 1024)?, batch(3000, len)?, batch(1000, 1024)?];
        let capture = assert_shuffle_compact(stream(&input, false)?, &input, "k", 2, true).await?;
        let rows: Vec<usize> = capture
            .channels
            .iter()
            .flatten()
            .map(|batch| batch.num_rows())
            .collect();
        assert_eq!(rows, [1000, 3000, 1000]);
        Ok(())
    }

    /// The rows of a slice of a larger batch keep its strings and list values alive, which
    /// does not make each input batch fill the budget of the write.
    #[tokio::test]
    async fn hash_shuffle_holds_input_batches_that_are_slices_of_larger_ones() -> Result<()> {
        for kind in [
            "utf8_view",
            "nullable_list",
            "struct_of_list_view",
            "dictionary",
        ] {
            // Each batch is a quarter of one that has several megabytes of strings.
            let input = matrix_input(kind, "sliced", &[2000; 4])?;
            let partitioning = hash(&input[0].schema(), "k", 13)?;
            let capture = shuffle_with(
                stream(&input, false)?,
                partitioning,
                TaskStreamWriteState::Active,
            )
            .await?;
            assert_eq!(capture.writes, 1, "{kind}");
            let output: Vec<RecordBatch> = capture.channels.into_iter().flatten().collect();
            assert_eq!(matrix_rows(&output)?, matrix_rows(&input)?, "{kind}");
        }
        Ok(())
    }

    /// Small batches that are slices of one large batch are combined, although each of
    /// them keeps the whole of it alive.
    #[tokio::test]
    async fn shuffle_combines_small_whole_batches_that_are_slices_of_a_larger_one() -> Result<()> {
        let large = input_with_rows(1, 2 * 1024 * 1024)?.remove(0);
        let input: Vec<RecordBatch> = (0..16).map(|i| large.slice(i * 1000, 1000)).collect();
        let schema = large.schema();
        for partitioning in [
            ShufflePartitioning::RoundRobinBatch(2),
            hash(&schema, "a", 1)?,
        ] {
            let capture = shuffle_with(
                stream(&input, false)?,
                partitioning,
                TaskStreamWriteState::Active,
            )
            .await?;
            let output: Vec<RecordBatch> = capture.channels.iter().flatten().cloned().collect();
            assert!(output.len() <= 4, "{} batches", output.len());
            assert_eq!(matrix_rows(&output)?, matrix_rows(&input)?);
            for (channel, output) in capture.channels.iter().enumerate() {
                assert_compact(channel, output)?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn hash_shuffle_output_matches_batch_partitioner_for_any_channel_count() -> Result<()> {
        let input = input(4)?;
        for channels in [2, 3, 7, 13, 97, 1009] {
            assert_shuffle_compact(memory(&input)?, &input, "a", channels, true).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn shuffle_writes_the_rows_of_batches_without_columns() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batch = |rows: usize| -> Result<RecordBatch> {
            let options = RecordBatchOptions::new().with_row_count(Some(rows));
            Ok(RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &options,
            )?)
        };
        let input = vec![batch(10)?, batch(5000)?, batch(7)?];
        for (partitioning, expected) in [
            (ShufflePartitioning::RoundRobinBatch(3), [10, 5000, 7]),
            (ShufflePartitioning::RoundRobinRow(3), [1673, 1672, 1672]),
        ] {
            for unbounded in [false, true] {
                let capture = shuffle_with(
                    stream(&input, unbounded)?,
                    partitioning.clone(),
                    TaskStreamWriteState::Active,
                )
                .await?;
                let rows: Vec<usize> = capture
                    .channels
                    .iter()
                    .map(|output| output.iter().map(|batch| batch.num_rows()).sum())
                    .collect();
                assert_eq!(rows, expected);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn shuffle_writes_null_columns() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("n", DataType::Null, true),
        ]));
        let mut base = 0;
        let input = [100, 5000, 3]
            .into_iter()
            .map(|rows| {
                let k = Int64Array::from_iter_values(base..base + rows as i64);
                base += rows as i64;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(k), Arc::new(NullArray::new(rows))],
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        for partitioning in [
            hash(&schema, "k", 3)?,
            ShufflePartitioning::RoundRobinRow(3),
            ShufflePartitioning::RoundRobinBatch(2),
        ] {
            for unbounded in [false, true] {
                let capture = shuffle_with(
                    stream(&input, unbounded)?,
                    partitioning.clone(),
                    TaskStreamWriteState::Active,
                )
                .await?;
                let output: Vec<RecordBatch> = capture.channels.into_iter().flatten().collect();
                assert_eq!(matrix_rows(&output)?, matrix_rows(&input)?);
            }
        }
        Ok(())
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L2745-L2819
    #[test]
    fn strength_reduced_u64_remainder_matches_modulo() {
        let divisors = [
            1,
            2,
            3,
            4,
            5,
            7,
            8,
            10,
            16,
            31,
            32,
            63,
            64,
            65,
            97,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            1_u64 << 32,
            (1_u64 << 63) - 1,
            1_u64 << 63,
            u64::MAX - 1,
            u64::MAX,
        ];
        let values = [
            0,
            1,
            2,
            3,
            4,
            5,
            31,
            32,
            33,
            63,
            64,
            65,
            u64::from(u32::MAX) - 1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            (1_u64 << 32) - 1,
            1_u64 << 32,
            (1_u64 << 32) + 1,
            (1_u64 << 63) - 1,
            1_u64 << 63,
            (1_u64 << 63) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];

        for divisor in divisors {
            let reducer = StrengthReducedU64::new(divisor);
            for value in values {
                assert_eq!(
                    reducer.remainder(value),
                    value % divisor,
                    "value={value} divisor={divisor}"
                );
            }

            let mut value = 0x1234_5678_9abc_def0 ^ divisor;
            for _ in 0..10_000 {
                value = value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                assert_eq!(
                    reducer.remainder(value),
                    value % divisor,
                    "value={value} divisor={divisor}"
                );
            }
        }
    }
    // SPDX-SnippetEnd

    mod memory;
    mod probe;
}
