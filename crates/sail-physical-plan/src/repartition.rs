// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::array::PrimitiveArray;
use datafusion::arrow::compute::{BatchCoalescer, take_arrays};
use datafusion::arrow::datatypes::{SchemaRef, UInt32Type};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::projection::{
    ProjectionExec, all_columns, make_with_child, update_expr,
};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    apply_expression_roots,
};
use datafusion_common::{Result, Statistics, internal_err, plan_err};
use futures::{Stream, StreamExt};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::repartition::{
    DEFAULT_REPARTITION_BUFFER_SIZE, RepartitionBufferConfig,
};
use tokio::sync::mpsc::{Receiver, Sender, channel};

#[derive(Debug, Clone)]
pub struct RowRoundRobinPartitioner {
    num_partitions: usize,
    next_idx: usize,
}

impl RowRoundRobinPartitioner {
    pub fn new(
        num_partitions: usize,
        input_partition: usize,
        num_input_partitions: usize,
    ) -> Result<Self> {
        if num_partitions == 0 {
            return internal_err!("round-robin repartition requires at least one output partition");
        }
        if num_input_partitions == 0 {
            return internal_err!("round-robin repartition requires at least one input partition");
        }
        Ok(Self {
            num_partitions,
            next_idx: (input_partition * num_partitions) / num_input_partitions,
        })
    }

    pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.num_partitions == 1 {
            return f(0, batch);
        }

        let schema = batch.schema();
        let mut indices = vec![Vec::new(); self.num_partitions];
        for row_index in 0..batch.num_rows() {
            let partition = (self.next_idx + row_index) % self.num_partitions;
            indices[partition].push(row_index as u32);
        }
        self.next_idx = (self.next_idx + batch.num_rows()) % self.num_partitions;

        for (partition, partition_indices) in indices.into_iter().enumerate() {
            if partition_indices.is_empty() {
                continue;
            }
            let indices_array: PrimitiveArray<UInt32Type> = partition_indices.into();
            let columns = take_arrays(batch.columns(), &indices_array, None)?;
            let options = RecordBatchOptions::new().with_row_count(Some(indices_array.len()));
            let partition_batch =
                RecordBatch::try_new_with_options(schema.clone(), columns, &options)?;
            f(partition, partition_batch)?;
        }

        Ok(())
    }
}

pub(crate) struct RoundRobinReceiverStream {
    state: Arc<Mutex<ExplicitRepartitionState>>,
    inner: Receiver<Result<RecordBatch>>,
}

impl Stream for RoundRobinReceiverStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_recv(cx)
    }
}

impl Drop for RoundRobinReceiverStream {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active_streams = state.active_streams.saturating_sub(1);
        let all_receivers_taken = state
            .receivers
            .as_ref()
            .is_some_and(|receivers| receivers.iter().all(Option::is_none));
        if state.active_streams == 0 && all_receivers_taken {
            state.receivers.take();
            state.tasks.take();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ExplicitRepartitionState {
    receivers: Option<Vec<Option<Receiver<Result<RecordBatch>>>>>,
    active_streams: usize,
    /// Handles to spawned tasks, removed once every output receiver has been
    /// taken and the active output stream count reaches zero.
    tasks: Option<Vec<SpawnedTask<()>>>,
}

/// A physical plan node for explicit repartitioning in the query.
/// This is a placeholder node that should be rewritten during physical optimization.
#[derive(Debug)]
pub struct ExplicitRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    state: Arc<Mutex<ExplicitRepartitionState>>,
}

impl ExplicitRepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        let mut eq_properties = input.equivalence_properties().clone();
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_orderings();
            eq_properties.clear_per_partition_constants();
        }
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                partitioning,
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Self {
            input,
            properties,
            state: Arc::new(Mutex::new(ExplicitRepartitionState::default())),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn initialize_round_robin(
        &self,
        output_partitions: usize,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        initialize_exchange(
            &self.state,
            &self.input,
            &Partitioning::RoundRobinBatch(output_partitions),
            context,
        )
    }

    fn take_round_robin_output(&self, partition: usize) -> Result<RoundRobinReceiverStream> {
        take_exchange_output(&self.state, partition)
    }
}

/// How the rows of an input partition are divided among the output partitions.
enum RowPartitioner {
    RoundRobin(RowRoundRobinPartitioner),
    Hash {
        partitioner: BatchPartitioner,
        outputs: Arc<HashOutputs>,
    },
}

/// The rows of a hash repartition that wait for more rows of their output partition.
///
/// A batch of the input has few rows for each output partition when there are
/// many output partitions, so the rows of each output partition are collected into
/// batches of the batch size of the session before they are sent. The rows are
/// collected for all input partitions together, so that the rows that are held
/// do not grow with the number of input partitions. A batch with at least half
/// of the batch size is sent as it is, without another copy of its rows.
struct HashOutputs {
    rows: Vec<Mutex<BatchCoalescer>>,
    /// The number of input partitions whose rows have not all been divided.
    inputs: AtomicUsize,
}

impl HashOutputs {
    fn new(schema: &SchemaRef, partitions: usize, inputs: usize, batch_size: usize) -> Self {
        let rows = (0..partitions)
            .map(|_| {
                Mutex::new(
                    BatchCoalescer::new(Arc::clone(schema), batch_size)
                        .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
                )
            })
            .collect();
        Self {
            rows,
            inputs: AtomicUsize::new(inputs),
        }
    }

    /// Adds rows of an output partition, or ends the rows of the output partition
    /// when there are no rows to add, and passes on the batches that are complete.
    fn add<F>(&self, partition: usize, batch: Option<RecordBatch>, f: &mut F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        let Some(rows) = self.rows.get(partition) else {
            return internal_err!("repartition has no output partition {partition}");
        };
        let mut rows = rows.lock().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "repartition output lock poisoned".to_string(),
            )
        })?;
        match batch {
            Some(batch) => rows.push_batch(batch)?,
            None => rows.finish_buffered_batch()?,
        }
        while let Some(batch) = rows.next_completed_batch() {
            f(partition, batch)?;
        }
        Ok(())
    }
}

impl RowPartitioner {
    fn try_new(
        partitioning: &Partitioning,
        input_partition: usize,
        input_partition_count: usize,
        outputs: Option<&Arc<HashOutputs>>,
    ) -> Result<Self> {
        match (partitioning, outputs) {
            (Partitioning::RoundRobinBatch(output_partitions), _) => {
                Ok(Self::RoundRobin(RowRoundRobinPartitioner::new(
                    *output_partitions,
                    input_partition,
                    input_partition_count,
                )?))
            }
            (Partitioning::Hash(keys, output_partitions), Some(outputs)) => Ok(Self::Hash {
                partitioner: BatchPartitioner::new_hash_partitioner(
                    keys.clone(),
                    *output_partitions,
                    Time::new(),
                )?,
                outputs: Arc::clone(outputs),
            }),
            (other, _) => internal_err!("repartition does not divide rows by {other}"),
        }
    }

    fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        match self {
            Self::RoundRobin(partitioner) => partitioner.partition(batch, f),
            Self::Hash {
                partitioner,
                outputs,
            } => partitioner.partition(batch, |partition, batch| {
                outputs.add(partition, Some(batch), &mut f)
            }),
        }
    }

    /// Passes on the rows that are left when the input partition has ended.
    fn finish<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        match self {
            Self::RoundRobin(_) => Ok(()),
            Self::Hash { outputs, .. } => {
                // The rows that are left are the rows of all input partitions.
                if outputs.inputs.fetch_sub(1, Ordering::SeqCst) != 1 {
                    return Ok(());
                }
                (0..outputs.rows.len())
                    .try_for_each(|partition| outputs.add(partition, None, &mut f))
            }
        }
    }
}

/// Starts the tasks that read the input partitions and divide their rows among
/// the output partitions, unless they have been started.
///
/// Each output partition buffers a limited number of batches, and a task waits
/// when the buffer of an output partition is full. The rows that the repartition
/// holds are therefore limited, however slowly an output partition is read.
/// The output partitions must be read independently of each other.
pub(crate) fn initialize_exchange(
    state: &Mutex<ExplicitRepartitionState>,
    input: &Arc<dyn ExecutionPlan>,
    partitioning: &Partitioning,
    context: Arc<TaskContext>,
) -> Result<()> {
    let mut state = state.lock().map_err(|_| {
        datafusion_common::DataFusionError::Execution("repartition state lock poisoned".to_string())
    })?;
    if state.receivers.is_some() {
        return Ok(());
    }

    let output_partitions = partitioning.partition_count();
    let buffer_size = round_robin_buffer_size(context.as_ref());
    let input_partition_count = input.output_partitioning().partition_count();
    let mut senders = Vec::with_capacity(output_partitions);
    let mut receivers = Vec::with_capacity(output_partitions);
    for _ in 0..output_partitions {
        let (sender, receiver) = channel::<Result<RecordBatch>>(buffer_size);
        senders.push(sender);
        receivers.push(Some(receiver));
    }

    let hash_outputs = matches!(partitioning, Partitioning::Hash(_, _)).then(|| {
        Arc::new(HashOutputs::new(
            &input.schema(),
            output_partitions,
            input_partition_count,
            context.session_config().batch_size(),
        ))
    });
    let mut tasks = Vec::with_capacity(input_partition_count);
    for input_partition in 0..input_partition_count {
        let input = input.execute(input_partition, context.clone())?;
        let output_senders = senders.clone();
        let partitioner = RowPartitioner::try_new(
            partitioning,
            input_partition,
            input_partition_count,
            hash_outputs.as_ref(),
        );
        tasks.push(SpawnedTask::spawn(async move {
            execute_round_robin_input_partition(
                input,
                output_senders,
                input_partition,
                partitioner,
            )
            .await;
        }));
    }

    state.receivers = Some(receivers);
    state.tasks = Some(tasks);
    Ok(())
}

/// Returns the stream of an output partition, which can be taken once.
pub(crate) fn take_exchange_output(
    state: &Arc<Mutex<ExplicitRepartitionState>>,
    partition: usize,
) -> Result<RoundRobinReceiverStream> {
    let shared = Arc::clone(state);
    let mut state = state.lock().map_err(|_| {
        datafusion_common::DataFusionError::Execution("repartition state lock poisoned".to_string())
    })?;
    if state.tasks.is_none() {
        return internal_err!("repartition tasks are not initialized");
    }
    let Some(receivers) = state.receivers.as_mut() else {
        return internal_err!("repartition receivers are not initialized");
    };
    let Some(receiver) = receivers.get_mut(partition) else {
        return internal_err!(
            "repartition partition index {partition} out of range for {} output partitions",
            receivers.len()
        );
    };
    let receiver = receiver.take().ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(format!(
            "repartition output partition {partition} was already executed"
        ))
    })?;
    state.active_streams += 1;
    Ok(RoundRobinReceiverStream {
        state: shared,
        inner: receiver,
    })
}

fn round_robin_buffer_size(context: &TaskContext) -> usize {
    context
        .extension::<RepartitionBufferConfig>()
        .map(|config| config.buffer_size())
        .unwrap_or(DEFAULT_REPARTITION_BUFFER_SIZE)
}

/// Returns the batches for the output partitions, given the rows of the next
/// batch of the input partition, or given nothing when the input partition has ended.
fn partition_round_robin_batch(
    partitioner: &mut RowPartitioner,
    batch: Option<RecordBatch>,
) -> Result<Vec<(usize, RecordBatch)>> {
    let mut partitions = vec![];
    let add = |partition, batch| {
        partitions.push((partition, batch));
        Ok(())
    };
    match batch {
        Some(batch) => partitioner.partition(batch, add)?,
        None => partitioner.finish(add)?,
    }
    Ok(partitions)
}

fn prune_closed_round_robin_senders(
    output_senders: &mut [Option<Sender<Result<RecordBatch>>>],
) -> bool {
    let mut active = false;
    for sender in output_senders.iter_mut() {
        if sender.as_ref().is_some_and(Sender::is_closed) {
            *sender = None;
        }
        if sender.is_some() {
            active = true;
        }
    }
    active
}

/// Sends the batches to their output partitions, and returns whether
/// an output partition is still read.
async fn send_round_robin_partitions(
    output_senders: &mut [Option<Sender<Result<RecordBatch>>>],
    partitions: Vec<(usize, RecordBatch)>,
) -> bool {
    for (partition, batch) in partitions {
        let Some(output) = output_senders.get_mut(partition) else {
            continue;
        };
        let Some(sender) = output.as_ref().cloned() else {
            continue;
        };
        if sender.send(Ok(batch)).await.is_err() {
            *output = None;
        }
    }
    prune_closed_round_robin_senders(output_senders)
}

async fn wait_for_all_round_robin_outputs_closed(
    output_senders: &[Option<Sender<Result<RecordBatch>>>],
) {
    futures::future::join_all(
        output_senders
            .iter()
            .filter_map(|sender| sender.as_ref())
            .map(|sender| sender.closed()),
    )
    .await;
}

async fn execute_round_robin_input_partition(
    mut input: SendableRecordBatchStream,
    output_senders: Vec<Sender<Result<RecordBatch>>>,
    input_partition: usize,
    partitioner: Result<RowPartitioner>,
) {
    let mut output_senders = output_senders.into_iter().map(Some).collect::<Vec<_>>();
    let mut partitioner = match partitioner {
        Ok(partitioner) => partitioner,
        Err(error) => {
            broadcast_round_robin_error(
                &mut output_senders,
                format!("failed to initialize repartition: {error}"),
            )
            .await;
            return;
        }
    };

    while prune_closed_round_robin_senders(&mut output_senders) {
        let item = tokio::select! {
            biased;
            _ = wait_for_all_round_robin_outputs_closed(&output_senders) => return,
            item = input.next() => item,
        };
        let ended = item.is_none();
        let partitions = match item.transpose() {
            Ok(batch) => partition_round_robin_batch(&mut partitioner, batch).map_err(|error| {
                format!("repartition failed on input partition {input_partition}: {error}")
            }),
            Err(error) => Err(format!(
                "repartition failed while reading input partition {input_partition}: {error}"
            )),
        };
        match partitions {
            Ok(partitions) => {
                if !send_round_robin_partitions(&mut output_senders, partitions).await || ended {
                    return;
                }
            }
            Err(message) => {
                broadcast_round_robin_error(&mut output_senders, message).await;
                return;
            }
        }
    }
}

async fn broadcast_round_robin_error(
    output_senders: &mut [Option<Sender<Result<RecordBatch>>>],
    message: String,
) {
    for sender in output_senders.iter_mut() {
        let Some(current_sender) = sender.as_ref().cloned() else {
            continue;
        };
        let disconnected = current_sender
            .send(Err(datafusion_common::DataFusionError::Execution(
                message.clone(),
            )))
            .await
            .is_err();
        if disconnected {
            *sender = None;
        }
    }
}

impl DisplayAs for ExplicitRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "{}: partitioning={}, input_partitions={}",
                Self::static_name(),
                self.properties.partitioning,
                self.input.output_partitioning().partition_count(),
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "partitioning_scheme={}", self.properties.partitioning)
            }
        }
    }
}

impl ExecutionPlan for ExplicitRepartitionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match self.properties.output_partitioning() {
            Partitioning::Hash(expressions, _) => apply_expression_roots(expressions, f),
            Partitioning::Range(range) => {
                apply_expression_roots(range.ordering().iter().map(|sort_expr| &sort_expr.expr), f)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(ExplicitRepartitionExec::new(
            child,
            self.properties.partitioning.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match &self.properties.partitioning {
            Partitioning::RoundRobinBatch(output_partitions) => {
                self.initialize_round_robin(*output_partitions, context)?;
                let stream = self.take_round_robin_output(partition)?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema(),
                    stream,
                )))
            }
            _ => internal_err!(
                "{} should be eliminated during physical optimization",
                self.name()
            ),
        }
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![if partition.is_none() {
            ChildStats::At(None)
        } else {
            ChildStats::Skip
        }]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if args.partition().is_none() {
            Ok(Arc::clone(&input_stats[0]))
        } else {
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // No benefit if projection does not narrow the schema
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        // Skip if projection expressions are not simple column references
        if projection.benefits_from_input_partitioning()[0] || !all_columns(projection.expr()) {
            return Ok(None);
        }

        let new_projection = make_with_child(projection, self.input())?;
        // Handle all partitioning variants — 'ProjectionPushdown' runs before 'RewriteExplicitRepartition'
        let new_partitioning = match &self.properties.partitioning {
            Partitioning::Hash(partitions, size) => {
                let mut new_partitions = vec![];
                for partition in partitions {
                    let Some(new_partition) = update_expr(partition, projection.expr(), false)?
                    else {
                        return Ok(None);
                    };
                    new_partitions.push(new_partition);
                }
                Partitioning::Hash(new_partitions, *size)
            }
            other => other.clone(),
        };
        Ok(Some(Arc::new(ExplicitRepartitionExec::new(
            new_projection,
            new_partitioning,
        ))))
    }

    // Do not implement Filter pushdown, as its evaluation can be potentially
    // expensive, and the presence of explicit repartitioning indicates that
    // the user wants to evaluate the filters after repartitioning.
}
