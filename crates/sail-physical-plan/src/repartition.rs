use std::sync::{Arc, Mutex};

use datafusion::arrow::array::PrimitiveArray;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::UInt32Type;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, plan_err, Result, Statistics};
use futures::StreamExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::repartition::{
    RepartitionBufferConfig, DEFAULT_REPARTITION_BUFFER_SIZE,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

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

#[derive(Debug, Default)]
struct ExplicitRepartitionState {
    receivers: Option<Vec<Option<Receiver<Result<RecordBatch>>>>>,
    tasks: Vec<SpawnedTask<()>>,
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
        let mut state = self.state.lock().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "round-robin repartition state lock poisoned".to_string(),
            )
        })?;
        if state.receivers.is_some() {
            return Ok(());
        }

        let buffer_size = round_robin_buffer_size(context.as_ref());
        let input_partition_count = self.input.output_partitioning().partition_count();
        let mut senders = Vec::with_capacity(output_partitions);
        let mut receivers = Vec::with_capacity(output_partitions);
        for _ in 0..output_partitions {
            let (sender, receiver) = channel::<Result<RecordBatch>>(buffer_size);
            senders.push(sender);
            receivers.push(Some(receiver));
        }

        for input_partition in 0..input_partition_count {
            let input = self.input.execute(input_partition, context.clone())?;
            let output_senders = senders.clone();
            state.tasks.push(SpawnedTask::spawn(async move {
                execute_round_robin_input_partition(
                    input,
                    output_senders,
                    input_partition,
                    input_partition_count,
                    output_partitions,
                )
                .await;
            }));
        }

        state.receivers = Some(receivers);
        Ok(())
    }

    fn take_round_robin_receiver(&self, partition: usize) -> Result<Receiver<Result<RecordBatch>>> {
        let mut state = self.state.lock().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "round-robin repartition state lock poisoned".to_string(),
            )
        })?;
        let Some(receivers) = state.receivers.as_mut() else {
            return internal_err!("round-robin repartition receivers are not initialized");
        };
        let Some(receiver) = receivers.get_mut(partition) else {
            return internal_err!(
                "round-robin repartition partition index {partition} out of range for {} output partitions",
                receivers.len()
            );
        };
        receiver.take().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "round-robin repartition output partition {partition} was already executed"
            ))
        })
    }
}

fn round_robin_buffer_size(context: &TaskContext) -> usize {
    context
        .extension::<RepartitionBufferConfig>()
        .map(|config| config.buffer_size())
        .unwrap_or(DEFAULT_REPARTITION_BUFFER_SIZE)
}

fn partition_round_robin_batch(
    partitioner: &mut RowRoundRobinPartitioner,
    batch: RecordBatch,
    output_partitions: usize,
) -> Result<Vec<Option<RecordBatch>>> {
    let mut partitions = vec![None; output_partitions];
    partitioner.partition(batch, |partition, batch| {
        partitions[partition] = Some(batch);
        Ok(())
    })?;
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

async fn send_round_robin_partitions(
    output_senders: &mut [Option<Sender<Result<RecordBatch>>>],
    partitions: Vec<Option<RecordBatch>>,
) -> bool {
    let mut active = 0;
    for (partition, batch) in partitions.into_iter().enumerate() {
        let Some(sender) = output_senders[partition].as_ref().cloned() else {
            continue;
        };
        if sender.is_closed() {
            output_senders[partition] = None;
            continue;
        }
        match batch {
            Some(batch) => {
                if sender.send(Ok(batch)).await.is_ok() {
                    active += 1;
                } else {
                    output_senders[partition] = None;
                }
            }
            None => {
                active += 1;
            }
        }
    }
    active > 0
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
    input_partition_count: usize,
    output_partitions: usize,
) {
    let mut output_senders = output_senders.into_iter().map(Some).collect::<Vec<_>>();
    let mut partitioner = match RowRoundRobinPartitioner::new(
        output_partitions,
        input_partition,
        input_partition_count,
    ) {
        Ok(partitioner) => partitioner,
        Err(error) => {
            broadcast_round_robin_error(
                &mut output_senders,
                format!("failed to initialize round-robin repartition: {error}"),
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
        let Some(item) = item else {
            return;
        };
        match item {
            Ok(batch) => {
                let partitions = match partition_round_robin_batch(
                    &mut partitioner,
                    batch,
                    output_partitions,
                ) {
                    Ok(partitions) => partitions,
                    Err(error) => {
                        broadcast_round_robin_error(
                            &mut output_senders,
                            format!(
                                "round-robin repartition failed on input partition {input_partition}: {error}"
                            ),
                        )
                        .await;
                        return;
                    }
                };
                if !send_round_robin_partitions(&mut output_senders, partitions).await {
                    return;
                }
            }
            Err(error) => {
                broadcast_round_robin_error(
                    &mut output_senders,
                    format!(
                        "round-robin repartition failed while reading input partition {input_partition}: {error}"
                    ),
                )
                .await;
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
        if current_sender
            .send(Err(datafusion_common::DataFusionError::Execution(
                message.clone(),
            )))
            .await
            .is_err()
        {
            *sender = None;
        }
    }
}

impl DisplayAs for ExplicitRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.properties.partitioning {
            Partitioning::RoundRobinBatch(partitions) => write!(
                f,
                "RepartitionExec: partitioning=RoundRobinBatch({}), input_partitions={}",
                partitions,
                self.input.output_partitioning().partition_count(),
            ),
            _ => write!(f, "{}", Self::static_name()),
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
                let receiver = self.take_round_robin_receiver(partition)?;
                // Keep the round-robin tasks alive for the lifetime of the returned stream.
                // `SpawnedTask` aborts on drop, and the tasks are owned by `self.state`;
                // without this keepalive, the parent `ExplicitRepartitionExec` can drop
                // mid-stream, aborting the producer tasks and causing data loss.
                let state_keepalive = Arc::clone(&self.state);
                let stream = ReceiverStream::new(receiver).map(move |batch| {
                    let _keep = &state_keepalive;
                    batch
                });
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_none() {
            self.input.partition_statistics(None)
        } else {
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    // TODO: Implement the logic to push down filters or projections.
    //   The filters and projections are safe to push down if they are
    //   column references. For other expressions, we may not want to
    //   push them down since the evaluation can be potentially expensive,
    //   and the presence of explicit repartitioning indicates that the user
    //   wants to evaluate these expressions after repartitioning.
}
