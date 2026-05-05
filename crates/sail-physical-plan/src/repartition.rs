use std::any::Any;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::PrimitiveArray;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::UInt32Type;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
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
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio_stream::wrappers::UnboundedReceiverStream;

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
    receivers: Option<Vec<Option<UnboundedReceiver<Result<RecordBatch>>>>>,
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

        let input_partition_count = self.input.output_partitioning().partition_count();
        let mut senders = Vec::with_capacity(output_partitions);
        let mut receivers = Vec::with_capacity(output_partitions);
        for _ in 0..output_partitions {
            let (sender, receiver) = unbounded_channel::<Result<RecordBatch>>();
            senders.push(sender);
            receivers.push(Some(receiver));
        }

        for input_partition in 0..input_partition_count {
            let input = self.input.execute(input_partition, context.clone())?;
            let output_senders = senders.clone();
            tokio::spawn(async move {
                execute_round_robin_input_partition(
                    input,
                    output_senders,
                    input_partition,
                    input_partition_count,
                    output_partitions,
                )
                .await;
            });
        }

        state.receivers = Some(receivers);
        Ok(())
    }

    fn take_round_robin_receiver(
        &self,
        partition: usize,
    ) -> Result<UnboundedReceiver<Result<RecordBatch>>> {
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

async fn execute_round_robin_input_partition(
    mut input: SendableRecordBatchStream,
    output_senders: Vec<tokio::sync::mpsc::UnboundedSender<Result<RecordBatch>>>,
    input_partition: usize,
    input_partition_count: usize,
    output_partitions: usize,
) {
    let mut partitioner = match RowRoundRobinPartitioner::new(
        output_partitions,
        input_partition,
        input_partition_count,
    ) {
        Ok(partitioner) => partitioner,
        Err(error) => {
            broadcast_round_robin_error(
                &output_senders,
                format!("failed to initialize round-robin repartition: {error}"),
            );
            return;
        }
    };

    while let Some(item) = input.next().await {
        match item {
            Ok(batch) => {
                if let Err(error) = partitioner.partition(batch, |partition, batch| {
                    let _ = output_senders[partition].send(Ok(batch));
                    Ok(())
                }) {
                    broadcast_round_robin_error(&output_senders, format!(
                        "round-robin repartition failed on input partition {input_partition}: {error}"
                    ));
                    return;
                }
            }
            Err(error) => {
                broadcast_round_robin_error(&output_senders, format!(
                    "round-robin repartition failed while reading input partition {input_partition}: {error}"
                ));
                return;
            }
        }
    }
}

fn broadcast_round_robin_error(
    output_senders: &[tokio::sync::mpsc::UnboundedSender<Result<RecordBatch>>],
    message: String,
) {
    for sender in output_senders {
        let _ = sender.send(Err(datafusion_common::DataFusionError::Execution(
            message.clone(),
        )));
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

    fn as_any(&self) -> &dyn Any {
        self
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
                let stream = UnboundedReceiverStream::new(receiver);
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            self.input.partition_statistics(None)
        } else {
            Ok(Statistics::new_unknown(&self.schema()))
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
