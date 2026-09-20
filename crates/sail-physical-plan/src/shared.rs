//! A shared producer marker and its query-local execution lowering.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    replace_children_if_necessary,
};
use futures::{StreamExt, TryStreamExt};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::repartition::{
    DEFAULT_REPARTITION_BUFFER_SIZE, RepartitionBufferConfig,
};
use sail_common_datafusion::spillable_stream::{SpillableStream, SpillableStreamWriter};
use tokio::task::JoinHandle;

/// IDs are scoped to one physical plan. This node contains no execution state.
#[derive(Debug)]
pub struct SharedPlanExec {
    id: usize,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SharedPlanExec {
    pub fn new(id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input.properties().clone();
        Self {
            id,
            input,
            properties,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for SharedPlanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedPlanExec: id={}", self.id)
    }
}

impl ExecutionPlan for SharedPlanExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "SharedPlanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children
            .try_into()
            .map_err(|_| DataFusionError::Internal("shared producer expects one input".into()))?;
        Ok(Arc::new(Self::new(self.id, input)))
    }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        internal_err!("shared producer must be lowered before execution")
    }
}

struct ProducerState {
    writers: Vec<SpillableStreamWriter>,
    tasks: Vec<JoinHandle<()>>,
    started: bool,
}

struct SharedProducer {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    outputs: Vec<SpillableStream>,
    state: Mutex<ProducerState>,
    metrics: ExecutionPlanMetricsSet,
}

impl SharedProducer {
    fn new(input: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let buffer = context
            .extension::<RepartitionBufferConfig>()
            .map(|config| config.buffer_size())
            .unwrap_or(DEFAULT_REPARTITION_BUFFER_SIZE);
        let (outputs, writers) = (0..input.output_partitioning().partition_count())
            .map(|partition| {
                SpillableStream::new(&context, input.schema(), &metrics, partition, buffer)
            })
            .unzip();
        Self {
            input,
            context,
            outputs,
            state: Mutex::new(ProducerState {
                writers,
                tasks: vec![],
                started: false,
            }),
            metrics,
        }
    }

    fn start(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.started {
            return;
        }
        state.started = true;
        // Drain partitions concurrently: repartition and join inputs may depend
        // on each other. Readers still receive each partition incrementally.
        state.tasks = std::mem::take(&mut state.writers)
            .into_iter()
            .enumerate()
            .map(|(partition, mut writer)| {
                let input = self.input.clone();
                let context = self.context.clone();
                let metrics = self.metrics.clone();
                tokio::spawn(async move {
                    MetricBuilder::new(&metrics)
                        .counter("producer_executions", partition)
                        .add(1);
                    let result: Result<()> = async {
                        let mut stream = input.execute(partition, context)?;
                        while let Some(batch) = stream.try_next().await? {
                            if !writer.write(batch).await? {
                                break;
                            }
                        }
                        Ok(())
                    }
                    .await;
                    match result {
                        Ok(()) => writer.finish(),
                        Err(error) => writer.fail(error),
                    }
                })
            })
            .collect();
    }
}

impl Drop for SharedProducer {
    fn drop(&mut self) {
        for task in &self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
        {
            task.abort();
        }
    }
}

struct SharedReadExec {
    id: usize,
    properties: Arc<PlanProperties>,
    producer: Arc<SharedProducer>,
    readers: Mutex<Vec<Option<SendableRecordBatchStream>>>,
}

impl Debug for SharedReadExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedReadExec({})", self.id)
    }
}

impl DisplayAs for SharedReadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedReadExec: id={}", self.id)
    }
}

impl ExecutionPlan for SharedReadExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "SharedReadExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("shared reader does not accept children");
        }
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.producer.metrics.clone_inner())
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let reader = self
            .readers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "shared reader partition {partition} is unavailable"
                ))
            })?;
        let producer = self.producer.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = futures::stream::once(async move {
            producer.start();
            Ok::<_, DataFusionError>(reader.map(move |batch| {
                let _keep_alive = &producer;
                let batch = batch?;
                Ok(RecordBatch::try_new_with_options(
                    schema.clone(),
                    batch.columns().to_vec(),
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )?)
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

/// Bind after tracing/plan rewrites. Every execution gets a fresh registry;
/// duplicate marker occurrences never execute their own input copies.
pub fn bind_shared_plans(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    fn bind(
        plan: Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        registry: &mut HashMap<usize, Arc<SharedProducer>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(shared) = plan.downcast_ref::<SharedPlanExec>() {
            let id = shared.id();
            let producer = if let Some(producer) = registry.get(&id) {
                producer.clone()
            } else {
                let input = bind(shared.input().clone(), context, registry)?;
                let producer = Arc::new(SharedProducer::new(input, context.clone()));
                registry.insert(id, producer.clone());
                producer
            };
            let readers = producer
                .outputs
                .iter()
                .map(|output| output.subscribe().map(Some))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Arc::new(SharedReadExec {
                id,
                properties: plan.properties().clone(),
                producer,
                readers: Mutex::new(readers),
            }));
        }
        let children = plan
            .children()
            .into_iter()
            .map(|child| bind(child.clone(), context, registry))
            .collect::<Result<Vec<_>>>()?;
        replace_children_if_necessary(plan, children)
    }
    bind(plan, &context, &mut HashMap::new())
}
