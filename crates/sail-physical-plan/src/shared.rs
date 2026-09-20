//! A shared producer marker and its query-local execution lowering.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    replace_children_if_necessary,
};
use futures::future::{BoxFuture, Shared, try_join_all};
use futures::{FutureExt, StreamExt, TryStreamExt};
use sail_common_datafusion::replay::{ReplayBuffer, ReplayOutput};

/// IDs are scoped to one physical plan. This node contains no execution state.
#[derive(Debug)]
pub struct SharedPlanExec {
    id: usize,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SharedPlanExec {
    pub fn new(id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_emission_type(EmissionType::Final),
        );
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

type SharedResult = Shared<
    BoxFuture<'static, std::result::Result<Arc<Vec<Arc<ReplayOutput>>>, Arc<DataFusionError>>>,
>;

struct SharedReadExec {
    id: usize,
    properties: Arc<PlanProperties>,
    output: SharedResult,
    metrics: ExecutionPlanMetricsSet,
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
        Some(self.metrics.clone_inner())
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let output = self.output.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = futures::stream::once(async move {
            let outputs = output.await.map_err(DataFusionError::Shared)?;
            let Some(output) = outputs.get(partition) else {
                return internal_err!("shared output partition {partition} does not exist");
            };
            Ok(output.stream()?.map(move |batch| {
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
        registry: &mut HashMap<usize, (SharedResult, ExecutionPlanMetricsSet)>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(shared) = plan.downcast_ref::<SharedPlanExec>() {
            let id = shared.id();
            let (output, metrics) = if let Some(entry) = registry.get(&id) {
                entry.clone()
            } else {
                let input = bind(shared.input().clone(), context, registry)?;
                let context = context.clone();
                let metrics = ExecutionPlanMetricsSet::new();
                let producer_metrics = metrics.clone();
                // A shared future is polled by any live reader. Dropping one
                // reader does not cancel production; dropping all owners does.
                let output = async move {
                    let result: Result<_> = async {
                        let partitions = input.output_partitioning().partition_count();
                        let outputs = try_join_all((0..partitions).map(|partition| {
                            let input = input.clone();
                            let context = context.clone();
                            let metrics = producer_metrics.clone();
                            async move {
                                MetricBuilder::new(&metrics)
                                    .counter("producer_executions", partition)
                                    .add(1);
                                let mut buffer = ReplayBuffer::new(
                                    &context,
                                    input.schema(),
                                    &metrics,
                                    partition,
                                );
                                let mut stream = input.execute(partition, context)?;
                                while let Some(batch) = stream.try_next().await? {
                                    buffer.append(batch)?;
                                }
                                Ok::<_, DataFusionError>(Arc::new(buffer.finish()?))
                            }
                        }))
                        .await?;
                        Ok(Arc::new(outputs))
                    }
                    .await;
                    result.map_err(Arc::new)
                }
                .boxed()
                .shared();
                registry.insert(id, (output.clone(), metrics.clone()));
                (output, metrics)
            };
            return Ok(Arc::new(SharedReadExec {
                id,
                properties: plan.properties().clone(),
                output,
                metrics,
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
