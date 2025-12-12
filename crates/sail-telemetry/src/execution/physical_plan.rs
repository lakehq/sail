use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_err, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    check_default_invariants, CardinalityEffect, InvariantLevel,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fastrace::Span;
use fastrace_futures::StreamExt;
use futures::Stream;
use pin_project_lite::pin_project;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::common::{KeyValue, SpanAttribute};
use crate::execution::metrics::MetricEmitter;
use crate::metrics::{MetricAttribute, MetricRegistry};

#[derive(Debug, Clone, Default)]
pub struct TracingExecOptions {
    pub metric_registry: Option<Arc<MetricRegistry>>,
    pub job_id: Option<u64>,
    pub task_id: Option<u64>,
    pub task_attempt: Option<usize>,
    pub operator_id: Option<u64>,
}

impl TracingExecOptions {
    pub fn with_metric_registry(mut self, registry: Arc<MetricRegistry>) -> Self {
        self.metric_registry = Some(registry);
        self
    }
}

pub fn trace_execution_plan(
    plan: Arc<dyn ExecutionPlan>,
    options: TracingExecOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if options.operator_id.is_some() {
        return plan_err!(
            "operator ID is supposed to be assigned automatically for tracing execution plan"
        );
    }
    let mut index = 0u64;
    plan.transform(|plan| {
        let mut options = options.clone();
        index += 1;
        options.operator_id = Some(index);
        Ok(Transformed::yes(Arc::new(TracingExec::new(plan, options))))
    })
    .data()
}

/// A physical execution plan wrapper that emits traces and metrics
/// for each operation during query execution.
///
/// Note that this wrapper should only be injected to optimized physical plans
/// right before query execution. Many of the [`ExecutionPlan`] methods
/// of this wrapper only have placeholder implementations, so physical optimizers
/// may break or become ineffective if this wrapper ever participates in
/// physical optimization.
#[derive(Debug)]
pub struct TracingExec {
    inner: Arc<dyn ExecutionPlan>,
    options: TracingExecOptions,
}

impl TracingExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, options: TracingExecOptions) -> Self {
        Self { inner, options }
    }
}

impl DisplayAs for TracingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[warn(clippy::missing_trait_methods)]
impl ExecutionPlan for TracingExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "TracingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.one()?;
        Ok(Arc::new(TracingExec::new(child, self.options.clone())))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let child = Arc::clone(&self.inner);
        Ok(Arc::new(TracingExec::new(child, self.options.clone())))
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let span = Span::enter_with_local_parent(self.inner.name().to_string())
            .with_property(|| (SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
        let stream = {
            let _guard = span.set_local_parent();
            self.inner.execute(partition, context)?
        };
        let schema = stream.schema();
        if let Some(ref registry) = self.options.metric_registry {
            let stream = MetricEmitterStream {
                inner: stream,
                plan: self.inner.clone(),
                emitter: self.build_metric_emitter(),
                attributes: self.build_metric_attributes(),
                registry: registry.clone(),
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream.in_span(span),
            )))
        } else {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream.in_span(span),
            )))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    #[allow(deprecated)]
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &[&self.inner],
        ))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

impl TracingExec {
    fn build_metric_emitter(&self) -> Box<dyn MetricEmitter> {
        crate::execution::metrics::build_metric_emitter(self.inner.as_ref())
    }

    fn build_metric_attributes(&self) -> Vec<KeyValue> {
        let mut attributes = vec![];
        if let Some(job_id) = self.options.job_id {
            attributes.push((MetricAttribute::JOB_ID, job_id.to_string().into()));
        }
        if let Some(task_id) = self.options.task_id {
            attributes.push((MetricAttribute::TASK_ID, task_id.to_string().into()));
        }
        if let Some(task_attempt) = self.options.task_attempt {
            attributes.push((
                MetricAttribute::TASK_ATTEMPT,
                task_attempt.to_string().into(),
            ));
        }
        if let Some(operator_id) = self.options.operator_id {
            attributes.push((MetricAttribute::OPERATOR_ID, operator_id.to_string().into()));
        }
        attributes.push((
            MetricAttribute::OPERATOR_NAME,
            self.inner.name().to_string().into(),
        ));
        attributes
    }
}

pin_project! {
    struct MetricEmitterStream {
        #[pin]
        inner: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        emitter: Box<dyn MetricEmitter>,
        attributes: Vec<KeyValue>,
        registry: Arc<MetricRegistry>,
    }
}

impl Stream for MetricEmitterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.inner.poll_next(cx);
        if poll.is_ready() {
            if let Some(metrics) = this.plan.metrics() {
                for metric in metrics.iter() {
                    let _ = this
                        .emitter
                        .try_emit(metric, this.attributes, this.registry);
                }
            }
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
