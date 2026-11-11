use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements, PlanProperties,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use fastrace::Span;
use fastrace_futures::StreamExt;
use futures::Stream;
use pin_project_lite::pin_project;
use sail_common::telemetry::{KeyValue, SpanAttribute};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::execution::metrics::{MetricEmitter, MetricsSetExt};
use crate::metrics::{MetricAttribute, MetricManager, MetricRegistry};

#[derive(Debug, Clone, Default)]
pub struct TracingExecOptions {
    pub metrics: Option<MetricManager>,
    pub session_id: Option<String>,
    pub job_id: Option<u64>,
    pub stage: Option<usize>,
    pub attempt: Option<usize>,
    pub operator_id: Option<u64>,
}

impl TracingExecOptions {
    pub fn with_metrics(mut self, manager: MetricManager) -> Self {
        self.metrics = Some(manager);
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
    emit_lock: Arc<Mutex<()>>,
}

impl TracingExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, options: TracingExecOptions) -> Self {
        Self {
            inner,
            options,
            emit_lock: Arc::new(Mutex::new(())),
        }
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

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "TracingExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.one()?;
        Ok(Arc::new(TracingExec::new(child, self.options.clone())))
    }

    fn with_new_children_and_same_properties(
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
        let span = if let Some(session_id) = self.options.session_id.clone() {
            span.with_property(|| (SpanAttribute::SESSION_ID, session_id))
        } else {
            span
        };
        let stream = {
            let _guard = span.set_local_parent();
            self.inner.execute(partition, context)?
        };
        let schema = stream.schema();
        if let Some(ref manager) = self.options.metrics {
            let stream = MetricEmitterStream {
                inner: stream,
                plan: self.inner.clone(),
                emitter: self.build_metric_emitter(),
                attributes: self.build_metric_attributes(),
                registry: manager.registry.clone(),
                interval: manager.collection_interval,
                last_emit: None,
                emit_lock: Arc::clone(&self.emit_lock),
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

    #[expect(
        deprecated,
        reason = "TracingExec transparently exposes legacy child statistics"
    )]
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        match input_stats {
            [inner] => Ok(Arc::clone(inner)),
            _ => plan_err!(
                "TracingExec expected statistics for one input, but got {}",
                input_stats.len()
            ),
        }
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
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

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn try_to_proto(
        &self,
        _ctx: &datafusion::physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
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

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }
}

impl TracingExec {
    fn build_metric_emitter(&self) -> Box<dyn MetricEmitter> {
        crate::execution::metrics::build_metric_emitter(self.inner.as_ref())
    }

    fn build_metric_attributes(&self) -> Vec<KeyValue> {
        let mut attributes = vec![];
        if let Some(session_id) = &self.options.session_id {
            attributes.push((MetricAttribute::SESSION_ID, session_id.clone().into()));
        }
        if let Some(job_id) = self.options.job_id {
            attributes.push((MetricAttribute::EXECUTION_JOB_ID, job_id.to_string().into()));
        }
        if let Some(stage) = self.options.stage {
            attributes.push((MetricAttribute::EXECUTION_STAGE, stage.to_string().into()));
        }
        if let Some(attempt) = self.options.attempt {
            attributes.push((
                MetricAttribute::EXECUTION_ATTEMPT,
                attempt.to_string().into(),
            ));
        }
        if let Some(operator_id) = self.options.operator_id {
            attributes.push((
                MetricAttribute::EXECUTION_OPERATOR_ID,
                operator_id.to_string().into(),
            ));
        }
        attributes.push((
            MetricAttribute::EXECUTION_OPERATOR_NAME,
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
        interval: Duration,
        last_emit: Option<Instant>,
        emit_lock: Arc<Mutex<()>>,
    }
}

impl Stream for MetricEmitterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.inner.poll_next(cx);
        if poll.is_ready() {
            let is_done = matches!(poll, Poll::Ready(None));
            // Note: metrics are not emitted regularly if a batch takes long to be produced,
            // but this is acceptable for the purpose of execution metrics.
            let should_emit =
                is_done || this.last_emit.is_none_or(|t| t.elapsed() >= *this.interval);
            if should_emit {
                // Guard here covers both reading and recording, to ensure the last emission
                // carries the latest values.
                let _guard = this
                    .emit_lock
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);

                if let Some(metrics) = this.plan.metrics() {
                    for metric in metrics.aggregate_duplicates().iter() {
                        let _ = this
                            .emitter
                            .try_emit(metric, this.attributes, this.registry);
                    }
                }
                *this.last_emit = Some(Instant::now());
            }
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, mpsc};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::{DataFusionError, internal_err};
    use datafusion::physical_plan::Metric;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use futures::TryStreamExt;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    use super::*;
    use crate::execution::metrics::{MetricHandled, build_metric_emitter};

    /// A plan without data whose metrics are set by the test.
    #[derive(Debug)]
    struct MetricsOnlyExec {
        metrics: ExecutionPlanMetricsSet,
        properties: Arc<PlanProperties>,
    }

    impl DisplayAs for MetricsOnlyExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MetricsOnlyExec")
        }
    }

    impl ExecutionPlan for MetricsOnlyExec {
        fn name(&self) -> &str {
            "MetricsOnlyExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            internal_err!("MetricsOnlyExec cannot be executed")
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    /// Runs a hook before emitting the first metric.
    struct PausingEmitter {
        inner: Box<dyn MetricEmitter>,
        hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl MetricEmitter for PausingEmitter {
        fn try_emit(
            &self,
            metric: &Metric,
            attributes: &[KeyValue],
            registry: &MetricRegistry,
        ) -> MetricHandled {
            let hook = self.hook.lock().ok().and_then(|mut hook| hook.take());
            if let Some(hook) = hook {
                hook();
            }
            self.inner.try_emit(metric, attributes, registry)
        }
    }

    fn emitter_stream(
        plan: Arc<dyn ExecutionPlan>,
        emitter: Box<dyn MetricEmitter>,
        registry: Arc<MetricRegistry>,
        emit_lock: Arc<Mutex<()>>,
    ) -> MetricEmitterStream {
        MetricEmitterStream {
            inner: Box::pin(RecordBatchStreamAdapter::new(
                plan.schema(),
                futures::stream::empty(),
            )),
            plan,
            emitter,
            attributes: vec![],
            registry,
            interval: Duration::ZERO,
            last_emit: None,
            emit_lock,
        }
    }

    fn drain(stream: MetricEmitterStream) -> Result<()> {
        futures::executor::block_on(stream.try_collect::<Vec<_>>()).map(|_| ())
    }

    /// Returns the last exported value of a `u64` gauge for a partition.
    fn exported_gauge(
        exporter: &InMemoryMetricExporter,
        name: &str,
        partition: &str,
    ) -> Result<Option<u64>> {
        let metrics = exporter
            .get_finished_metrics()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(metrics
            .iter()
            .flat_map(|m| m.scope_metrics())
            .flat_map(|m| m.metrics())
            .filter(|m| m.name() == name)
            .filter_map(|m| match m.data() {
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
                    .data_points()
                    .filter(|p| {
                        p.attributes().any(|kv| {
                            kv.key.as_str() == MetricAttribute::EXECUTION_PARTITION
                                && kv.value.as_str() == partition
                        })
                    })
                    .last()
                    .map(|p| p.value()),
                _ => None,
            })
            .last())
    }

    #[test]
    fn test_concurrent_streams_emit_latest_metric_values() -> Result<()> {
        let metrics = ExecutionPlanMetricsSet::new();
        let rows = MetricBuilder::new(&metrics).output_rows(1);
        MetricBuilder::new(&metrics).output_rows(1);
        rows.add(100);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(MetricsOnlyExec {
            metrics,
            properties: Arc::clone(EmptyExec::new(Arc::new(Schema::empty())).properties()),
        });

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let registry = Arc::new(MetricRegistry::new(&provider.meter("test")));

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel::<()>();
        let hook: Box<dyn FnOnce() + Send> = Box::new(move || {
            let _ = paused_tx.send(());
            // Times out if the other stream cannot emit while this one is emitting.
            let _ = resume_rx.recv_timeout(Duration::from_secs(1));
        });
        let emit_lock = Arc::new(Mutex::new(()));
        let paused = emitter_stream(
            Arc::clone(&plan),
            Box::new(PausingEmitter {
                inner: build_metric_emitter(plan.as_ref()),
                hook: Mutex::new(Some(hook)),
            }),
            Arc::clone(&registry),
            Arc::clone(&emit_lock),
        );
        let finishing = emitter_stream(
            Arc::clone(&plan),
            build_metric_emitter(plan.as_ref()),
            Arc::clone(&registry),
            emit_lock,
        );

        std::thread::scope(|s| {
            let paused = s.spawn(move || drain(paused));
            let finishing = s.spawn(move || -> Result<()> {
                paused_rx
                    .recv()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                rows.add(50);
                drain(finishing)?;
                let _ = resume_tx.send(());
                Ok(())
            });
            for handle in [paused, finishing] {
                handle
                    .join()
                    .map_err(|_| DataFusionError::Internal("stream thread panicked".into()))??;
            }
            Ok::<_, DataFusionError>(())
        })?;
        provider
            .force_flush()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let rows = exported_gauge(&exporter, &registry.execution_output_row_count.name(), "1")?;
        assert_eq!(rows, Some(150));
        Ok(())
    }
}
