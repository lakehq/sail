use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

use crate::TracingExecOptions;
use crate::execution::physical_plan::TracingExec;
use crate::metrics::{MetricManager, MetricRegistry};

fn format_raw_metrics(plan: &dyn ExecutionPlan) -> String {
    let Some(metrics) = plan.metrics() else {
        return "[]".to_string();
    };
    let out = metrics
        .iter()
        .map(|m| format!("{:?}", m.value().name()))
        .collect::<Vec<_>>();
    format!("[{}]", out.join(", "))
}

type ValueCheck = Box<dyn Fn(u64) -> bool>;

/// A utility for metric emitter unit tests.
/// This tester executes a given plan and examines the emitted metrics
/// collected by an in-memory exporter.
pub struct MetricEmitterTester {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
    registry: Arc<MetricRegistry>,
    plan: Option<Arc<dyn ExecutionPlan>>,
    task_context: Option<Arc<TaskContext>>,
    expected_metrics: Vec<Cow<'static, str>>,
    expected_values: Vec<(Cow<'static, str>, ValueCheck)>,
}

impl MetricEmitterTester {
    pub fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let meter = provider.meter("test");
        let registry = Arc::new(MetricRegistry::new(&meter));
        Self {
            exporter,
            provider,
            registry,
            plan: None,
            task_context: None,
            expected_metrics: vec![],
            expected_values: vec![],
        }
    }

    /// Set the execution plan to be tested.
    /// This must be called before running the tester.
    /// The outermost plan will be wrapped with metric emitters.
    /// No metrics will be emitted for the child plans.
    pub fn with_plan(mut self, plan: Arc<dyn ExecutionPlan>) -> Self {
        self.plan = Some(plan);
        self
    }

    /// Set the task context used to execute the plan.
    pub fn with_task_context(mut self, task_context: Arc<TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Expect the metrics provided by DataFusion's `BaselineMetrics`.
    pub fn with_baseline_metrics(mut self) -> Self {
        self.expected_metrics.extend([
            self.registry.execution_output_size.name(),
            self.registry.execution_output_batch_count.name(),
            self.registry.execution_output_row_count.name(),
            self.registry.execution_elapsed_compute_time.name(),
        ]);
        self
    }

    /// Expect the counter for metrics that no emitter handles.
    pub fn with_unknown_metrics(mut self) -> Self {
        #[cfg(debug_assertions)]
        self.expected_metrics
            .push(self.registry.execution_unknown_metric_count.name());
        self
    }

    pub fn with_expected_metrics<F: FnOnce(&MetricRegistry) -> Vec<Cow<'static, str>>>(
        mut self,
        metrics: F,
    ) -> Self {
        self.expected_metrics.extend(metrics(&self.registry));
        self
    }

    /// Expect the last exported value of a `u64` gauge to pass the given check.
    pub fn with_expected_value<F, C>(mut self, metric: F, check: C) -> Self
    where
        F: FnOnce(&MetricRegistry) -> Cow<'static, str>,
        C: Fn(u64) -> bool + 'static,
    {
        self.expected_values
            .push((metric(&self.registry), Box::new(check)));
        self
    }

    pub async fn run(self) -> Result<()> {
        let Some(plan) = self.plan else {
            return plan_err!("missing execution plan");
        };
        let options = TracingExecOptions::default().with_metrics(MetricManager {
            registry: self.registry,
            collection_interval: Duration::ZERO,
        });
        let plan = Arc::new(TracingExec::new(plan, options));
        let context = self
            .task_context
            .unwrap_or_else(|| Arc::new(TaskContext::default()));
        let _ = plan.execute(0, context)?.try_collect::<Vec<_>>().await?;
        self.provider
            .force_flush()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metrics = self
            .exporter
            .get_finished_metrics()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut missing_metrics = self.expected_metrics.clone();
        let mut unexpected_metrics = vec![];
        metrics
            .iter()
            .flat_map(|m| m.scope_metrics())
            .flat_map(|m| m.metrics())
            .for_each(|m| {
                let name = m.name();
                let mut unexpected = true;
                missing_metrics.retain(|x| {
                    let matched = x.as_ref() == name;
                    if matched {
                        unexpected = false;
                    }
                    !matched
                });
                if unexpected {
                    unexpected_metrics.push(name.to_string());
                }
            });
        if !missing_metrics.is_empty() {
            return plan_err!("missing expected metrics: {missing_metrics:?}");
        }
        if !unexpected_metrics.is_empty() {
            return plan_err!(
                "found unexpected metrics: {unexpected_metrics:?} (raw metrics: {})",
                format_raw_metrics(plan.as_ref())
            );
        }
        for (name, check) in &self.expected_values {
            let value = metrics
                .iter()
                .flat_map(|m| m.scope_metrics())
                .flat_map(|m| m.metrics())
                .filter(|m| m.name() == name.as_ref())
                .filter_map(|m| match m.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        gauge.data_points().last().map(|p| p.value())
                    }
                    _ => None,
                })
                .last();
            if !value.is_some_and(check) {
                return plan_err!("unexpected value for metric {name}: {value:?}");
            }
        }
        Ok(())
    }
}
