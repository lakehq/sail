use std::borrow::Cow;
use std::sync::Arc;

use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

use crate::execution::physical_plan::TracingExec;
use crate::metrics::MetricRegistry;
use crate::TracingExecOptions;

fn format_raw_metrics(plan: &dyn ExecutionPlan) -> String {
    let Some(metrics) = plan.metrics() else {
        return "[]".to_string();
    };
    let mut out = vec![];
    for m in metrics.iter() {
        out.push(format!(
            "{{ partition: {:?}, value: {:?} }}",
            m.partition(),
            m.value(),
        ));
    }
    format!("[{}]", out.join(", "))
}

/// A utility for metric emitter unit tests.
/// This tester executes a given plan and examines the emitted metrics
/// collected by an in-memory exporter.
pub struct MetricEmitterTester {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
    registry: Arc<MetricRegistry>,
    plan: Option<Arc<dyn ExecutionPlan>>,
    expected_metrics: Vec<Cow<'static, str>>,
    unexpected_metrics: Vec<Cow<'static, str>>,
}

impl MetricEmitterTester {
    pub fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let meter = provider.meter("test");
        let registry = Arc::new(MetricRegistry::new(&meter));
        let expected_metrics = vec![
            // Each DataFusion execution plan is expected to at least emit
            // the metrics defined in `BaselineMetrics`.
            registry.execution_output_size.name(),
            registry.execution_output_row_count.name(),
            registry.execution_elapsed_compute_time.name(),
        ];
        let unexpected_metrics = vec![
            registry.execution_unknown_metric_count.name(),
            registry.execution_unknown_metric_label_count.name(),
        ];
        Self {
            exporter,
            provider,
            registry,
            plan: None,
            expected_metrics,
            unexpected_metrics,
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

    pub fn with_expected_metrics<F: FnOnce(&MetricRegistry) -> Vec<Cow<'static, str>>>(
        mut self,
        metrics: F,
    ) -> Self {
        self.expected_metrics.extend(metrics(&self.registry));
        self
    }

    #[expect(unused)]
    pub fn with_unexpected_metrics<F: FnOnce(&MetricRegistry) -> Vec<Cow<'static, str>>>(
        mut self,
        metrics: F,
    ) -> Self {
        self.unexpected_metrics.extend(metrics(&self.registry));
        self
    }

    pub async fn run(self) -> Result<()> {
        let Some(plan) = self.plan else {
            return plan_err!("missing execution plan");
        };
        let options = TracingExecOptions::default().with_metric_registry(self.registry);
        let plan = Arc::new(TracingExec::new(plan, options));
        let context = Arc::new(TaskContext::default());
        let _ = plan.execute(0, context)?.try_collect::<Vec<_>>().await?;
        self.provider
            .force_flush()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metrics = self
            .exporter
            .get_finished_metrics()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut missing_metrics = self.expected_metrics.clone();
        let mut actual_unexpected_metrics = vec![];
        metrics
            .iter()
            .flat_map(|m| m.scope_metrics())
            .flat_map(|m| m.metrics())
            .for_each(|m| {
                let name = m.name();
                if self.unexpected_metrics.iter().any(|x| x.as_ref() == name) {
                    actual_unexpected_metrics.push(name.to_string());
                }
                missing_metrics.retain(|x| x.as_ref() != name);
            });
        if !missing_metrics.is_empty() {
            return plan_err!("missing expected metrics: {missing_metrics:?}");
        }
        if !actual_unexpected_metrics.is_empty() {
            return plan_err!(
                "found unexpected metrics: {actual_unexpected_metrics:?}\nraw datafusion metrics: {}",
                format_raw_metrics(plan.as_ref())
            );
        }
        Ok(())
    }
}
