use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use opentelemetry::metrics::MeterProvider;
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

    pub fn with_expected_metrics<F: FnOnce(&MetricRegistry) -> Vec<Cow<'static, str>>>(
        mut self,
        metrics: F,
    ) -> Self {
        self.expected_metrics.extend(metrics(&self.registry));
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
        Ok(())
    }
}
