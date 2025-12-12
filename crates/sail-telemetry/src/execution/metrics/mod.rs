//! This module bridges DataFusion query execution metrics with OpenTelemetry.
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::Metric;

use crate::metrics::{MetricAttribute, MetricRegistry};

pub enum MetricEmitted {
    #[expect(unused)]
    Yes,
    No,
}

pub trait MetricEmitterExtension: Send {
    fn try_emit(&self, metric: &Metric, registry: &MetricRegistry) -> MetricEmitted;
}

pub struct DefaultMetricEmitterExtension;

impl MetricEmitterExtension for DefaultMetricEmitterExtension {
    fn try_emit(&self, _metric: &Metric, _registry: &MetricRegistry) -> MetricEmitted {
        MetricEmitted::No
    }
}

pub struct MetricEmitter {
    extension: Box<dyn MetricEmitterExtension>,
}

impl Default for MetricEmitter {
    fn default() -> Self {
        Self::new(Box::new(DefaultMetricEmitterExtension))
    }
}

impl MetricEmitter {
    pub fn new(extension: Box<dyn MetricEmitterExtension>) -> Self {
        Self { extension }
    }

    pub fn emit(&self, metrics: &MetricsSet, registry: &MetricRegistry) {
        for metric in metrics.iter() {
            match self.extension.try_emit(metric, registry) {
                MetricEmitted::Yes => continue,
                MetricEmitted::No => {}
            }
            match metric.value() {
                MetricValue::OutputRows(count) => registry
                    .execution_output_row_count
                    .recorder(count)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::ElapsedCompute(time) => registry
                    .execution_elapsed_compute_time
                    .recorder(time)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::SpillCount(count) => registry
                    .execution_spill_count
                    .recorder(count)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::SpilledBytes(count) => registry
                    .execution_spill_size
                    .recorder(count)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::OutputBytes(count) => registry
                    .execution_output_size
                    .recorder(count)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::SpilledRows(count) => registry
                    .execution_spill_row_count
                    .recorder(count)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::CurrentMemoryUsage(gauge) => registry
                    .execution_memory_used
                    .recorder(gauge)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit(),
                MetricValue::Count { .. }
                | MetricValue::Gauge { .. }
                | MetricValue::Time { .. }
                | MetricValue::PruningMetrics { .. }
                | MetricValue::Ratio { .. }
                | MetricValue::Custom { .. } => {}
                MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => {}
            }
        }
    }
}
