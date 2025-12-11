use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::Metric;

use crate::common::KeyValue;
use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A default metric emitter that handles common execution metrics.
/// This emitter must be used at last since it increments the unknown metric counter
/// for all unknown metrics in debug builds.
pub struct DefaultMetricEmitter;

impl MetricEmitter for DefaultMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::OutputRows(count) => {
                registry
                    .execution_output_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::ElapsedCompute(time) => {
                registry
                    .execution_elapsed_compute_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::SpillCount(count) => {
                registry
                    .execution_spill_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::SpilledBytes(count) => {
                registry
                    .execution_spill_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::OutputBytes(count) => {
                registry
                    .execution_output_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::SpilledRows(count) => {
                registry
                    .execution_spill_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::CurrentMemoryUsage(gauge) => {
                registry
                    .execution_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
            }
            MetricValue::Count { .. }
            | MetricValue::Gauge { .. }
            | MetricValue::Time { .. }
            | MetricValue::PruningMetrics { .. }
            | MetricValue::Ratio { .. }
            | MetricValue::Custom { .. } => {
                #[cfg(debug_assertions)]
                registry.execution_unknown_metric_count.adder(1u64).emit();
            }
            MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => {
                // The timestamp is ignored since it is not a metric.
                // We record timestamps via our tracing spans instead.
            }
        }
        // The default emitter always claims to have handled the metric.
        MetricHandled::Yes
    }
}
