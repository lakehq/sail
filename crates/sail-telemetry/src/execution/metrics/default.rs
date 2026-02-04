use datafusion::physical_plan::metrics::{Label, MetricValue};
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
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::ElapsedCompute(time) => {
                registry
                    .execution_elapsed_compute_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpillCount(count) => {
                registry
                    .execution_spill_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpilledBytes(count) => {
                registry
                    .execution_spill_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::OutputBytes(count) => {
                registry
                    .execution_output_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpilledRows(count) => {
                registry
                    .execution_spill_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::CurrentMemoryUsage(gauge) => {
                registry
                    .execution_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::OutputBatches(count) => {
                registry
                    .execution_output_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::Count { .. }
            | MetricValue::Gauge { .. }
            | MetricValue::Time { .. }
            | MetricValue::Ratio { .. }
            | MetricValue::PruningMetrics { .. }
            | MetricValue::Custom { .. } => {
                // These metric types are not yet handled by any emitter.
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

#[derive(Default)]
pub struct LabelExtractor {
    extractors: Vec<(&'static str, &'static str)>,
}

impl LabelExtractor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_extractor(mut self, from: &'static str, to: &'static str) -> Self {
        self.extractors.push((from, to));
        self
    }

    pub fn extract(&self, labels: &[Label], registry: &MetricRegistry) -> Vec<KeyValue> {
        #[cfg(not(debug_assertions))]
        let _ = registry;
        let mut attributes = vec![];
        'outer: for label in labels {
            for (from, to) in &self.extractors {
                if label.name() == *from {
                    attributes.push((*to, label.value().to_string().into()));
                    continue 'outer;
                }
            }
            #[cfg(debug_assertions)]
            registry
                .execution_unknown_metric_label_count
                .adder(1u64)
                .emit();
        }
        attributes
    }
}
