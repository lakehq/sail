use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::MetricValue;
use sail_common::telemetry::KeyValue;

use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A metric emitter for buffer operator metrics.
pub struct BufferMetricEmitter;

impl MetricEmitter for BufferMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::PeakMemoryUsage { name, gauge } if name == "max_mem_used" => {
                registry
                    .execution_buffer_peak_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Gauge { name, gauge } if name == "max_queued" => {
                registry
                    .execution_buffer_peak_queued_batch_count
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            _ => MetricHandled::No,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::physical_plan::buffer::BufferExec;
    use datafusion::physical_plan::empty::EmptyExec;

    use crate::execution::metrics::testing::MetricEmitterTester;

    #[tokio::test]
    async fn test_buffer_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let plan = Arc::new(BufferExec::new(Arc::new(EmptyExec::new(schema)), 1));

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(|registry| {
                vec![
                    registry.execution_buffer_peak_memory_used.name(),
                    registry.execution_buffer_peak_queued_batch_count.name(),
                ]
            })
            .run()
            .await
    }
}
