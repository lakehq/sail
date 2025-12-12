use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::Metric;

use crate::common::KeyValue;
use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A metric emitter for filter operator metrics.
pub struct FilterMetricEmitter;

impl MetricEmitter for FilterMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } if name == "selectivity" => {
                registry
                    .execution_filter_input_row_count
                    .recorder(ratio_metrics.total())
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
                    .emit();
                registry
                    .execution_filter_output_row_count
                    .recorder(ratio_metrics.part())
                    .with_attributes(attributes)
                    .with_optional_attribute(MetricAttribute::PARTITION, metric.partition())
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
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;

    use crate::execution::metrics::testing::MetricEmitterTester;

    #[tokio::test]
    async fn test_filter_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let plan = Arc::new(EmptyExec::new(schema));
        let plan = Arc::new(FilterExec::try_new(Arc::new(Column::new("a", 0)), plan)?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(|registry| {
                vec![
                    registry.execution_filter_input_row_count.name(),
                    registry.execution_filter_output_row_count.name(),
                ]
            })
            .run()
            .await
    }
}
