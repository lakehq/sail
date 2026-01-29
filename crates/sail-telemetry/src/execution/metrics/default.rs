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
            MetricValue::OutputBatches(_count) => {
                // OutputBatches is now tracked as part of BaselineMetrics
                // This is already handled by RecordOutput trait, so we just acknowledge it
                // without incrementing unknown metric count
            }
            MetricValue::Count { .. } | MetricValue::Gauge { .. } | MetricValue::Time { .. } => {
                // These are legitimate operator-specific metrics (like "build_time", "join_time", etc.)
                // that are emitted by DataFusion operators. We don't handle them explicitly
                // but they're not "unknown" in the sense that they're expected.
                // Let specific emitters handle the ones they care about.
            }
            MetricValue::Ratio { .. } => {
                // Ratio metrics are legitimate operator-specific metrics (e.g. selectivity,
                // probe_hit_rate, avg_fanout). Specific emitters can map ones they care about.
                // We intentionally do not treat them as "unknown".
            }
            MetricValue::PruningMetrics { .. } | MetricValue::Custom { .. } => {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::PhysicalExpr;

    use crate::execution::metrics::testing::MetricEmitterTester;

    #[tokio::test]
    async fn test_projection_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let plan = Arc::new(EmptyExec::new(schema));
        let plan = Arc::new(ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                "b".to_string(),
            )],
            plan,
        )?);

        MetricEmitterTester::new().with_plan(plan).run().await
    }
}
