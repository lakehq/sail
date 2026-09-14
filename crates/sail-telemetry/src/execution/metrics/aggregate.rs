use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::MetricValue;
use sail_common::telemetry::KeyValue;

use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A metric emitter for aggregate operator metrics.
pub struct AggregateMetricEmitter;

impl MetricEmitter for AggregateMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::PeakMemoryUsage { name, gauge } if name == "peak_mem_used" => {
                registry
                    .execution_aggregate_peak_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Time { name, time } if name == "time_calculating_group_ids" => {
                registry
                    .execution_aggregate_group_id_calculation_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Time { name, time } if name == "aggregate_arguments_time" => {
                registry
                    .execution_aggregate_argument_evaluation_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Time { name, time } if name == "aggregation_time" => {
                registry
                    .execution_aggregate_aggregation_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Time { name, time } if name == "emitting_time" => {
                registry
                    .execution_aggregate_output_emission_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "skipped_aggregation_rows" => {
                registry
                    .execution_aggregate_skipped_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } if name == "reduction_factor" => {
                registry
                    .execution_aggregate_input_row_count
                    .recorder(ratio_metrics.total())
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                registry
                    .execution_aggregate_output_row_count
                    .recorder(ratio_metrics.part())
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

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{DataFusionError, Result};
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::prelude::{SessionConfig, SessionContext, col};

    use crate::execution::metrics::testing::MetricEmitterTester;

    fn find_aggregate_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        for child in plan.children() {
            if let Ok(plan) = find_aggregate_plan(Arc::clone(child)) {
                return Ok(plan);
            }
        }
        if plan.is::<AggregateExec>() {
            Ok(plan)
        } else {
            Err(DataFusionError::Plan(
                "aggregate plan not found".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn test_aggregate_metrics() -> Result<()> {
        let mut session_config = SessionConfig::new();
        session_config
            .options_mut()
            .execution
            .enable_migration_aggregate = false;
        let context = SessionContext::new_with_config(session_config.clone());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 1, 2]))],
        )?;
        let plan = context
            .read_batch(batch)?
            .aggregate(vec![col("a")], vec![count(col("a"))])?
            .create_physical_plan()
            .await?;

        MetricEmitterTester::new()
            .with_plan(find_aggregate_plan(plan)?)
            // `peak_mem_used` is emitted by DataFusion's grouped-hash aggregate fallback.
            .with_task_context(Arc::new(
                TaskContext::default().with_session_config(session_config),
            ))
            .with_baseline_metrics()
            .with_expected_metrics(|registry| {
                vec![
                    registry.execution_aggregate_peak_memory_used.name(),
                    registry
                        .execution_aggregate_group_id_calculation_time
                        .name(),
                    registry.execution_aggregate_argument_evaluation_time.name(),
                    registry.execution_aggregate_aggregation_time.name(),
                    registry.execution_aggregate_output_emission_time.name(),
                    registry.execution_aggregate_input_row_count.name(),
                    registry.execution_aggregate_output_row_count.name(),
                    registry.execution_aggregate_skipped_row_count.name(),
                    registry.execution_spill_count.name(),
                    registry.execution_spill_size.name(),
                    registry.execution_spill_row_count.name(),
                ]
            })
            .run()
            .await
    }
}
