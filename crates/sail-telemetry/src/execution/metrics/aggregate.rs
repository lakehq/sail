use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::MetricValue;
use sail_common::telemetry::KeyValue;

use crate::execution::metrics::default::LabelExtractor;
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
            MetricValue::Time { name, time } => {
                let Some((id, phase)) = aggregate_expression_phase(name) else {
                    return MetricHandled::No;
                };
                registry
                    .execution_aggregate_expression_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_attributes(
                        &LabelExtractor::new()
                            .with_extractor("aggregate", MetricAttribute::EXECUTION_EXPRESSION_NAME)
                            .extract(metric.labels(), registry),
                    )
                    .with_optional_attribute(MetricAttribute::EXECUTION_EXPRESSION_ID, Some(id))
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_AGGREGATE_PHASE,
                        Some(phase),
                    )
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

fn aggregate_expression_phase(name: &str) -> Option<(&str, &str)> {
    let (id, phase) = name
        .strip_prefix("agg_expr_")?
        .strip_suffix("_time")?
        .split_once('_')?;
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let known_phase = matches!(
        phase,
        "arguments" | "update" | "merge" | "state" | "convert_to_state" | "evaluate"
    );
    // AggregateMetrics permits each accumulator to define stable internal subphases.
    let internal_phase = phase
        .strip_prefix("internal_")
        .is_some_and(|s| !s.is_empty());
    (known_phase || internal_phase).then_some((id, phase))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{DataFusionError, Result, internal_err};
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::physical_plan::metrics::{Count, Label, MetricsSet, Time};
    use datafusion::prelude::{SessionConfig, SessionContext, col};
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    use super::*;
    use crate::execution::metrics::default::DefaultMetricEmitter;
    use crate::execution::metrics::testing::{
        MetricEmitterTester, export_metrics, exported_metrics,
    };

    #[test]
    fn aggregate_expression_times_preserve_phase_identity_and_seconds() -> Result<()> {
        let phases = [
            "arguments",
            "update",
            "merge",
            "state",
            "convert_to_state",
            "evaluate",
            "internal_distinct",
            "internal_sort",
        ];
        let mut metrics = MetricsSet::new();
        for id in [0, 12] {
            for phase in phases {
                let time = Time::new();
                time.add_duration(Duration::from_millis(1500));
                metrics.push(Arc::new(
                    Metric::new(
                        MetricValue::Time {
                            name: format!("agg_expr_{id}_{phase}_time").into(),
                            time,
                        },
                        Some(3),
                    )
                    .with_label(Label::new("aggregate", "SUM(a)")),
                ));
            }
        }
        let exported = export_metrics(
            &(AggregateMetricEmitter, DefaultMetricEmitter),
            &metrics,
            &[(MetricAttribute::EXECUTION_OPERATOR_ID, "aggregate".into())],
        )?;
        let metrics = exported_metrics(&exported).collect::<Vec<_>>();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name(), "execution.aggregate.expression_time");
        assert_eq!(metrics[0].unit(), "s");
        let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metrics[0].data() else {
            return internal_err!("expected an f64 expression-time gauge");
        };
        let mut actual = vec![];
        for point in gauge.data_points() {
            assert!((point.value() - 1.5).abs() < 1e-10);
            actual.push(
                point
                    .attributes()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            );
        }
        let mut expected = vec![];
        for id in [0, 12] {
            for phase in phases {
                expected.push(BTreeMap::from([
                    ("execution.operator.id".to_string(), "aggregate".to_string()),
                    ("execution.partition".to_string(), "3".to_string()),
                    ("execution.expression.id".to_string(), id.to_string()),
                    (
                        "execution.expression.name".to_string(),
                        "SUM(a)".to_string(),
                    ),
                    ("execution.aggregate.phase".to_string(), phase.to_string()),
                ]));
            }
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    #[cfg(debug_assertions)]
    fn unknown_aggregate_metrics_and_labels_are_not_suppressed() -> Result<()> {
        let mut metrics = MetricsSet::new();
        for name in [
            "agg_expr_x_update_time",
            "agg_expr__update_time",
            "agg_expr_0_future_time",
            "agg_expr_0_internal__time",
            "agg_expr_0_update_time_extra",
        ] {
            metrics.push(Arc::new(Metric::new(
                MetricValue::Time {
                    name: name.into(),
                    time: Time::new(),
                },
                Some(0),
            )));
        }
        metrics.push(Arc::new(Metric::new(
            MetricValue::Count {
                name: "agg_expr_0_update_time".into(),
                count: Count::new(),
            },
            Some(0),
        )));
        metrics.push(Arc::new(
            Metric::new(
                MetricValue::Time {
                    name: "agg_expr_0_update_time".into(),
                    time: Time::new(),
                },
                Some(0),
            )
            .with_label(Label::new("unknown_label", "value")),
        ));
        let exported = export_metrics(
            &(AggregateMetricEmitter, DefaultMetricEmitter),
            &metrics,
            &[],
        )?;
        let metrics = exported_metrics(&exported).collect::<Vec<_>>();
        assert_eq!(metrics.len(), 3);
        for metric in metrics {
            match metric.name() {
                "execution.unknown_metric_count" | "execution.unknown_metric_label_count" => {
                    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                        return internal_err!("expected an unknown metric counter");
                    };
                    let expected = if metric.name() == "execution.unknown_metric_count" {
                        6
                    } else {
                        1
                    };
                    assert_eq!(
                        sum.data_points().map(|p| p.value()).collect::<Vec<_>>(),
                        vec![expected]
                    );
                }
                "execution.aggregate.expression_time" => {}
                name => return internal_err!("unexpected metric: {name}"),
            }
        }
        Ok(())
    }

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
                    registry.execution_aggregate_expression_time.name(),
                    registry.execution_output_rows_skew.name(),
                    registry.execution_spill_count.name(),
                    registry.execution_spill_size.name(),
                    registry.execution_spill_row_count.name(),
                ]
            })
            .run()
            .await
    }
}
