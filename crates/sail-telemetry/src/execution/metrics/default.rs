use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{Label, MetricValue};
use num_traits::ToPrimitive;
use sail_common::telemetry::KeyValue;

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
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } if name == "output_rows_skew" && metric.partition().is_none() => {
                // This is an operator-wide score, not a per-partition row count.
                // A zero denominator denotes N/A, not a balanced distribution.
                let total = ratio_metrics.total();
                if total != 0
                    && let (Some(part), Some(total)) =
                        (ratio_metrics.part().to_f64(), total.to_f64())
                {
                    registry
                        .execution_output_rows_skew
                        .recorder(part / total)
                        .with_attributes(attributes)
                        .emit();
                }
            }
            // Peak-memory metrics are handled only by emitters that recognize both the physical
            // operator and the DataFusion metric name.
            MetricValue::PeakMemoryUsage { .. }
            | MetricValue::Count { .. }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::{Result, internal_err};
    use datafusion::physical_plan::metrics::{
        BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RatioMetrics,
    };
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    use super::*;
    use crate::execution::metrics::testing::{export_metrics, exported_metrics};

    #[test]
    fn output_rows_skew_preserves_ratio_and_operator_scope() -> Result<()> {
        for (rows, expected) in [
            (vec![], None),
            (vec![0], Some(0.0)),
            (vec![0, 0], None),
            (vec![10, 10, 10, 10], Some(0.0)),
            (vec![40, 0, 0, 0], Some(1.0)),
            (vec![10, 30], Some(0.4)),
        ] {
            let upstream = ExecutionPlanMetricsSet::new();
            for (partition, rows) in rows.into_iter().enumerate() {
                MetricBuilder::new(&upstream)
                    .output_rows(partition)
                    .add(rows);
            }
            let mut metrics = MetricsSet::new();
            if let Some(skew) = BaselineMetrics::output_rows_skew_metric(&upstream.clone_inner()) {
                metrics.push(skew);
            }
            let exported = export_metrics(
                &DefaultMetricEmitter,
                &metrics,
                &[(MetricAttribute::EXECUTION_OPERATOR_ID, "operator".into())],
            )?;
            let metrics = exported_metrics(&exported).collect::<Vec<_>>();
            let Some(expected) = expected else {
                assert!(metrics.is_empty());
                continue;
            };
            assert_eq!(metrics.len(), 1);
            let metric = metrics[0];
            assert_eq!(metric.name(), "execution.output_rows_skew");
            assert_eq!(metric.unit(), "1");
            let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data() else {
                return internal_err!("expected an f64 skew gauge");
            };
            let points = gauge.data_points().collect::<Vec<_>>();
            assert_eq!(points.len(), 1);
            assert!((points[0].value() - expected).abs() < 1e-10);
            assert_eq!(
                points[0].attributes().cloned().collect::<Vec<_>>(),
                vec![opentelemetry::KeyValue::new(
                    MetricAttribute::EXECUTION_OPERATOR_ID,
                    "operator"
                )]
            );
        }
        Ok(())
    }

    #[test]
    #[cfg(debug_assertions)]
    fn unknown_names_types_and_partitioned_skew_remain_unknown() -> Result<()> {
        let mut metrics = MetricsSet::new();
        for (value, partition) in [
            (
                MetricValue::Count {
                    name: "output_rows_skew".into(),
                    count: Count::new(),
                },
                None,
            ),
            (
                MetricValue::Ratio {
                    name: "unknown_ratio".into(),
                    ratio_metrics: RatioMetrics::new(),
                },
                None,
            ),
            (
                MetricValue::Ratio {
                    name: "output_rows_skew".into(),
                    ratio_metrics: RatioMetrics::new(),
                },
                Some(0),
            ),
            (
                MetricValue::PeakMemoryUsage {
                    name: "peak_mem_used".into(),
                    gauge: Default::default(),
                },
                None,
            ),
        ] {
            metrics.push(Arc::new(Metric::new(value, partition)));
        }
        let exported = export_metrics(&DefaultMetricEmitter, &metrics, &[])?;
        let metrics = exported_metrics(&exported).collect::<Vec<_>>();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name(), "execution.unknown_metric_count");
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metrics[0].data() else {
            return internal_err!("expected an unknown metric counter");
        };
        assert_eq!(
            sum.data_points().map(|p| p.value()).collect::<Vec<_>>(),
            vec![4]
        );
        Ok(())
    }
}
