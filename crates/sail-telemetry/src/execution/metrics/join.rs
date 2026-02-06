use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::Metric;

use crate::common::KeyValue;
use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A metric emitter for build-probe join operator metrics.
pub struct BuildProbeJoinMetricEmitter;

impl MetricEmitter for BuildProbeJoinMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::Time { name, time } if name == "build_time" => {
                registry
                    .execution_join_build_side_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Time { name, time } if name == "join_time" => {
                registry
                    .execution_join_operation_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "build_input_batches" => {
                registry
                    .execution_join_build_side_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "build_input_rows" => {
                registry
                    .execution_join_build_side_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Gauge { name, gauge } if name == "build_mem_used" => {
                registry
                    .execution_join_build_side_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "input_batches" => {
                registry
                    .execution_join_probe_side_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "input_rows" => {
                registry
                    .execution_join_probe_side_row_count
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
            } if name == "probe_hit_rate" => {
                registry
                    .execution_join_probe_side_matched_row_count
                    .recorder(ratio_metrics.part())
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                // We ignore the denominator (total) as it should be the same as
                // probe-side row count which is already emitted above.
                MetricHandled::Yes
            }
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } if name == "avg_fanout" => {
                registry
                    .execution_join_build_side_match_count
                    .recorder(ratio_metrics.part())
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                // We ignore the denominator (total) as it should be the same as
                // probe-side matched row count which is already emitted above.
                MetricHandled::Yes
            }
            _ => MetricHandled::No,
        }
    }
}

/// A metric emitter for nested loop join operator metrics.
/// This emitter should be used after [`BuildProbeJoinMetricEmitter`].
pub struct NestedLoopJoinMetricEmitter;

impl MetricEmitter for NestedLoopJoinMetricEmitter {
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
                    .execution_join_candidate_count
                    .recorder(ratio_metrics.total())
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                registry
                    .execution_join_output_row_count
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

/// A metric emitter for stream join operator metrics.
pub struct StreamJoinMetricEmitter;

impl MetricEmitter for StreamJoinMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::Count { name, count } if name == "left_input_batches" => {
                registry
                    .execution_join_left_input_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "left_input_rows" => {
                registry
                    .execution_join_left_input_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "right_input_batches" => {
                registry
                    .execution_join_right_input_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "right_input_rows" => {
                registry
                    .execution_join_right_input_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Gauge { name, gauge } if name == "stream_memory_usage" => {
                registry
                    .execution_join_memory_used
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

/// A metric emitter for sort-merge join operator metrics.
pub struct SortMergeJoinMetricEmitter;

impl MetricEmitter for SortMergeJoinMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::Time { name, time } if name == "join_time" => {
                registry
                    .execution_join_operation_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "input_batches" => {
                registry
                    .execution_join_input_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Count { name, count } if name == "input_rows" => {
                registry
                    .execution_join_input_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
                MetricHandled::Yes
            }
            MetricValue::Gauge { name, gauge } if name == "peak_mem_used" => {
                registry
                    .execution_join_memory_used
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
    use std::borrow::Cow;
    use std::sync::Arc;

    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality, Result};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{
        CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
        SortMergeJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
    };

    use crate::execution::metrics::testing::MetricEmitterTester;
    use crate::metrics::MetricRegistry;

    fn expected_build_probe_join_metrics(registry: &MetricRegistry) -> Vec<Cow<'static, str>> {
        vec![
            registry.execution_join_build_side_time.name(),
            registry.execution_join_operation_time.name(),
            registry.execution_join_build_side_batch_count.name(),
            registry.execution_join_build_side_row_count.name(),
            registry.execution_join_build_side_match_count.name(),
            registry.execution_join_build_side_memory_used.name(),
            registry.execution_join_probe_side_batch_count.name(),
            registry.execution_join_probe_side_row_count.name(),
            registry.execution_join_probe_side_matched_row_count.name(),
        ]
    }

    fn expected_nested_loop_join_metrics(registry: &MetricRegistry) -> Vec<Cow<'static, str>> {
        vec![
            registry.execution_join_candidate_count.name(),
            registry.execution_join_output_row_count.name(),
        ]
    }

    fn expected_stream_join_metrics(registry: &MetricRegistry) -> Vec<Cow<'static, str>> {
        vec![
            registry.execution_join_left_input_batch_count.name(),
            registry.execution_join_left_input_row_count.name(),
            registry.execution_join_right_input_batch_count.name(),
            registry.execution_join_right_input_row_count.name(),
            registry.execution_join_memory_used.name(),
        ]
    }

    fn expected_sort_merge_join_metrics(registry: &MetricRegistry) -> Vec<Cow<'static, str>> {
        vec![
            registry.execution_join_operation_time.name(),
            registry.execution_join_input_batch_count.name(),
            registry.execution_join_input_row_count.name(),
            registry.execution_join_memory_used.name(),
            registry.execution_spill_count.name(),
            registry.execution_spill_size.name(),
            registry.execution_spill_row_count.name(),
        ]
    }

    #[tokio::test]
    async fn test_hash_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(HashJoinExec::try_new(
            plan1,
            plan2,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 0)))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_build_probe_join_metrics)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_piecewise_merge_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(PiecewiseMergeJoinExec::try_new(
            plan1,
            plan2,
            (Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 0))),
            Operator::Gt,
            JoinType::Inner,
            1,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_build_probe_join_metrics)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_cross_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(CrossJoinExec::new(plan1, plan2));

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_build_probe_join_metrics)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_nested_loop_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            plan1,
            plan2,
            None,
            &JoinType::Inner,
            None,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_build_probe_join_metrics)
            .with_expected_metrics(expected_nested_loop_join_metrics)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_symmetric_hash_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(SymmetricHashJoinExec::try_new(
            plan1,
            plan2,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 0)))],
            None,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            None,
            None,
            StreamJoinPartitionMode::Partitioned,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_stream_join_metrics)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_sort_merge_join_metrics() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let plan1 = Arc::new(EmptyExec::new(schema1));
        let plan2 = Arc::new(EmptyExec::new(schema2));
        let plan = Arc::new(SortMergeJoinExec::try_new(
            plan1,
            plan2,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 0)))],
            None,
            JoinType::Inner,
            vec![SortOptions {
                descending: false,
                nulls_first: false,
            }],
            NullEquality::NullEqualsNothing,
        )?);

        MetricEmitterTester::new()
            .with_plan(plan)
            .with_expected_metrics(expected_sort_merge_join_metrics)
            .run()
            .await
    }
}
