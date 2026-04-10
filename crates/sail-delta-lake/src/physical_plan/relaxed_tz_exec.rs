use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::{internal_datafusion_err, Result, ScalarValue, Statistics};
use futures::StreamExt;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone)]
pub struct RelaxedTzCastExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RelaxedTzCastExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            schema,
            properties,
        }
    }

    /// Returns per-column retag info: `(column_name, from_tz, to_tz)` for every
    /// timestamp column whose timezone label is changed by this node.
    fn timestamp_retag_info(&self) -> Vec<(String, String, String)> {
        let input_schema = self.input.schema();
        self.schema
            .fields()
            .iter()
            .filter_map(|field| {
                let input_field = input_schema.field_with_name(field.name()).ok()?;
                match (input_field.data_type(), field.data_type()) {
                    (DataType::Timestamp(_, src_tz), DataType::Timestamp(_, tgt_tz))
                        if input_field.data_type() != field.data_type() =>
                    {
                        let from = src_tz.as_deref().unwrap_or("none").to_string();
                        let to = tgt_tz.as_deref().unwrap_or("none").to_string();
                        Some((field.name().clone(), from, to))
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

impl DisplayAs for RelaxedTzCastExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let retag_info = self.timestamp_retag_info();
        let retag_str = retag_info
            .iter()
            .map(|(col, from, to)| format!("{col}: {from} -> {to}"))
            .collect::<Vec<_>>()
            .join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RelaxedTzCastExec: retag [{}]", retag_str)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "retag [{}]", retag_str)
            }
        }
    }
}

impl ExecutionPlan for RelaxedTzCastExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.one().map_err(|_| {
            internal_datafusion_err!("RelaxedTzCastExec must have exactly one child")
        })?;
        Ok(Arc::new(Self::new(input, Arc::clone(&self.schema))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let stream = self.input.execute(partition, context)?;
        let stream = stream.map(move |batch| {
            let schema = Arc::clone(&schema);
            batch.and_then(|batch| cast_record_batch_relaxed_tz(&batch, &schema))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let statistics = self.input.partition_statistics(partition)?;
        if self.input.schema() == self.schema {
            return Ok(statistics);
        }

        let input_schema = self.input.schema();
        // Pre-build a name→index map to avoid O(n²) linear scans in the loop below.
        let name_to_idx: HashMap<&str, usize> = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().as_str(), i))
            .collect();

        let column_statistics = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let Some(&input_idx) = name_to_idx.get(field.name().as_str()) else {
                    return ColumnStatistics::new_unknown();
                };
                let input_field = input_schema.field(input_idx);
                let Some(col_stats) = statistics.column_statistics.get(input_idx).cloned() else {
                    return ColumnStatistics::new_unknown();
                };

                // For relaxed-tz retagging, retag the timezone label on timestamp bounds
                // instead of converting the underlying epoch values via cast_to.
                match (input_field.data_type(), field.data_type()) {
                    (DataType::Timestamp(_, _), DataType::Timestamp(_, target_tz)) => {
                        ColumnStatistics {
                            min_value: retag_scalar_tz_bound(
                                &col_stats.min_value,
                                target_tz.clone(),
                            ),
                            max_value: retag_scalar_tz_bound(
                                &col_stats.max_value,
                                target_tz.clone(),
                            ),
                            ..col_stats
                        }
                    }
                    _ => col_stats,
                }
            })
            .collect();

        Ok(Statistics {
            num_rows: statistics.num_rows,
            total_byte_size: statistics.total_byte_size,
            column_statistics,
        })
    }
}

/// Retag the timezone label of a `Precision<ScalarValue>` timestamp bound without
/// converting the underlying epoch value.  Non-timestamp values are returned unchanged.
fn retag_scalar_tz_bound(
    bound: &Precision<ScalarValue>,
    target_tz: Option<Arc<str>>,
) -> Precision<ScalarValue> {
    match bound {
        Precision::Exact(v) => Precision::Exact(retag_scalar_tz(v, target_tz)),
        Precision::Inexact(v) => Precision::Inexact(retag_scalar_tz(v, target_tz)),
        Precision::Absent => Precision::Absent,
    }
}

fn retag_scalar_tz(value: &ScalarValue, target_tz: Option<Arc<str>>) -> ScalarValue {
    match value {
        ScalarValue::TimestampSecond(v, _) => ScalarValue::TimestampSecond(*v, target_tz),
        ScalarValue::TimestampMillisecond(v, _) => ScalarValue::TimestampMillisecond(*v, target_tz),
        ScalarValue::TimestampMicrosecond(v, _) => ScalarValue::TimestampMicrosecond(*v, target_tz),
        ScalarValue::TimestampNanosecond(v, _) => ScalarValue::TimestampNanosecond(*v, target_tz),
        _ => value.clone(),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::Partitioning;
    use datafusion_common::stats::{ColumnStatistics, Precision};
    use datafusion_common::ScalarValue;
    use futures::{stream, StreamExt};

    use super::*;

    #[test]
    fn relaxed_tz_cast_exec_retags_timestamp_batches() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        )]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("America/Los_Angeles")),
            ),
            true,
        )]));
        let input_batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_714_566_400_000_000)])
                    .with_timezone_opt(Some(Arc::from("UTC"))),
            )],
        )?;
        let input = Arc::new(TestExec::new(Arc::clone(&input_schema), vec![input_batch]));
        let exec = RelaxedTzCastExec::new(input, Arc::clone(&target_schema));

        let task_ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, task_ctx)?;
        let batches = stream.collect::<Vec<_>>();
        let batches = futures::executor::block_on(batches);
        let batch = batches.into_iter().next().unwrap()?;

        assert_eq!(batch.schema(), target_schema);
        assert_eq!(
            batch.column(0).data_type(),
            target_schema.field(0).data_type()
        );
        Ok(())
    }

    #[test]
    fn relaxed_tz_cast_exec_remaps_statistics_to_output_schema() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from("America/Los_Angeles")),
                ),
                true,
            ),
        ]));
        let statistics = Statistics {
            num_rows: Precision::Exact(42),
            total_byte_size: Precision::Exact(1024),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(9))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(7),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::TimestampMicrosecond(
                        Some(2_000_000),
                        Some(Arc::from("UTC")),
                    )),
                    min_value: Precision::Exact(ScalarValue::TimestampMicrosecond(
                        Some(1_000_000),
                        Some(Arc::from("UTC")),
                    )),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
            ],
        };
        let input = Arc::new(TestExec::with_statistics(
            Arc::clone(&input_schema),
            vec![],
            statistics,
        ));
        let exec = RelaxedTzCastExec::new(input, Arc::clone(&target_schema));

        let stats = exec.partition_statistics(None)?;

        // Non-timestamp column statistics must be fully preserved.
        assert_eq!(stats.num_rows, Precision::Exact(42));
        assert_eq!(stats.total_byte_size, Precision::Exact(1024));
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(1));
        assert_eq!(
            stats.column_statistics[0].distinct_count,
            Precision::Exact(7)
        );
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(9)))
        );

        // Timestamp column statistics must be retagged (same epoch value, new TZ label).
        assert_eq!(stats.column_statistics[1].null_count, Precision::Exact(0));
        assert_eq!(
            stats.column_statistics[1].min_value,
            Precision::Exact(ScalarValue::TimestampMicrosecond(
                Some(1_000_000),
                Some(Arc::from("America/Los_Angeles")),
            ))
        );
        assert_eq!(
            stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::TimestampMicrosecond(
                Some(2_000_000),
                Some(Arc::from("America/Los_Angeles")),
            ))
        );

        Ok(())
    }

    #[test]
    fn relaxed_tz_cast_exec_fmt_shows_retag_info() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from("America/Los_Angeles")),
                ),
                true,
            ),
        ]));
        let input = Arc::new(TestExec::new(Arc::clone(&input_schema), vec![]));
        let exec = RelaxedTzCastExec::new(input, Arc::clone(&target_schema));

        let default_fmt = DisplayableExecutionPlan::new(&exec)
            .indent(false)
            .to_string();
        let first_line = default_fmt.lines().next().unwrap();
        assert!(
            first_line.contains("RelaxedTzCastExec"),
            "expected RelaxedTzCastExec in fmt output, got: {first_line}"
        );
        assert!(
            first_line.contains("event_time"),
            "expected column name in fmt output, got: {first_line}"
        );
        assert!(
            first_line.contains("UTC"),
            "expected source timezone in fmt output, got: {first_line}"
        );
        assert!(
            first_line.contains("America/Los_Angeles"),
            "expected target timezone in fmt output, got: {first_line}"
        );

        Ok(())
    }

    #[test]
    fn relaxed_tz_cast_exec_passthrough_when_no_timestamp_columns() -> Result<()> {
        // When both schemas are identical (no retagging needed), statistics are passed through.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let statistics = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(256),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics::new_unknown(),
            ],
        };
        let input = Arc::new(TestExec::with_statistics(
            Arc::clone(&schema),
            vec![],
            statistics.clone(),
        ));
        let exec = RelaxedTzCastExec::new(input, Arc::clone(&schema));

        let stats = exec.partition_statistics(None)?;
        assert_eq!(stats.num_rows, Precision::Exact(10));
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        Ok(())
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        statistics: Statistics,
        properties: Arc<PlanProperties>,
    }

    impl TestExec {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self::with_statistics(
                schema.clone(),
                batches,
                Statistics::new_unknown(schema.as_ref()),
            )
        }

        fn with_statistics(
            schema: SchemaRef,
            batches: Vec<RecordBatch>,
            statistics: Statistics,
        ) -> Self {
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                batches,
                statistics,
                properties,
            }
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "TestExec")
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            "TestExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return Err(internal_datafusion_err!(
                    "TestExec does not accept children"
                ));
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if partition != 0 {
                return Err(internal_datafusion_err!("TestExec has only one partition"));
            }
            let schema = Arc::clone(&self.schema);
            let stream = stream::iter(self.batches.clone().into_iter().map(Ok));
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }

        fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
            if partition.is_none() {
                Ok(self.statistics.clone())
            } else {
                Ok(Statistics::new_unknown(self.schema.as_ref()))
            }
        }
    }
}
