use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_datafusion_err, Result, Statistics};
use futures::StreamExt;
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::physical_plan::scan_by_adds_exec::map_statistics_to_schema;

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

    fn aligned_timestamp_columns(&self) -> Vec<String> {
        let input_schema = self.input.schema();
        self.schema
            .fields()
            .iter()
            .filter_map(|field| {
                let input_field = input_schema.field_with_name(field.name()).ok()?;
                match (input_field.data_type(), field.data_type()) {
                    (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                        if input_field.data_type() != field.data_type() =>
                    {
                        Some(field.name().clone())
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

impl DisplayAs for RelaxedTzCastExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let schema_changed = self.input.schema() != self.schema;
        let timestamp_columns = self.aligned_timestamp_columns().join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RelaxedTzCastExec(schema_changed={}, timestamp_columns=[{}])",
                    schema_changed, timestamp_columns
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "schema_changed={}, timestamp_columns=[{}]",
                    schema_changed, timestamp_columns
                )
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
            Ok(statistics)
        } else {
            Ok(map_statistics_to_schema(
                &statistics,
                &self.input.schema(),
                &self.schema,
            ))
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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

        assert_eq!(stats.num_rows, Precision::Exact(42));
        assert_eq!(stats.total_byte_size, Precision::Exact(1024));
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(1));
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(9)))
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
