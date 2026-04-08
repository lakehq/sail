use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_datafusion_err, Result, Statistics};
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
}

impl DisplayAs for RelaxedTzCastExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
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
        self.input.partition_statistics(partition)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::Partitioning;
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

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        properties: Arc<PlanProperties>,
    }

    impl TestExec {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                batches,
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
    }
}
