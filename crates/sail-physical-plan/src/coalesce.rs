use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{internal_err, plan_err, Result, Statistics};
use futures::Stream;

/// A physical plan node for Spark-style coalesce without a shuffle.
#[derive(Debug)]
pub struct CoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    output_partitions: usize,
    properties: Arc<PlanProperties>,
}

impl CoalesceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, output_partitions: usize) -> Self {
        let mut eq_properties = input.equivalence_properties().clone();
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_orderings();
            eq_properties.clear_per_partition_constants();
        }
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(output_partitions),
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Self {
            input,
            output_partitions,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn output_partitions(&self) -> usize {
        self.output_partitions
    }
}

impl DisplayAs for CoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CoalesceExec: partitions={}", self.output_partitions)
    }
}

impl ExecutionPlan for CoalesceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(Self::new(child, self.output_partitions)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.output_partitions {
            return internal_err!(
                "{} partition index {partition} out of range for {} output partitions",
                self.name(),
                self.output_partitions
            );
        }

        let input_partition_count = self.input.output_partitioning().partition_count();
        if self.output_partitions > input_partition_count {
            return internal_err!(
                "{} cannot increase partition count from {} to {}",
                self.name(),
                input_partition_count,
                self.output_partitions
            );
        }

        let (start, end) =
            coalesced_input_range(partition, input_partition_count, self.output_partitions);
        let streams = (start..end)
            .map(|index| self.input.execute(index, context.clone()))
            .collect::<Result<VecDeque<_>>>()?;

        Ok(Box::pin(CoalesceStream {
            schema: self.schema(),
            inputs: streams,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            self.input.partition_statistics(None)
        } else {
            Ok(Statistics::new_unknown(&self.schema()))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

// Keep coalescing deterministic by assigning each output partition a contiguous range.
// Unlike Spark Core's locality-aware CoalescedRDD packing, this path does not use
// preferred-location metadata because DataFusion does not expose it.
fn coalesced_input_range(
    partition: usize,
    input_partitions: usize,
    output_partitions: usize,
) -> (usize, usize) {
    let start = partition * input_partitions / output_partitions;
    let end = (partition + 1) * input_partitions / output_partitions;
    (start, end)
}

struct CoalesceStream {
    schema: SchemaRef,
    inputs: VecDeque<SendableRecordBatchStream>,
}

impl RecordBatchStream for CoalesceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let poll = {
                let Some(input) = self.inputs.front_mut() else {
                    return Poll::Ready(None);
                };
                input.as_mut().poll_next(cx)
            };

            match poll {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.inputs.pop_front();
                }
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use futures::StreamExt;

    use super::{coalesced_input_range, CoalesceExec};

    fn test_plan(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
        let schema = if let Some(batch) = partitions.iter().flatten().next() {
            batch.schema()
        } else {
            test_schema()
        };
        let table = MemTable::try_new(schema, partitions).unwrap();
        let ctx = SessionContext::new();
        futures::executor::block_on(table.scan(&ctx.state(), None, &[], None)).unwrap()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn test_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn collect_values(exec: &CoalesceExec, partition: usize) -> Result<Vec<i32>> {
        let stream = exec.execute(partition, Arc::new(TaskContext::default()))?;
        let batches = futures::executor::block_on(stream.collect::<Vec<_>>());
        let mut values = vec![];
        for batch in batches {
            let batch = batch?;
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(array.values().iter().copied());
        }
        Ok(values)
    }

    #[test]
    fn test_coalesce_exec_reads_contiguous_input_ranges() -> Result<()> {
        let input = test_plan(vec![
            vec![test_batch(&[0, 1])],
            vec![test_batch(&[2, 3])],
            vec![test_batch(&[4, 5])],
            vec![test_batch(&[6, 7])],
        ]);
        let exec = CoalesceExec::new(input, 2);

        assert_eq!(collect_values(&exec, 0)?, vec![0, 1, 2, 3]);
        assert_eq!(collect_values(&exec, 1)?, vec![4, 5, 6, 7]);

        Ok(())
    }

    #[test]
    fn test_coalesced_input_range_even_groups() {
        assert_eq!(coalesced_input_range(0, 4, 2), (0, 2));
        assert_eq!(coalesced_input_range(1, 4, 2), (2, 4));
    }

    #[test]
    fn test_coalesced_input_range_uneven_groups() {
        assert_eq!(coalesced_input_range(0, 5, 3), (0, 1));
        assert_eq!(coalesced_input_range(1, 5, 3), (1, 3));
        assert_eq!(coalesced_input_range(2, 5, 3), (3, 5));
    }

    #[test]
    fn test_coalesce_exec_merges_even_partition_groups() -> Result<()> {
        let input = test_plan(vec![
            vec![test_batch(&[0, 1])],
            vec![test_batch(&[2])],
            vec![test_batch(&[3, 4])],
            vec![test_batch(&[5])],
        ]);
        let exec = CoalesceExec::new(input, 2);

        assert_eq!(collect_values(&exec, 0)?, vec![0, 1, 2]);
        assert_eq!(collect_values(&exec, 1)?, vec![3, 4, 5]);

        Ok(())
    }

    #[test]
    fn test_coalesce_exec_merges_uneven_partition_groups() -> Result<()> {
        let input = test_plan(vec![
            vec![test_batch(&[0])],
            vec![test_batch(&[1])],
            vec![test_batch(&[2])],
            vec![test_batch(&[3])],
            vec![test_batch(&[4])],
        ]);
        let exec = CoalesceExec::new(input, 3);

        assert_eq!(collect_values(&exec, 0)?, vec![0]);
        assert_eq!(collect_values(&exec, 1)?, vec![1, 2]);
        assert_eq!(collect_values(&exec, 2)?, vec![3, 4]);

        Ok(())
    }

    #[test]
    fn test_coalesce_exec_rejects_out_of_range_partition() {
        let input = test_plan(vec![vec![test_batch(&[0])], vec![test_batch(&[1])]]);
        let exec = CoalesceExec::new(input, 2);

        assert!(matches!(
            exec.execute(2, Arc::new(TaskContext::default())),
            Err(error) if error.to_string().contains("out of range")
        ));
    }

    #[test]
    fn test_coalesce_exec_rejects_partition_increase() {
        let input = test_plan(vec![vec![test_batch(&[0])], vec![test_batch(&[1])]]);
        let exec = CoalesceExec::new(input, 3);

        assert!(matches!(
            exec.execute(0, Arc::new(TaskContext::default())),
            Err(error) if error.to_string().contains("cannot increase partition count")
        ));
    }
}
