use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{exec_err, internal_err, ColumnStatistics, Result, Statistics};
use futures::Stream;

#[derive(Debug, Clone)]
pub struct SparkPartitionIdExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SparkPartitionIdExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: impl Into<String>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let column_name = column_name.into();
        // Basic sanity: ensure the column exists in schema and is Int32
        let idx = schema.index_of(&column_name)?;
        let field = schema.field(idx);
        if field.data_type() != &DataType::Int32 {
            return exec_err!(
                "SparkPartitionIdExec expects Int32 field for {}, got {:?}",
                column_name,
                field.data_type()
            );
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            column_name,
            schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl DisplayAs for SparkPartitionIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SparkPartitionIdExec: col={}", self.column_name)
    }
}

impl ExecutionPlan for SparkPartitionIdExec {
    fn name(&self) -> &'static str {
        "SparkPartitionIdExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("SparkPartitionIdExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.column_name.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SparkPartitionIdStream::new(
            input,
            self.schema.clone(),
            self.column_name.clone(),
            partition,
        )?))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let mut stats = self.input.partition_statistics(partition)?;
        let col_idx = self.schema.index_of(&self.column_name)?;
        let unknown_col_stats = ColumnStatistics::new_unknown();
        if col_idx <= stats.column_statistics.len() {
            stats.column_statistics.insert(col_idx, unknown_col_stats);
        } else {
            while stats.column_statistics.len() < col_idx {
                stats
                    .column_statistics
                    .push(ColumnStatistics::new_unknown());
            }
            stats.column_statistics.push(unknown_col_stats);
        }

        // One additional Int32 output column contributes 4 bytes per row when row counts are known.
        let added_bytes = stats
            .num_rows
            .multiply(&Precision::Exact(std::mem::size_of::<i32>()));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);

        Ok(stats)
    }
}

struct SparkPartitionIdStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    col_idx: usize,
    partition: i32,
}

impl SparkPartitionIdStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        column_name: String,
        partition: usize,
    ) -> Result<Self> {
        let col_idx = schema.index_of(&column_name)?;
        let partition = i32::try_from(partition).map_err(|_| {
            datafusion_common::DataFusionError::Execution(format!(
                "spark_partition_id: partition index {partition} does not fit in i32"
            ))
        })?;
        Ok(Self {
            input,
            schema,
            col_idx,
            partition,
        })
    }

    fn make_ids(&self, len: usize) -> ArrayRef {
        Arc::new(Int32Array::from_value(self.partition, len)) as ArrayRef
    }
}

impl RecordBatchStream for SparkPartitionIdStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SparkPartitionIdStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let mut cols = batch.columns().to_vec();
                let id_col = self.make_ids(batch.num_rows());
                if self.col_idx > cols.len() {
                    return Poll::Ready(Some(internal_err!(
                        "SparkPartitionIdExec output column index {0} exceeds input column count {1}",
                        self.col_idx,
                        cols.len()
                    )));
                }
                cols.insert(self.col_idx, id_col);
                Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(Into::into),
                ))
            }
        }
    }
}
