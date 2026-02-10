use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{exec_err, internal_err, ColumnStatistics, Result, Statistics};
use futures::Stream;

#[derive(Debug, Clone)]
pub struct MonotonicIdExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MonotonicIdExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: impl Into<String>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let column_name = column_name.into();
        // Basic sanity: ensure the column exists in schema and is Int64
        let idx = schema.index_of(&column_name)?;
        let field = schema.field(idx);
        if field.data_type() != &DataType::Int64 {
            return exec_err!(
                "MonotonicIdExec expects Int64 field for {}, got {:?}",
                column_name,
                field.data_type()
            );
        }

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Both,
            input.boundedness(),
        );
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

impl DisplayAs for MonotonicIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MonotonicIdExec: col={}", self.column_name)
    }
}

impl ExecutionPlan for MonotonicIdExec {
    fn name(&self) -> &'static str {
        "MonotonicIdExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
            return internal_err!("MonotonicIdExec requires exactly one child");
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
        Ok(Box::pin(MonotonicIdStream::new(
            input,
            self.schema.clone(),
            self.column_name.clone(),
            partition,
        )?))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let mut stats = self.input.partition_statistics(partition)?;
        let col_idx = self.schema.index_of(&self.column_name)?;

        if col_idx < stats.column_statistics.len() {
            stats.column_statistics[col_idx] = ColumnStatistics::new_unknown();
        } else {
            while stats.column_statistics.len() < col_idx {
                stats
                    .column_statistics
                    .push(ColumnStatistics::new_unknown());
            }
            stats
                .column_statistics
                .push(ColumnStatistics::new_unknown());
        }

        // One additional Int64 output column contributes 8 bytes per row when row counts are known.
        let added_bytes = stats
            .num_rows
            .multiply(&Precision::Exact(std::mem::size_of::<i64>()));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);

        Ok(stats)
    }
}

struct MonotonicIdStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    col_idx: usize,
    partition: usize,
    offset: u64,
}

impl MonotonicIdStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        column_name: String,
        partition: usize,
    ) -> Result<Self> {
        let col_idx = schema.index_of(&column_name)?;
        Ok(Self {
            input,
            schema,
            col_idx,
            partition,
            offset: 0,
        })
    }

    fn make_ids(&mut self, len: usize) -> Result<ArrayRef> {
        // Spark: (partitionId << 33) + recordNumberWithinPartition
        let base = (self.partition as u64) << 33;
        if self.offset + (len as u64) > (1u64 << 33) {
            return exec_err!(
                "monotonically_increasing_id overflow: exceeded 2^33 rows in one partition"
            );
        }
        let start = self.offset;
        self.offset += len as u64;
        let values = (0..len)
            .map(|i| (base + start + (i as u64)) as i64)
            .collect::<Vec<_>>();
        Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
    }
}

impl RecordBatchStream for MonotonicIdStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MonotonicIdStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let mut cols = batch.columns().to_vec();
                let id_col = match self.make_ids(batch.num_rows()) {
                    Ok(c) => c,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                if self.col_idx >= cols.len() {
                    cols.push(id_col);
                } else {
                    cols[self.col_idx] = id_col;
                }
                Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(Into::into),
                ))
            }
        }
    }
}
