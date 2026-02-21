use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{internal_err, ColumnStatistics, Result, Statistics};
use futures::Stream;
use sail_function::scalar::math::xorshift::SparkXorShiftRandom;
use sail_logical_plan::rand::RandMode;

#[derive(Debug, Clone)]
pub struct RandExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: String,
    seed: i64,
    mode: RandMode,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl RandExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: impl Into<String>,
        seed: i64,
        mode: RandMode,
        schema: SchemaRef,
    ) -> Result<Self> {
        let column_name = column_name.into();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Ok(Self {
            input,
            column_name,
            seed,
            mode,
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

    pub fn seed(&self) -> i64 {
        self.seed
    }

    pub fn mode(&self) -> &RandMode {
        &self.mode
    }
}

impl DisplayAs for RandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RandExec: col={}, seed={}, mode={:?}",
            self.column_name, self.seed, self.mode
        )
    }
}

impl ExecutionPlan for RandExec {
    fn name(&self) -> &'static str {
        "RandExec"
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
            return internal_err!("RandExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.column_name.clone(),
            self.seed,
            self.mode.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        // Match Spark: seed + partitionIndex
        let rng = SparkXorShiftRandom::new(self.seed + partition as i64);
        Ok(Box::pin(RandStream::new(
            input,
            self.schema.clone(),
            self.column_name.clone(),
            rng,
            self.mode.clone(),
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
        // One Float64 column = 8 bytes per row.
        let added_bytes = stats
            .num_rows
            .multiply(&Precision::Exact(std::mem::size_of::<f64>()));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);
        Ok(stats)
    }
}

struct RandStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    col_idx: usize,
    rng: SparkXorShiftRandom,
    mode: RandMode,
}

impl RandStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        column_name: String,
        rng: SparkXorShiftRandom,
        mode: RandMode,
    ) -> Result<Self> {
        let col_idx = schema.index_of(&column_name)?;
        Ok(Self {
            input,
            schema,
            col_idx,
            rng,
            mode,
        })
    }

    fn make_random_column(&mut self, len: usize) -> ArrayRef {
        let values: Vec<f64> = match self.mode {
            RandMode::Uniform => (0..len).map(|_| self.rng.next_double()).collect(),
            RandMode::Gaussian => (0..len).map(|_| self.rng.next_gaussian()).collect(),
        };
        Arc::new(Float64Array::from(values)) as ArrayRef
    }
}

impl RecordBatchStream for RandStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RandStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let mut cols = batch.columns().to_vec();
                let rand_col = self.make_random_column(batch.num_rows());
                if self.col_idx > cols.len() {
                    return Poll::Ready(Some(internal_err!(
                        "RandExec output column index {0} exceeds input column count {1}",
                        self.col_idx,
                        cols.len()
                    )));
                }
                cols.insert(self.col_idx, rand_col);
                Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(Into::into),
                ))
            }
        }
    }
}
