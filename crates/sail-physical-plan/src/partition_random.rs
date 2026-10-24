use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, Statistics, exec_err, internal_err};
use futures::Stream;
use sail_function::scalar::math::xorshift::SparkXorShiftRandom;

/// Appends a column of random numbers in `[0, 1)`, drawn from Spark's `XORShiftRandom`
/// seeded with `seed + partition index` and advanced across the batches of the partition,
/// as Spark's `rand(seed)` and Bernoulli sampler do.
#[derive(Debug, Clone)]
pub struct PartitionRandomExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: String,
    seed: i64,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PartitionRandomExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: impl Into<String>,
        seed: i64,
        schema: SchemaRef,
    ) -> Result<Self> {
        let column_name = column_name.into();
        let idx = schema.index_of(&column_name)?;
        let field = schema.field(idx);
        if field.data_type() != &DataType::Float64 {
            return exec_err!(
                "PartitionRandomExec expects Float64 field for {}, got {:?}",
                column_name,
                field.data_type()
            );
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone())
                .extend(input.equivalence_properties().clone())?,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            column_name,
            seed,
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
}

impl DisplayAs for PartitionRandomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionRandomExec: col={}, seed={}",
            self.column_name, self.seed
        )
    }
}

impl ExecutionPlan for PartitionRandomExec {
    fn name(&self) -> &'static str {
        "PartitionRandomExec"
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("PartitionRandomExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.column_name.clone(),
            self.seed,
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let col_idx = self.schema.index_of(&self.column_name)?;
        // Spark adds the partition index to the seed as a `Long`, which wraps on overflow.
        let seed = self.seed.wrapping_add(partition as i64);
        Ok(Box::pin(PartitionRandomStream {
            input,
            schema: self.schema.clone(),
            col_idx,
            rng: SparkXorShiftRandom::new(seed),
        }))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let mut stats = input_stats[0].as_ref().clone();
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

        // One additional Float64 output column contributes 8 bytes per row when row counts are known.
        let added_bytes = stats
            .num_rows
            .multiply(&Precision::Exact(std::mem::size_of::<f64>()));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);

        Ok(Arc::new(stats))
    }
}

struct PartitionRandomStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    col_idx: usize,
    rng: SparkXorShiftRandom,
}

impl RecordBatchStream for PartitionRandomStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for PartitionRandomStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let mut cols = batch.columns().to_vec();
                if self.col_idx > cols.len() {
                    return Poll::Ready(Some(internal_err!(
                        "PartitionRandomExec output column index {0} exceeds input column count {1}",
                        self.col_idx,
                        cols.len()
                    )));
                }
                let rng = &mut self.rng;
                let values = Float64Array::from_iter_values(
                    std::iter::repeat_with(|| rng.next_double()).take(batch.num_rows()),
                );
                cols.insert(self.col_idx, Arc::new(values) as ArrayRef);
                Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(Into::into),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::common::collect;

    use super::*;

    fn draws(seed: i64, n: usize) -> Vec<f64> {
        let mut rng = SparkXorShiftRandom::new(seed);
        (0..n).map(|_| rng.next_double()).collect()
    }

    #[tokio::test]
    async fn draws_continue_across_batches_and_seed_by_partition() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = |values: Vec<i64>| {
            RecordBatch::try_new(
                input_schema.clone(),
                vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            )
        };
        let partitions = vec![
            vec![batch(vec![1, 2])?, batch(vec![3])?],
            vec![batch(vec![4, 5, 6])?],
        ];
        let input = MemorySourceConfig::try_new_exec(&partitions, input_schema, None)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("r", DataType::Float64, false),
        ]));
        let exec = Arc::new(PartitionRandomExec::try_new(input, "r", 7, schema)?);
        for (partition, seed) in [(0, 7), (1, 8)] {
            let batches =
                collect(exec.execute(partition, Arc::new(TaskContext::default()))?).await?;
            let actual: Vec<f64> = batches
                .iter()
                .flat_map(|b| {
                    b.column(1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|a| a.values().to_vec())
                        .unwrap_or_default()
                })
                .collect();
            assert_eq!(actual, draws(seed, 3));
        }
        Ok(())
    }
}
