use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, Float32Array, RecordBatch, UInt64Array};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::compute::{interleave_record_batch, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr, SplitPoint};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::utils::compare_rows;
use datafusion_common::{Result, ScalarValue, internal_err};
use futures::{StreamExt, TryStreamExt};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sail_common_datafusion::array::compact::compact_array;

const WEIGHT_COLUMN: &str = "__weight";

/// Samples the range keys of each input partition for range repartitioning, like the
/// sketch step of Spark's `RangePartitioner`.
///
/// Each input partition keeps a reservoir sample of at most `sample_size` rows and
/// emits it with a weight column: the number of rows the partition read divided by
/// the number of rows it kept. An empty partition emits nothing.
/// <https://github.com/apache/spark/blob/v4.1.0/core/src/main/scala/org/apache/spark/Partitioner.scala>
#[derive(Debug, Clone)]
pub struct RangePartitionSampleExec {
    input: Arc<dyn ExecutionPlan>,
    sample_size: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RangePartitionSampleExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, sample_size: usize) -> Result<Self> {
        if sample_size == 0 {
            return internal_err!("range partition sample size must be positive");
        }
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            WEIGHT_COLUMN,
            DataType::Float32,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sample_size,
            schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sample_size(&self) -> usize {
        self.sample_size
    }
}

impl DisplayAs for RangePartitionSampleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangePartitionSampleExec: sample_size={}",
            self.sample_size
        )
    }
}

impl ExecutionPlan for RangePartitionSampleExec {
    fn name(&self) -> &'static str {
        "RangePartitionSampleExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            return internal_err!("RangePartitionSampleExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.sample_size,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let sample_size = self.sample_size;
        let schema = Arc::clone(&self.schema);
        let output = futures::stream::once(sample_partition(
            input,
            sample_size,
            partition,
            Arc::clone(&schema),
        ))
        .try_filter_map(|batch| async move { Ok(batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

/// Reservoir sampling (Algorithm R) over one input partition.
async fn sample_partition(
    mut input: SendableRecordBatchStream,
    sample_size: usize,
    partition: usize,
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    let mut reservoir = Reservoir::new(sample_size, partition as u64);
    while let Some(batch) = input.next().await {
        reservoir.insert(&batch?)?;
    }
    let count = reservoir.count;
    let Some(sample) = reservoir.finish()? else {
        return Ok(None);
    };
    // Spark computes the weight in double precision and keeps it as a float.
    let weight = (count as f64 / sample.num_rows() as f64) as f32;
    let mut columns = sample.columns().to_vec();
    columns.push(Arc::new(Float32Array::from(vec![
        weight;
        sample.num_rows()
    ])));
    Ok(Some(RecordBatch::try_new(schema, columns)?))
}

/// The rows kept by reservoir sampling.
///
/// Each input batch adds one small batch with only the rows picked from it, so the
/// work per input batch depends on the rows picked, not on the sample size.
/// Every stored batch is compacted, so the sample never holds the buffers of an
/// input batch. The rows that are no longer in the sample are dropped when the
/// stored rows exceed twice the sample size.
struct Reservoir {
    sample_size: usize,
    /// The seed only needs to keep the sample the same across runs on the same input.
    rng: StdRng,
    /// The number of input rows seen.
    count: u64,
    /// The stored batches, including rows that are no longer in the sample.
    batches: Vec<RecordBatch>,
    /// The number of rows in `batches`.
    stored_rows: usize,
    /// For each slot of the sample, the stored batch and the row in it.
    slots: Vec<(usize, usize)>,
}

impl Reservoir {
    fn new(sample_size: usize, seed: u64) -> Self {
        Self {
            sample_size,
            rng: StdRng::seed_from_u64(seed),
            count: 0,
            batches: vec![],
            stored_rows: 0,
            slots: vec![],
        }
    }

    fn insert(&mut self, batch: &RecordBatch) -> Result<()> {
        // The picked rows go to a new stored batch at index `next`, in the order picked.
        let next = self.batches.len();
        let mut picked: Vec<u64> = vec![];
        for row in 0..batch.num_rows() {
            self.count += 1;
            let slot = if self.slots.len() < self.sample_size {
                self.slots.push((next, 0));
                self.slots.len() - 1
            } else {
                match usize::try_from(self.rng.random_range(0..self.count)) {
                    Ok(slot) if slot < self.sample_size => slot,
                    _ => continue,
                }
            };
            self.slots[slot] = (next, picked.len());
            picked.push(row as u64);
        }
        if picked.is_empty() {
            return Ok(());
        }
        let picked = take_record_batch(batch, &UInt64Array::from(picked))?;
        self.stored_rows += picked.num_rows();
        self.batches.push(compact_record_batch(picked)?);
        if self.stored_rows > 2 * self.sample_size {
            self.compact()?;
        }
        Ok(())
    }

    /// Keeps only the rows in the sample, in one batch in slot order.
    fn compact(&mut self) -> Result<()> {
        let batches = self.batches.iter().collect::<Vec<_>>();
        let sample = compact_record_batch(interleave_record_batch(&batches, &self.slots)?)?;
        self.stored_rows = sample.num_rows();
        self.batches = vec![sample];
        self.slots = (0..self.stored_rows).map(|row| (0, row)).collect();
        Ok(())
    }

    /// Returns the sample in slot order, or `None` if no row was seen.
    fn finish(mut self) -> Result<Option<RecordBatch>> {
        if self.slots.is_empty() {
            return Ok(None);
        }
        self.compact()?;
        Ok(self.batches.pop())
    }
}

/// Copies the bytes that a batch shares with its source, such as view buffers.
fn compact_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| compact_array(Arc::clone(column)))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Picks range split points from the batches of [`RangePartitionSampleExec`], like
/// `RangePartitioner.determineBounds` in Spark, for at most `partitions` partitions.
///
/// Spark puts a row equal to a bound in the partition below the bound, while
/// DataFusion puts a row equal to a split point in the partition above it.
/// So each split point is the smallest sampled key greater than its Spark bound.
/// A bound at the largest sampled key has no such key and is dropped, which leaves
/// out the empty last partition Spark would have.
pub fn range_split_points(
    batches: &[RecordBatch],
    sort_options: &[SortOptions],
    partitions: usize,
) -> Result<Vec<SplitPoint>> {
    // Step 1: collect every sampled row as a key and its weight. The weight is the
    // number of input rows that one sampled row stands for.
    let mut candidates: Vec<(Vec<ScalarValue>, f32)> = vec![];
    for batch in batches {
        let Some(weight_index) = batch.num_columns().checked_sub(1) else {
            return internal_err!("range partition sample has no weight column");
        };
        if weight_index != sort_options.len() {
            return internal_err!(
                "range partition sample has {weight_index} keys, expected {}",
                sort_options.len()
            );
        }
        let weights = batch
            .column(weight_index)
            .as_primitive_opt::<Float32Type>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "range partition sample weight must be Float32"
                )
            })?;
        for row in 0..batch.num_rows() {
            let key = (0..weight_index)
                .map(|column| ScalarValue::try_from_array(batch.column(column), row))
                .collect::<Result<Vec<_>>>()?;
            candidates.push((key, weights.value(row)));
        }
    }
    // There cannot be more partitions than sampled rows. No split points means one partition.
    let partitions = partitions.min(candidates.len());
    if partitions <= 1 {
        return Ok(vec![]);
    }

    // Step 2: sort the candidates in the range ordering, so "greater" below means
    // "later in the ordering". `sort_by` cannot fail, so the first comparison error
    // is kept and returned after the sort.
    let mut error = None;
    candidates.sort_by(|(left, _), (right, _)| {
        compare_rows(left, right, sort_options).unwrap_or_else(|e| {
            error.get_or_insert(e);
            Ordering::Equal
        })
    });
    if let Some(e) = error {
        return Err(e);
    }

    // Step 3: pick the Spark bounds. Each partition should get `step` of the total
    // weight. Walk the sorted candidates and add up their weight. When the sum reaches
    // the next target, the key becomes a bound, unless it equals the previous bound.
    // A repeated key does not move the target, so the next greater key becomes the
    // bound, and a skewed key gives fewer than `partitions - 1` bounds.
    let step = candidates
        .iter()
        .map(|(_, weight)| f64::from(*weight))
        .sum::<f64>()
        / partitions as f64;
    let mut cumulative = 0.0;
    let mut target = step;
    let mut bounds: Vec<usize> = vec![];
    for (index, (key, weight)) in candidates.iter().enumerate() {
        if bounds.len() >= partitions - 1 {
            break;
        }
        cumulative += f64::from(*weight);
        if cumulative >= target {
            let is_new = match bounds.last() {
                None => true,
                Some(&previous) => {
                    compare_rows(key, &candidates[previous].0, sort_options)? == Ordering::Greater
                }
            };
            if is_new {
                bounds.push(index);
                target += step;
            }
        }
    }

    // Step 4: turn each Spark bound into a split point. A Spark partition ends at its
    // bound and includes it. A DataFusion partition starts at its split point and
    // includes it. So the split point is the first candidate greater than the bound.
    // This keeps every sampled key in the same partition as in Spark. A key that was
    // not sampled and lies between the bound and that candidate goes one partition
    // lower than in Spark.
    let mut split_points = vec![];
    for bound in bounds {
        let mut next = bound + 1;
        while next < candidates.len()
            && compare_rows(&candidates[next].0, &candidates[bound].0, sort_options)?
                != Ordering::Greater
        {
            next += 1;
        }
        if let Some((key, _)) = candidates.get(next) {
            split_points.push(SplitPoint::new(key.clone()));
        }
    }
    Ok(split_points)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringViewArray};
    use datafusion::arrow::datatypes::Float64Type;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn key_schema(data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", data_type, true)]))
    }

    fn sample(keys: Vec<Option<i64>>, weight: f32) -> RecordBatch {
        let n = keys.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Float32, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Float32Array::from(vec![weight; n])),
            ],
        )
        .unwrap()
    }

    fn values(split_points: &[SplitPoint]) -> Vec<Vec<ScalarValue>> {
        split_points.iter().map(|s| s.values().to_vec()).collect()
    }

    const ASC: SortOptions = SortOptions {
        descending: false,
        nulls_first: true,
    };

    #[test]
    fn test_split_points_without_partitions_or_samples() {
        let batch = sample(vec![Some(1), Some(2)], 1.0);
        assert!(range_split_points(&[batch], &[ASC], 1).unwrap().is_empty());
        assert!(range_split_points(&[], &[ASC], 4).unwrap().is_empty());
    }

    #[test]
    fn test_split_points_move_spark_bounds_to_the_next_key() {
        // Spark bounds for 0..12 in 3 partitions are 3 and 7, so the split points are 4 and 8.
        let batch = sample((0..12).map(Some).collect(), 1.0);
        let split_points = range_split_points(&[batch], &[ASC], 3).unwrap();
        assert_eq!(
            values(&split_points),
            vec![
                vec![ScalarValue::Int64(Some(4))],
                vec![ScalarValue::Int64(Some(8))]
            ]
        );
    }

    #[test]
    fn test_split_points_skip_repeated_keys_and_drop_the_last_bound() {
        // Spark bounds are 0, 1, 2 for 3 distinct keys in 10 partitions.
        // The bound 2 is the largest key, so only two split points remain.
        let keys = (0..3).flat_map(|k| vec![Some(k); 4]).collect();
        let split_points = range_split_points(&[sample(keys, 1.0)], &[ASC], 10).unwrap();
        assert_eq!(
            values(&split_points),
            vec![
                vec![ScalarValue::Int64(Some(1))],
                vec![ScalarValue::Int64(Some(2))]
            ]
        );
    }

    #[test]
    fn test_split_points_for_all_null_keys() {
        let split_points = range_split_points(&[sample(vec![None; 12], 1.0)], &[ASC], 4).unwrap();
        assert!(split_points.is_empty());
    }

    #[test]
    fn test_split_points_follow_descending_order_with_nulls_last() {
        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let keys = vec![Some(1), None, Some(3), Some(2), None, Some(4)];
        let split_points = range_split_points(&[sample(keys, 1.0)], &[options], 2).unwrap();
        // Sorted: 4, 3, 2, 1, null, null. The Spark bound is 2, so the split point is 1.
        assert_eq!(
            values(&split_points),
            vec![vec![ScalarValue::Int64(Some(1))]]
        );
    }

    #[test]
    fn test_split_points_use_partition_weights() {
        // The first partition read 10 rows per kept row, so its keys weigh more.
        let heavy = sample(vec![Some(0), Some(1)], 10.0);
        let light = sample(vec![Some(2), Some(3), Some(4), Some(5)], 1.0);
        let split_points = range_split_points(&[heavy, light], &[ASC], 2).unwrap();
        // Total weight 24: the Spark bound is 1 (cumulative weight 20), so the split point is 2.
        assert_eq!(
            values(&split_points),
            vec![vec![ScalarValue::Int64(Some(2))]]
        );
    }

    #[test]
    fn test_split_points_pass_range_partitioning_validation() {
        let keys = (0..100).map(|k| Some(k % 7)).collect();
        let split_points = range_split_points(&[sample(keys, 1.0)], &[ASC], 5).unwrap();
        let schema = key_schema(DataType::Int64);
        let ordering = datafusion::physical_expr::LexOrdering::new(vec![
            datafusion::physical_expr::PhysicalSortExpr::new(
                Arc::new(
                    datafusion::physical_expr::expressions::Column::new_with_schema("k", &schema)
                        .unwrap(),
                ),
                ASC,
            ),
        ])
        .unwrap();
        let range =
            datafusion::physical_expr::RangePartitioning::try_new(ordering, split_points).unwrap();
        assert_eq!(range.partition_count(), 5);
    }

    async fn run_sample(batches: Vec<RecordBatch>, sample_size: usize) -> Vec<RecordBatch> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| key_schema(DataType::Float64));
        let input = TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap();
        let exec = Arc::new(RangePartitionSampleExec::try_new(input, sample_size).unwrap());
        let ctx = SessionContext::new();
        exec.execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    fn float_batch(values: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            key_schema(DataType::Float64),
            vec![Arc::new(Float64Array::from(values))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sample_keeps_every_row_below_the_sample_size() {
        let output = run_sample(vec![float_batch(vec![1.0, 2.0]), float_batch(vec![3.0])], 5).await;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 3);
        let weights = output[0].column(1).as_primitive::<Float32Type>();
        assert!(weights.iter().all(|w| w == Some(1.0)));
    }

    #[tokio::test]
    async fn test_sample_is_bounded_weighted_and_repeatable() {
        let batches = (0..10)
            .map(|b| float_batch((0..100).map(|r| f64::from(b * 100 + r)).collect()))
            .collect::<Vec<_>>();
        let first = run_sample(batches.clone(), 50).await;
        let second = run_sample(batches, 50).await;
        assert_eq!(first, second);
        assert_eq!(first[0].num_rows(), 50);
        let weights = first[0].column(1).as_primitive::<Float32Type>();
        assert!(weights.iter().all(|w| w == Some(20.0)));
    }

    #[tokio::test]
    async fn test_sample_emits_nothing_for_an_empty_partition() {
        let output = run_sample(vec![], 5).await;
        assert!(output.is_empty());
    }

    /// Reservoir sampling (Algorithm R) over all rows at once, with the seed of partition 0.
    fn reference_sample(values: &[f64], sample_size: usize) -> Vec<f64> {
        let mut rng = StdRng::seed_from_u64(0);
        let mut sample = vec![];
        for (count, value) in (1u64..).zip(values) {
            if sample.len() < sample_size {
                sample.push(*value);
            } else {
                let slot = rng.random_range(0..count) as usize;
                if slot < sample_size {
                    sample[slot] = *value;
                }
            }
        }
        sample
    }

    #[tokio::test]
    async fn test_sample_does_not_depend_on_batch_boundaries() {
        let values = (0..2_000).map(f64::from).collect::<Vec<_>>();
        for (batch_size, sample_size) in [(1, 7), (3, 50), (100, 50), (128, 300), (2_000, 300)] {
            let batches = values
                .chunks(batch_size)
                .map(|chunk| float_batch(chunk.to_vec()))
                .collect();
            let output = run_sample(batches, sample_size).await;
            let sampled = output[0].column(0).as_primitive::<Float64Type>().values();
            assert_eq!(
                sampled.to_vec(),
                reference_sample(&values, sample_size),
                "batch size {batch_size}, sample size {sample_size}"
            );
        }
    }

    #[tokio::test]
    async fn test_sample_does_not_hold_the_string_buffers_of_its_input() {
        // Strings longer than 12 bytes live in each batch's data buffer, not in the views.
        let batches = (0..100)
            .map(|b| {
                let values = (0..100).map(|r| format!("key-{b:04}-{r:04}-padding"));
                RecordBatch::try_new(
                    key_schema(DataType::Utf8View),
                    vec![Arc::new(StringViewArray::from_iter_values(values))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let output = run_sample(batches, 10).await;
        let sampled = output[0].column(0).as_string_view();
        let kept_bytes = sampled.iter().flatten().map(str::len).sum::<usize>();
        let held_bytes = sampled
            .data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>();
        assert_eq!(sampled.len(), 10);
        assert!(
            held_bytes <= kept_bytes,
            "holds {held_bytes} bytes of strings for {kept_bytes} bytes of kept keys"
        );
    }
}
