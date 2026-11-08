use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, Float32Array, RecordBatch};
use datafusion::arrow::compute::interleave_record_batch;
use datafusion::arrow::compute::kernels::sort::SortOptions;
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
    // The seed only needs to keep the sample the same across runs on the same input.
    let mut rng = StdRng::seed_from_u64(partition as u64);
    let mut reservoir = RecordBatch::new_empty(input.schema());
    let mut count: u64 = 0;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        // Each entry is (0, row) for a row kept so far or (1, row) for a row of `batch`.
        let mut indices = (0..reservoir.num_rows())
            .map(|row| (0, row))
            .collect::<Vec<_>>();
        let mut changed = false;
        for row in 0..batch.num_rows() {
            count += 1;
            if indices.len() < sample_size {
                indices.push((1, row));
                changed = true;
            } else {
                let slot = rng.random_range(0..count);
                if let Ok(slot) = usize::try_from(slot)
                    && slot < sample_size
                {
                    indices[slot] = (1, row);
                    changed = true;
                }
            }
        }
        if changed {
            reservoir = interleave_record_batch(&[&reservoir, &batch], &indices)?;
        }
    }
    if reservoir.num_rows() == 0 {
        return Ok(None);
    }
    // Spark computes the weight in double precision and keeps it as a float.
    let weight = (count as f64 / reservoir.num_rows() as f64) as f32;
    let mut columns = reservoir.columns().to_vec();
    columns.push(Arc::new(Float32Array::from(vec![
        weight;
        reservoir.num_rows()
    ])));
    Ok(Some(RecordBatch::try_new(schema, columns)?))
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
    let partitions = partitions.min(candidates.len());
    if partitions <= 1 {
        return Ok(vec![]);
    }

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
    use datafusion::arrow::array::{Float64Array, Int64Array};
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
}
