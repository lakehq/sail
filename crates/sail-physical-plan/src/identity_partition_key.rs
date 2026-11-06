use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use datafusion_common::{ColumnStatistics, Result, Statistics, exec_err, internal_err, plan_err};
use futures::Stream;

const MAX_FALSE_SHARDS: i32 = 4096;

/// True rows share one allocation domain; false rows use bounded, stream-local shards.
/// Generation masks must contain non-null values, but their schema may be nullable
/// after physical expression lowering. Validate that invariant on the arrays.
#[derive(Debug, Clone)]
pub struct IdentityPartitionKeyExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<(usize, String)>,
    false_shards: i32,
    properties: Arc<PlanProperties>,
}

impl IdentityPartitionKeyExec {
    /// Resolve on the driver, not from a worker's task configuration.
    pub fn false_shards_for_partitions(target_partitions: usize) -> i32 {
        if target_partitions <= 1 {
            1
        } else {
            target_partitions
                .saturating_mul(32)
                .min(MAX_FALSE_SHARDS as usize) as i32
        }
    }

    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<(usize, String)>,
        false_shards: i32,
    ) -> Result<Self> {
        if keys.is_empty() || !(1..=MAX_FALSE_SHARDS).contains(&false_shards) {
            return plan_err!("IdentityPartitionKeyExec requires masks and 1..=4096 false shards");
        }
        let schema = input.schema();
        let mut fields = schema.fields().to_vec();
        let mut masks = HashSet::new();
        let mut names = fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<_>>();
        for (index, name) in &keys {
            let Some(field) = schema.fields().get(*index) else {
                return plan_err!("IdentityPartitionKeyExec mask index {index} is out of bounds");
            };
            if field.data_type() != &DataType::Boolean {
                return plan_err!("IdentityPartitionKeyExec requires Boolean masks");
            }
            if !masks.insert(*index) || name.is_empty() || !names.insert(name.clone()) {
                return plan_err!(
                    "IdentityPartitionKeyExec requires distinct masks and fresh key names"
                );
            }
            fields.push(Arc::new(Field::new(name, DataType::Int32, false)));
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        // Appending columns preserves input ordering, partitioning and equivalences.
        // The new keys are neither ordered nor equivalent to the Boolean masks.
        let equivalences =
            EquivalenceProperties::new(schema).extend(input.equivalence_properties().clone())?;
        let properties = Arc::new(PlanProperties::new(
            equivalences,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            keys,
            false_shards,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn keys(&self) -> &[(usize, String)] {
        &self.keys
    }

    pub fn false_shards(&self) -> i32 {
        self.false_shards
    }
}

impl DisplayAs for IdentityPartitionKeyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IdentityPartitionKeyExec: keys={:?}, false_shards={}",
            self.keys, self.false_shards
        )
    }
}

impl ExecutionPlan for IdentityPartitionKeyExec {
    fn name(&self) -> &'static str {
        "IdentityPartitionKeyExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
            return internal_err!("IdentityPartitionKeyExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.keys.clone(),
            self.false_shards,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(IdentityPartitionKeyStream {
            input: self.input.execute(partition, context)?,
            schema: self.schema(),
            mask_indices: self.keys.iter().map(|(index, _)| *index).collect(),
            next_false: vec![0; self.keys.len()],
            false_shards: self.false_shards,
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
        stats
            .column_statistics
            .extend((0..self.keys.len()).map(|_| ColumnStatistics::new_unknown()));
        let added_bytes = stats.num_rows.multiply(&Precision::Exact(
            self.keys.len() * std::mem::size_of::<i32>(),
        ));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);
        Ok(Arc::new(stats))
    }
}

struct IdentityPartitionKeyStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    mask_indices: Vec<usize>,
    next_false: Vec<i32>,
    false_shards: i32,
}

impl IdentityPartitionKeyStream {
    fn append_keys(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        for (index, next) in self.mask_indices.iter().zip(&mut self.next_false) {
            let mask = batch.column(*index).as_any().downcast_ref::<BooleanArray>();
            let Some(mask) = mask.filter(|mask| mask.null_count() == 0) else {
                return exec_err!("IdentityPartitionKeyExec requires non-null Boolean mask values");
            };
            let keys = Int32Array::from_iter_values(mask.values().iter().map(|generate| {
                if generate {
                    self.false_shards
                } else {
                    let key = *next;
                    *next = if key + 1 == self.false_shards {
                        0
                    } else {
                        key + 1
                    };
                    key
                }
            }));
            columns.push(Arc::new(keys));
        }
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl RecordBatchStream for IdentityPartitionKeyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for IdentityPartitionKeyStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.append_keys(batch))),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{Distribution, Partitioning, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use futures::{StreamExt, TryStreamExt};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]))
    }

    fn keys() -> Vec<(usize, String)> {
        vec![(0, "ka".into()), (1, "kb".into())]
    }

    fn values(batch: &RecordBatch, index: usize) -> Result<Vec<i32>> {
        let Some(array) = batch.column(index).as_any().downcast_ref::<Int32Array>() else {
            return internal_err!("expected Int32 partition keys");
        };
        assert_eq!(array.null_count(), 0);
        Ok(array.values().to_vec())
    }

    #[test]
    fn shard_count_is_bounded() {
        for (partitions, expected) in [
            (0, 1),
            (1, 1),
            (2, 64),
            (8, 256),
            (128, 4096),
            (usize::MAX, 4096),
        ] {
            assert_eq!(
                IdentityPartitionKeyExec::false_shards_for_partitions(partitions),
                expected
            );
        }
    }

    #[tokio::test]
    async fn independent_streams_masks_and_batch_boundaries() -> Result<()> {
        let schema = schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false, true, false, true, false])),
                Arc::new(BooleanArray::from(vec![true, false, false, false, true])),
            ],
        )?;
        let batches = vec![
            batch.slice(0, 2),
            RecordBatch::new_empty(schema.clone()),
            batch.slice(2, 3),
            batch.clone(),
        ];
        let input = MemorySourceConfig::try_new_exec(&[batches.clone(), batches], schema, None)?;
        let plan = Arc::new(IdentityPartitionKeyExec::try_new(input, keys(), 4)?);
        let context = Arc::new(TaskContext::default());
        let mut partial = plan.execute(0, context.clone())?;
        assert!(partial.next().await.transpose()?.is_some());
        // Neither another execute on this partition, another partition, nor reset
        // inherits counters from a partially consumed stream.
        for (candidate, partition) in [
            (plan.clone() as Arc<dyn ExecutionPlan>, 0),
            (plan.clone() as Arc<dyn ExecutionPlan>, 1),
            (plan.clone().reset_state()?, 0),
        ] {
            let output = candidate
                .execute(partition, context.clone())?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
            let mut a = Vec::new();
            let mut b = Vec::new();
            for batch in &output {
                a.extend(values(batch, 2)?);
                b.extend(values(batch, 3)?);
            }
            assert_eq!(a, [0, 4, 1, 4, 2, 3, 4, 0, 4, 1]);
            assert_eq!(b, [4, 0, 1, 2, 4, 4, 3, 0, 1, 4]);
            for (index, expected) in [false, true, false, true, false]
                .into_iter()
                .cycle()
                .take(10)
                .enumerate()
            {
                assert_eq!(a[index] == 4, expected);
            }
        }
        assert!(
            plan.replace_children(
                vec![],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Keep,
                )
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn false_rows_are_balanced_and_true_rows_do_not_advance_counters() -> Result<()> {
        for shards in [1, 256, MAX_FALSE_SHARDS] {
            let masks = (0..(shards as usize * 5))
                .flat_map(|_| [false, true, true])
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(BooleanArray::from(masks.clone())),
                    Arc::new(BooleanArray::from(masks)),
                ],
            )?;
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None)?;
            let plan = IdentityPartitionKeyExec::try_new(input, keys(), shards)?;
            let output = plan
                .execute(0, Arc::new(TaskContext::default()))?
                .try_collect::<Vec<_>>()
                .await?;
            let mut counts = vec![0; shards as usize + 1];
            for batch in output {
                assert_eq!(values(&batch, 2)?, values(&batch, 3)?);
                for value in values(&batch, 2)? {
                    counts[value as usize] += 1;
                }
            }
            assert!(counts[..shards as usize].iter().all(|count| *count == 5));
            assert_eq!(counts[shards as usize], shards as usize * 10);
        }
        Ok(())
    }

    #[test]
    fn validates_masks_names_and_shards() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        for shards in [-1, 0, MAX_FALSE_SHARDS + 1] {
            assert!(IdentityPartitionKeyExec::try_new(input.clone(), keys(), shards).is_err());
        }
        for keys in [
            vec![],
            vec![(2, "key".into())],
            vec![(0, "a".into())],
            vec![(0, "".into())],
            vec![(0, "ka".into()), (0, "kb".into())],
            vec![(0, "key".into()), (1, "key".into())],
        ] {
            assert!(IdentityPartitionKeyExec::try_new(input.clone(), keys, 1).is_err());
        }
        for nullable in [false, true] {
            let input = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int32,
                nullable,
            )]))));
            assert!(IdentityPartitionKeyExec::try_new(input, vec![(0, "key".into())], 1).is_err());
        }
    }

    #[test]
    fn preserves_row_and_column_statistics() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let plan = IdentityPartitionKeyExec::try_new(input, keys(), 256)?;
        let mut stats = Statistics::new_unknown(schema().as_ref());
        stats.num_rows = Precision::Exact(10);
        stats.total_byte_size = Precision::Exact(20);
        let result =
            plan.statistics_from_inputs(&[Arc::new(stats.clone())], &StatisticsArgs::default())?;
        assert_eq!(result.num_rows, stats.num_rows);
        assert_eq!(result.total_byte_size, Precision::Exact(100));
        assert_eq!(result.column_statistics.len(), 4);
        assert_eq!(result.column_statistics[..2], stats.column_statistics);
        let unknown = Statistics::new_unknown(schema().as_ref());
        let result =
            plan.statistics_from_inputs(&[Arc::new(unknown)], &StatisticsArgs::default())?;
        assert_eq!(result.num_rows, Precision::Absent);
        assert_eq!(result.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[tokio::test]
    async fn nullable_schema_with_non_null_masks() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false, true, false, false, true])),
                Arc::new(BooleanArray::from(vec![true, false, false, true, false])),
            ],
        )?;
        let input = MemorySourceConfig::try_new_exec(
            &[vec![
                batch.slice(0, 2),
                RecordBatch::new_empty(schema.clone()),
                batch.slice(2, 3),
            ]],
            schema,
            None,
        )?;
        let plan = IdentityPartitionKeyExec::try_new(input, keys(), 2)?;
        let output = plan
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?;
        let a = output
            .iter()
            .map(|batch| values(batch, 2))
            .collect::<Result<Vec<_>>>()?;
        let b = output
            .iter()
            .map(|batch| values(batch, 3))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(a.concat(), [0, 2, 1, 0, 2]);
        assert_eq!(b.concat(), [2, 0, 1, 2, 0]);
        for result in &output {
            assert!(result.schema().field(0).is_nullable());
            assert!(result.schema().field(1).is_nullable());
            assert!(!result.schema().field(2).is_nullable());
            assert!(!result.schema().field(3).is_nullable());
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_null_mask_values() -> Result<()> {
        // MERGE constructs masks from IS TRUE predicates. Actual NULL values
        // violate that contract; accepting nullable schema must not hide them.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BooleanArray::from(vec![
                Some(false),
                None,
                Some(true),
            ]))],
        )?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let plan = IdentityPartitionKeyExec::try_new(input, vec![(0, "key".into())], 2)?;
        let Err(error) = plan
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await
        else {
            return internal_err!("NULL generation masks must fail at execution, not planning");
        };
        assert!(
            error
                .to_string()
                .contains("requires non-null Boolean mask values")
        );
        Ok(())
    }

    #[test]
    fn preserves_input_properties_without_claiming_key_order_or_distribution() -> Result<()> {
        let column = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            datafusion::physical_expr::LexOrdering::new([PhysicalSortExpr::new_default(
                column.clone(),
            )])
            .ok_or_else(|| datafusion_common::internal_datafusion_err!("missing ordering"))?,
            Arc::new(EmptyExec::new(schema())),
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(IdentityPartitionKeyExec::try_new(
            input.clone(),
            keys(),
            256,
        )?);
        assert_eq!(plan.output_ordering(), input.output_ordering());
        assert!(matches!(
            plan.output_partitioning(),
            Partitioning::UnknownPartitioning(1)
        ));
        assert_eq!(
            plan.output_partitioning().partition_count(),
            input.output_partitioning().partition_count()
        );
        assert_eq!(plan.pipeline_behavior(), input.pipeline_behavior());
        assert_eq!(plan.boundedness(), input.boundedness());
        let key = Arc::new(Column::new("ka", 2)) as Arc<dyn PhysicalExpr>;
        assert!(
            !plan
                .equivalence_properties()
                .ordering_satisfy([PhysicalSortExpr::new_default(key.clone())])?
        );
        let partitioning = Partitioning::Hash(vec![column], 8);
        assert!(
            !partitioning
                .satisfaction(
                    &Distribution::KeyPartitioned(vec![key]),
                    plan.equivalence_properties(),
                    true
                )
                .is_satisfied()
        );
        assert_eq!(plan.schema().fields().len(), 4);
        assert_eq!(plan.schema().field(2).data_type(), &DataType::Int32);
        assert!(!plan.schema().field(2).is_nullable());
        Ok(())
    }
}
