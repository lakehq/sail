use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, OrderingRequirements, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions,
};
use datafusion_common::{
    DataFusionError, Result, exec_err, internal_datafusion_err, resources_datafusion_err,
};
use futures::{StreamExt, TryStreamExt, stream};
use sail_common_datafusion::utils::items::ItemTaker;

/// Returns the last rows of the input, which is evaluated once (see Spark's `CollectTailExec`).
/// The input has one partition, and only the batches that hold the last rows are kept.
#[derive(Debug, Clone)]
pub struct TailExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: usize,
    properties: Arc<PlanProperties>,
}

impl TailExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.boundedness(),
        ));
        Self {
            input,
            fetch,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn fetch(&self) -> usize {
        self.fetch
    }
}

impl DisplayAs for TailExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TailExec: fetch={}", self.fetch)
    }
}

impl ExecutionPlan for TailExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.input.output_ordering().cloned().map(Into::into)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Changing the input ordering changes which rows the tail selects.
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .one()
            .map_err(|_| internal_datafusion_err!("TailExec should have one child"))?;
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input,
                ..self.as_ref().clone()
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(Self::new(input, self.fetch))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("TailExec: partition index out of range: {partition}");
        }
        let fetch = self.fetch;
        let reservation = MemoryConsumer::new("TailExec").register(context.memory_pool());
        let input = if fetch == 0 {
            None
        } else {
            Some(self.input.execute(0, context)?)
        };
        let output = stream::once(async move {
            let batches = match input {
                Some(input) => take_last(last_rows(input, fetch, reservation).await?, fetch),
                None => RetainedBatches::new(reservation),
            };
            Ok::<_, DataFusionError>(stream::iter(batches.map(Ok)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

/// Keep the original charge when slicing a batch: the slice still retains its backing buffers.
/// Fields drop in order, freeing the batch before returning its array reservation to the pool.
struct RetainedBatch {
    batch: RecordBatch,
    array_reservation: MemoryReservation,
}

/// Own the queue allocation and its reservation together, including during output iteration.
/// The deque drops before its capacity reservation, and each entry owns its array reservation.
struct RetainedBatches {
    batches: VecDeque<RetainedBatch>,
    reservation: MemoryReservation,
}

impl RetainedBatches {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            batches: VecDeque::new(),
            reservation,
        }
    }

    fn push_back(&mut self, batch: RecordBatch) -> Result<()> {
        // All reservations share one registered consumer. Entry ownership releases array
        // bytes on eviction, emission, failure or cancellation without changing the queue charge.
        let retained = RetainedBatch {
            batch,
            array_reservation: self.reservation.new_empty(),
        };
        retained
            .array_reservation
            .try_grow(retained.batch.get_array_memory_size())?;
        self.reserve_slot()?;
        self.batches.push_back(retained);
        Ok(())
    }

    fn reserve_slot(&mut self) -> Result<()> {
        if self.batches.len() < self.batches.capacity() {
            return Ok(());
        }
        let capacity = self
            .batches
            .capacity()
            .checked_mul(2)
            .map(|capacity| capacity.max(1))
            .ok_or_else(|| resources_datafusion_err!("TailExec queue capacity overflow"))?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<RetainedBatch>())
            .ok_or_else(|| resources_datafusion_err!("TailExec queue allocation size overflow"))?;

        // Reserve the entire new allocation while the old allocation remains charged.
        // Construct it separately so both allocations are covered even when growth moves it.
        let reservation = self.reservation.new_empty();
        reservation.try_grow(bytes)?;
        let mut batches = VecDeque::new();
        batches.try_reserve_exact(capacity).map_err(|error| {
            resources_datafusion_err!("TailExec queue allocation failed: {error}")
        })?;
        // Account for the actual capacity rather than relying on allocator growth behavior.
        reservation.try_resize(batches.capacity() * std::mem::size_of::<RetainedBatch>())?;
        batches.extend(self.batches.drain(..));
        self.batches = batches;
        // The old deque allocation was freed by the assignment above.
        self.reservation = reservation;
        Ok(())
    }
}

impl Iterator for RetainedBatches {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let retained = self.batches.pop_front()?;
        if self.batches.is_empty() {
            // Free the allocation before its charge, including when the consumer never
            // polls again after receiving the final batch.
            self.batches = VecDeque::new();
            self.reservation.free();
        }
        // The array reservation ends when ownership of this batch passes downstream.
        Some(retained.batch)
    }
}

/// Returns the batches that hold the last rows of the stream, keeping no more batches than
/// needed for them. The queue owns all reservations until their allocations are released.
async fn last_rows(
    mut input: SendableRecordBatchStream,
    fetch: usize,
    reservation: MemoryReservation,
) -> Result<RetainedBatches> {
    let mut batches = RetainedBatches::new(reservation);
    // Zero-column batches can have usize::MAX rows without allocating their values.
    // Two such batch lengths can exceed usize while the retained suffix still fits.
    let mut rows = 0_u128;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows() as u128;
        // Release obsolete arrays before reserving the new one. The deque's allocated
        // capacity remains charged even when eviction leaves it empty.
        while let Some(front) = batches.batches.front()
            && rows - front.batch.num_rows() as u128 >= fetch as u128
        {
            rows -= front.batch.num_rows() as u128;
            batches.batches.pop_front();
        }
        batches.push_back(batch)?;
    }
    Ok(batches)
}

/// Returns the last rows of the batches.
fn take_last(mut batches: RetainedBatches, fetch: usize) -> RetainedBatches {
    let rows = batches
        .batches
        .iter()
        .map(|x| x.batch.num_rows() as u128)
        .sum::<u128>();
    // last_rows already removed every batch wholly outside the suffix.
    if let Some(front) = batches.batches.front_mut()
        && rows > fetch as u128
    {
        // The suffix invariant guarantees the excess is smaller than the first batch.
        let excess = (rows - fetch as u128) as usize;
        front.batch = front.batch.slice(excess, front.batch.num_rows() - excess);
    }
    batches
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array, RecordBatchOptions, new_null_array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{
        DataType, Field, Int64Type, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use sail_common_datafusion::array::placeholder::placeholder_array;

    use super::*;

    /// Returns the last rows of a partition of six batches of three rows each,
    /// where one batch has no rows.
    async fn tail(fetch: usize) -> Result<Vec<i64>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batches = (0..6)
            .map(|batch| {
                let start = batch * 3;
                let end = if batch == 4 { start } else { start + 3 };
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(start..end))],
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
        let plan = Arc::new(TailExec::new(input, fetch));
        let batches = collect(plan, Arc::new(TaskContext::default())).await?;
        Ok(batches
            .iter()
            .flat_map(|x| x.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect())
    }

    #[tokio::test]
    async fn test_tail() -> Result<()> {
        let rows = (0..12).chain(15..18).collect::<Vec<_>>();
        assert_eq!(tail(0).await?, Vec::<i64>::new());
        assert_eq!(tail(1).await?, vec![17]);
        assert_eq!(tail(4).await?, vec![11, 15, 16, 17]);
        assert_eq!(tail(10).await?, rows[5..].to_vec());
        assert_eq!(tail(100).await?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_propagates_late_error_under_an_early_limit() -> Result<()> {
        use datafusion::physical_plan::limit::GlobalLimitExec;
        use datafusion::physical_plan::test::exec::MockExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let input = Arc::new(
            MockExec::new(
                vec![Ok(batch), exec_err!("terminal input error after rows")],
                schema,
            )
            .with_unknown_statistics()
            .with_use_task(false),
        );
        // Tail must observe the end of its input before emitting any retained rows.
        // Otherwise LIMIT 1 could stop polling before the source reports its error.
        let tail = Arc::new(TailExec::new(input, 2));
        let limited = Arc::new(GlobalLimitExec::new(tail, 0, Some(1)));
        let Err(error) = collect(limited, Arc::new(TaskContext::default())).await else {
            return exec_err!("tail must propagate a terminal error before returning rows");
        };
        assert!(
            error
                .to_string()
                .contains("terminal input error after rows")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_tail_does_not_execute_its_input() -> Result<()> {
        use datafusion::physical_plan::test::exec::ErrorExec;

        // ErrorExec fails in execute(), before producing a stream. A zero tail must
        // avoid executing the input, not merely avoid polling its stream.
        let plan = Arc::new(TailExec::new(Arc::new(ErrorExec::new()), 0));
        assert!(
            collect(plan, Arc::new(TaskContext::default()))
                .await?
                .is_empty()
        );
        Ok(())
    }

    fn memory_test_batch(start: i64, length: i64) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values(
                start..start + length,
            ))],
        )?)
    }

    fn memory_test_context(
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<Arc<TaskContext>> {
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()?;
        Ok(Arc::new(TaskContext::default().with_runtime(runtime)))
    }

    #[tokio::test]
    async fn test_tail_obeys_memory_limit_and_releases_on_error() -> Result<()> {
        use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool};

        let batch = memory_test_batch(0, 1024)?;
        let bytes = batch.get_array_memory_size() + std::mem::size_of::<RetainedBatch>();
        for pool in [
            Arc::new(GreedyMemoryPool::new(bytes)) as Arc<dyn MemoryPool>,
            Arc::new(FairSpillPool::new(bytes)) as Arc<dyn MemoryPool>,
        ] {
            let input = MemorySourceConfig::try_new_exec(
                &[vec![batch.clone(), batch.clone()]],
                batch.schema(),
                None,
            )?;
            let tail = TailExec::new(input, usize::MAX);
            let mut output = tail.execute(0, memory_test_context(Arc::clone(&pool))?)?;
            let Some(Err(error)) = output.next().await else {
                return exec_err!("retained tail batches must respect the task memory limit");
            };
            assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
            assert_eq!(
                pool.reserved(),
                0,
                "failed tail must release retained batches"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_releases_evicted_and_emitted_batches() -> Result<()> {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

        let batches = (0..3)
            .map(|index| memory_test_batch(index * 4, 4))
            .collect::<Result<Vec<_>>>()?;
        let array_bytes = batches[0].get_array_memory_size();
        let slot_bytes = std::mem::size_of::<RetainedBatch>();
        // A one-batch suffix must fit in a one-batch pool despite reading three batches.
        // Growing to two entries temporarily needs both the old and new queue allocations.
        for (fetch, capacity, remaining) in [
            (3, array_bytes + slot_bytes, 0),
            (
                5,
                2 * array_bytes + 3 * slot_bytes,
                array_bytes + 2 * slot_bytes,
            ),
        ] {
            let pool = Arc::new(GreedyMemoryPool::new(capacity)) as Arc<dyn MemoryPool>;
            let input = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                batches[0].schema(),
                None,
            )?;
            let mut output =
                TailExec::new(input, fetch).execute(0, memory_test_context(Arc::clone(&pool))?)?;
            let first = output.try_next().await?.ok_or_else(|| {
                internal_datafusion_err!("tail must return its first retained batch")
            })?;
            assert_eq!(first.num_rows(), if fetch == 3 { 3 } else { 1 });
            assert_eq!(pool.reserved(), remaining);
            // This also covers cancellation with another retained output batch pending.
            drop(output);
            assert_eq!(pool.reserved(), 0);
        }
        Ok(())
    }

    #[test]
    fn test_tail_queue_growth_reserves_old_and_new_allocations() -> Result<()> {
        use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool};

        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        let slot_bytes = std::mem::size_of::<RetainedBatch>();
        for budget in [2, 3] {
            for pool in [
                Arc::new(GreedyMemoryPool::new(budget * slot_bytes)) as Arc<dyn MemoryPool>,
                Arc::new(FairSpillPool::new(budget * slot_bytes)) as Arc<dyn MemoryPool>,
            ] {
                let mut retained =
                    RetainedBatches::new(MemoryConsumer::new("tail-test").register(&pool));
                retained.push_back(batch.clone())?;
                assert_eq!(retained.batches.capacity(), 1);
                assert_eq!(pool.reserved(), slot_bytes);
                let result = retained.push_back(batch.clone());
                if budget == 2 {
                    // The final two-entry queue would fit, but the old allocation must
                    // remain charged while its replacement is being allocated.
                    assert!(matches!(
                        result,
                        Err(DataFusionError::ResourcesExhausted(_))
                    ));
                    assert_eq!(retained.batches.len(), 1);
                    assert_eq!(retained.batches.capacity(), 1);
                    assert_eq!(pool.reserved(), slot_bytes);
                } else {
                    result?;
                    assert_eq!(retained.batches.len(), 2);
                    assert_eq!(pool.reserved(), retained.batches.capacity() * slot_bytes);
                    assert!(retained.next().is_some());
                    assert_eq!(pool.reserved(), 2 * slot_bytes);
                    assert!(retained.next().is_some());
                    assert_eq!(retained.batches.capacity(), 0);
                    assert_eq!(
                        pool.reserved(),
                        0,
                        "last emission frees queue without another poll"
                    );
                }
                drop(retained);
                assert_eq!(pool.reserved(), 0);
            }
        }
        Ok(())
    }

    #[test]
    fn test_tail_queue_growth_failure_releases_incoming_array() -> Result<()> {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

        let batch = memory_test_batch(0, 4)?;
        let array_bytes = batch.get_array_memory_size();
        let slot_bytes = std::mem::size_of::<RetainedBatch>();
        // Both arrays fit, but replacing the one-slot queue requires three slots temporarily.
        let pool = Arc::new(GreedyMemoryPool::new(2 * array_bytes + 2 * slot_bytes))
            as Arc<dyn MemoryPool>;
        let mut retained = RetainedBatches::new(MemoryConsumer::new("tail-test").register(&pool));
        retained.push_back(batch.clone())?;
        assert!(matches!(
            retained.push_back(batch),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(retained.batches.len(), 1);
        assert_eq!(pool.reserved(), array_bytes + slot_bytes);
        drop(retained);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_releases_memory_on_input_error() -> Result<()> {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        use datafusion::physical_plan::test::exec::MockExec;

        let batch = memory_test_batch(0, 1024)?;
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024)) as Arc<dyn MemoryPool>;
        let input = Arc::new(
            MockExec::new(
                vec![Ok(batch.clone()), exec_err!("input failed")],
                batch.schema(),
            )
            .with_unknown_statistics()
            .with_use_task(false),
        );
        let mut output =
            TailExec::new(input, 2048).execute(0, memory_test_context(Arc::clone(&pool))?)?;
        assert!(output.try_next().await.is_err());
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_charges_backing_buffers_and_releases_on_cancellation() -> Result<()> {
        use datafusion::arrow::datatypes::SchemaRef;
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};

        #[derive(Debug)]
        struct PendingInput {
            schema: SchemaRef,
            batch: RecordBatch,
        }
        impl PartitionStream for PendingInput {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }
            fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    stream::iter([Ok(self.batch.clone())]).chain(stream::pending()),
                ))
            }
        }

        let full_batch = memory_test_batch(0, 1024)?;
        let bytes = full_batch.get_array_memory_size();
        let batch = full_batch.slice(1023, 1);
        let pool = Arc::new(GreedyMemoryPool::new(bytes * 2)) as Arc<dyn MemoryPool>;
        let input = Arc::new(StreamingTableExec::try_new(
            batch.schema(),
            vec![Arc::new(PendingInput {
                schema: batch.schema(),
                batch,
            })],
            None,
            [],
            false,
            None,
        )?);
        let mut output =
            TailExec::new(input, 1).execute(0, memory_test_context(Arc::clone(&pool))?)?;
        assert!(futures::poll!(output.next()).is_pending());
        assert!(
            pool.reserved() >= bytes,
            "a slice retains its full backing buffer"
        );
        drop(output);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_column_tail_obeys_memory_limit() -> Result<()> {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        let pool = Arc::new(GreedyMemoryPool::new(std::mem::size_of::<RetainedBatch>()))
            as Arc<dyn MemoryPool>;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch; 2]], schema, None)?;
        let mut output =
            TailExec::new(input, 2).execute(0, memory_test_context(Arc::clone(&pool))?)?;
        assert!(output.try_next().await.is_err());
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_enforcement_preserves_tail_selection() -> Result<()> {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::common::config::ConfigOptions;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
        use datafusion::physical_plan::sorts::sort::SortExec;

        let ordering = |descending| -> Result<LexOrdering> {
            LexOrdering::new([PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending,
                    nulls_first: false,
                },
            )])
            .ok_or_else(|| internal_datafusion_err!("nonempty sort ordering"))
        };
        for (descending, expected) in [(false, vec![9, 8, 7]), (true, vec![0, 1, 2])] {
            for partitions in [1, 2] {
                let batch = memory_test_batch(0, 10)?;
                let input_batches = if partitions == 1 {
                    vec![vec![batch.clone()]]
                } else {
                    vec![vec![batch.slice(0, 5)], vec![batch.slice(5, 5)]]
                };
                let input = MemorySourceConfig::try_new_exec(&input_batches, batch.schema(), None)?;
                let input = Arc::new(
                    SortExec::new(ordering(descending)?, input).with_preserve_partitioning(true),
                );
                let tail = Arc::new(TailExec::new(input, 3));
                let plan = Arc::new(SortExec::new(ordering(!descending)?, tail));
                let optimized =
                    EnsureRequirements::new().optimize(plan, &ConfigOptions::default())?;
                let batches = collect(optimized, Arc::new(TaskContext::default())).await?;
                let actual = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_primitive::<Int64Type>()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual, expected,
                    "descending={descending}, partitions={partitions}"
                );
            }
        }
        // Even without declared child ordering, a parent sort must stay above Tail.
        let batch = memory_test_batch(0, 10)?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
        let tail = Arc::new(TailExec::new(input, 3));
        let plan = Arc::new(SortExec::new(ordering(true)?, tail));
        let optimized = EnsureRequirements::new().optimize(plan, &ConfigOptions::default())?;
        let batches = collect(optimized, Arc::new(TaskContext::default())).await?;
        let actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![9, 8, 7]);
        Ok(())
    }

    fn arrow_types() -> Vec<DataType> {
        let child = Arc::new(Field::new("item", DataType::Int32, true));
        let fields = vec![
            Field::new("number", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ];
        let union_fields: UnionFields = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (i as i8, Arc::new(f.clone())))
            .collect();
        let mut types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(4),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Decimal32(8, 2),
            DataType::Decimal64(16, 4),
            DataType::Decimal128(30, 6),
            DataType::Decimal256(60, 8),
            DataType::List(Arc::clone(&child)),
            DataType::LargeList(Arc::clone(&child)),
            DataType::ListView(Arc::clone(&child)),
            DataType::LargeListView(Arc::clone(&child)),
            DataType::FixedSizeList(Arc::clone(&child), 3),
            DataType::Struct(fields.into()),
            DataType::Union(union_fields.clone(), UnionMode::Sparse),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::List(child), true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            types.push(DataType::Duration(unit));
            types.push(DataType::Timestamp(unit, None));
            types.push(DataType::Timestamp(unit, Some("UTC".into())));
        }
        for key in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            types.push(DataType::Dictionary(
                Box::new(key),
                Box::new(DataType::Utf8),
            ));
        }
        for run_end in [DataType::Int16, DataType::Int32, DataType::Int64] {
            types.push(DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", run_end, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ));
        }
        types
    }

    #[tokio::test]
    async fn test_tail_preserves_arrow_values_and_schema() -> Result<()> {
        // Alternate valid and null batches, including empty batches and nonzero array
        // offsets. The ordinal column makes batch loss, duplication and reordering visible.
        for data_type in arrow_types() {
            let schema = Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("ordinal", DataType::Int64, false),
                    Field::new("value", data_type.clone(), true)
                        .with_metadata([("field-key".to_owned(), "field-value".to_owned())].into()),
                ],
                [("schema-key".to_owned(), "schema-value".to_owned())].into(),
            ));
            let mut ordinal = 0;
            let batches = [0, 1, 4, 0, 6, 2, 4, 0]
                .into_iter()
                .enumerate()
                .map(|(index, length)| {
                    let values = if index % 2 == 0 {
                        let array = placeholder_array(&data_type, length + 2)?;
                        // Placeholder decimal arrays use their default precision and scale.
                        cast(array.as_ref(), &data_type)?
                    } else {
                        new_null_array(&data_type, length + 2)
                    };
                    let ids = Int64Array::from_iter_values(ordinal..ordinal + length as i64);
                    ordinal += length as i64;
                    Ok(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(ids), values.slice(1, length)],
                    )?)
                })
                .collect::<Result<Vec<_>>>()?;
            let expected = batches
                .iter()
                .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
                .collect::<Vec<_>>();
            for fetch in [0, 1, 4, 5, 9, 17, 100] {
                let input = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&batches),
                    Arc::clone(&schema),
                    None,
                )?;
                let plan = Arc::new(TailExec::new(input, fetch));
                assert_eq!(plan.schema(), schema);
                let actual = collect(plan, Arc::new(TaskContext::default())).await?;
                let actual = actual
                    .iter()
                    .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    expected[expected.len().saturating_sub(fetch)..],
                    "type={data_type:?}, fetch={fetch}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_tail_of_zero_column_batches() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batches = [0, 3, 0, 7]
            .into_iter()
            .map(|rows| {
                RecordBatch::try_new_with_options(
                    Arc::clone(&schema),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows)),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for fetch in [0, 1, 5, 10, 20] {
            let input = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&batches),
                Arc::clone(&schema),
                None,
            )?;
            let output = collect(
                Arc::new(TailExec::new(input, fetch)),
                Arc::new(TaskContext::default()),
            )
            .await?;
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                fetch.min(10)
            );
            assert!(output.iter().all(|batch| batch.schema() == schema));
        }
        Ok(())
    }
}
