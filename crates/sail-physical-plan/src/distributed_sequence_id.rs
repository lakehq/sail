use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DataFusionError, Result, Statistics, exec_datafusion_err, internal_err,
    plan_err,
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::OnceCell;

/// Assigns IDs in input partition order, then in row order within each partition.
/// Local execution materializes the input once; distributed planning supplies a
/// replayable input and broadcasts the small partition-count relation separately.
#[derive(Debug)]
pub struct DistributedSequenceIdExec {
    input: Arc<dyn ExecutionPlan>,
    counts: Option<Arc<dyn ExecutionPlan>>,
    column_name: String,
    properties: Arc<PlanProperties>,
    state: Arc<OnceCell<Result<Arc<SequenceState>, Arc<DataFusionError>>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl DistributedSequenceIdExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: String,
        counts: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if matches!(input.boundedness(), Boundedness::Unbounded { .. }) {
            return plan_err!("distributed_sequence_id requires bounded input");
        }
        if let Some(counts) = &counts
            && (counts.schema() != PartitionCountsExec::schema_ref()
                || counts.output_partitioning().partition_count() != 1)
        {
            return plan_err!("distributed_sequence_id requires a single partition of counts");
        }
        let input_schema = input.schema();
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(&column_name, DataType::Int64, false)));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema).extend(input.equivalence_properties().clone())?,
            input.output_partitioning().clone(),
            if input.output_partitioning().partition_count() == 1 && counts.is_none() {
                input.pipeline_behavior()
            } else {
                EmissionType::Final
            },
            input.boundedness(),
        ));
        Ok(Self {
            input,
            counts,
            column_name,
            properties,
            state: Arc::new(OnceCell::new()),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn counts(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        self.counts.as_ref()
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl DisplayAs for DistributedSequenceIdExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DistributedSequenceIdExec: col={}", self.column_name)
    }
}

impl ExecutionPlan for DistributedSequenceIdExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        std::iter::once(&self.input)
            .chain(self.counts.iter())
            .collect()
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    // Keep the default maintains_input_order=false: pushing a new sort below
    // this operator would change the IDs attached to the input rows.

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != self.children().len() {
            return internal_err!(
                "DistributedSequenceIdExec received an invalid number of children"
            );
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.column_name.clone(),
            children.get(1).cloned(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.input.output_partitioning().partition_count();
        if partition >= partitions {
            return internal_err!(
                "DistributedSequenceIdExec partition {partition} is out of range"
            );
        }
        if partitions == 1 && self.counts.is_none() {
            return Ok(attach_ids(
                self.input.execute(0, context)?,
                self.schema(),
                0,
            ));
        }
        let input = self.input.clone();
        let counts = self.counts.clone();
        let state = self.state.clone();
        let metrics = self.metrics.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let future = async move {
            let state = state
                .get_or_init(|| async {
                    prepare_sequence(input.clone(), counts, context.clone(), metrics)
                        .await
                        .map(Arc::new)
                        .map_err(Arc::new)
                })
                .await
                .clone()
                .map_err(DataFusionError::Shared)?;
            let stream = match &state.partitions {
                Some(cached) => cached[partition].stream(input.schema())?,
                None => input.execute(partition, context)?,
            };
            let output = attach_ids(stream, schema, state.offsets[partition]);
            // Keep the memory reservation alive for cached record batches.
            Ok::<_, DataFusionError>(output.map(move |batch| {
                let _guard = &state;
                batch
            }))
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream::once(future).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        std::iter::once(ChildStats::At(partition))
            .chain(self.counts.iter().map(|_| ChildStats::Skip))
            .collect()
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let mut stats = input_stats[0].as_ref().clone();
        stats
            .column_statistics
            .push(ColumnStatistics::new_unknown());
        stats.total_byte_size = stats.total_byte_size.add(
            &stats
                .num_rows
                .multiply(&Precision::Exact(std::mem::size_of::<i64>())),
        );
        Ok(Arc::new(stats))
    }
}

#[derive(Debug)]
struct SequenceState {
    offsets: Vec<u64>,
    partitions: Option<Vec<CachedPartition>>,
}

struct CachedPartition {
    rows: u64,
    batches: Vec<RecordBatch>,
    _reservation: MemoryReservation,
    spill: Option<Arc<dyn SpillFile>>,
    manager: SpillManager,
}

impl std::fmt::Debug for CachedPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPartition")
            .field("rows", &self.rows)
            .field("spilled", &self.spill.is_some())
            .finish_non_exhaustive()
    }
}

impl CachedPartition {
    fn stream(&self, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        match &self.spill {
            Some(file) => self.manager.read_spill_as_stream(file.clone(), None),
            None => Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::iter(self.batches.clone().into_iter().map(Ok)),
            ))),
        }
    }
}

async fn cache_partition(
    mut input: SendableRecordBatchStream,
    partition: usize,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<CachedPartition> {
    const CAPACITY: usize = 8 * 1024 * 1024;
    let reservation = MemoryConsumer::new(format!("DistributedSequenceId[{partition}]"))
        .with_can_spill(true)
        .register(context.memory_pool());
    let manager = SpillManager::new(
        context.runtime_env(),
        SpillMetrics::new(&metrics, partition),
        input.schema(),
    )
    .with_compression_type(
        context
            .session_config()
            .options()
            .execution
            .spill_compression,
    );
    let mut batches = Vec::new();
    let mut rows = 0;
    while let Some(batch) = input.try_next().await? {
        rows = checked_end(rows, batch.num_rows() as u64)?;
        let size = batch.get_array_memory_size();
        if size <= CAPACITY.saturating_sub(reservation.size()) && reservation.try_grow(size).is_ok()
        {
            batches.push(batch);
            continue;
        }
        let mut writer = manager.create_in_progress_file("distributed_sequence_id input")?;
        for buffered in batches.drain(..) {
            writer.append_batch(&buffered)?;
        }
        reservation.free();
        writer.append_batch(&batch)?;
        while let Some(batch) = input.try_next().await? {
            rows = checked_end(rows, batch.num_rows() as u64)?;
            writer.append_batch(&batch)?;
        }
        return Ok(CachedPartition {
            rows,
            batches,
            _reservation: reservation,
            spill: writer.finish()?,
            manager,
        });
    }
    Ok(CachedPartition {
        rows,
        batches,
        _reservation: reservation,
        spill: None,
        manager,
    })
}

async fn prepare_sequence(
    input: Arc<dyn ExecutionPlan>,
    counts: Option<Arc<dyn ExecutionPlan>>,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SequenceState> {
    let partitions = input.output_partitioning().partition_count();
    let (counts, cached) = if let Some(counts) = counts {
        let mut stream = counts.execute(0, context)?;
        let mut sizes = vec![None; partitions];
        while let Some(batch) = stream.try_next().await? {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("invalid distributed_sequence_id partition IDs")
                })?;
            let rows = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| exec_datafusion_err!("invalid distributed_sequence_id counts"))?;
            for i in 0..batch.num_rows() {
                let slot = usize::try_from(ids.value(i))
                    .ok()
                    .and_then(|p| sizes.get_mut(p));
                let Some(slot) = slot else {
                    return internal_err!(
                        "distributed_sequence_id count partition is out of range"
                    );
                };
                if ids.is_null(i) || rows.is_null(i) || slot.replace(rows.value(i)).is_some() {
                    return internal_err!("invalid or duplicate distributed_sequence_id count");
                }
            }
        }
        let sizes = sizes
            .into_iter()
            .map(|size| {
                size.ok_or_else(|| {
                    exec_datafusion_err!("missing distributed_sequence_id partition count")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        (sizes, None)
    } else {
        // Poll all partitions concurrently so bounded repartition channels can drain.
        let streams = (0..partitions)
            .map(|p| input.execute(p, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let cached = futures::future::try_join_all(
            streams
                .into_iter()
                .enumerate()
                .map(|(p, stream)| cache_partition(stream, p, context.clone(), metrics.clone())),
        )
        .await?;
        (cached.iter().map(|p| p.rows).collect(), Some(cached))
    };
    let mut total = 0;
    let offsets = counts
        .into_iter()
        .map(|rows| {
            let offset = total;
            total = checked_end(total, rows)?;
            Ok(offset)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SequenceState {
        offsets,
        partitions: cached,
    })
}

fn checked_end(start: u64, rows: u64) -> Result<u64> {
    start
        .checked_add(rows)
        .filter(|end| *end <= i64::MAX as u64 + 1)
        .ok_or_else(|| {
            exec_datafusion_err!("distributed_sequence_id overflow: exceeded Int64 range")
        })
}

fn attach_ids(
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    mut offset: u64,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let stream = input.map(move |batch| {
        let batch = batch?;
        let end = checked_end(offset, batch.num_rows() as u64)?;
        let ids = Int64Array::from_iter_values((offset..end).map(|id| id as i64));
        offset = end;
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(ids));
        Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// One count row per input partition, including empty partitions.
#[derive(Debug)]
pub struct PartitionCountsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl PartitionCountsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Self::schema_ref()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            input.boundedness(),
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn schema_ref() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("partition", DataType::UInt64, false),
            Field::new("count", DataType::UInt64, false),
        ]))
    }
}

impl DisplayAs for PartitionCountsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PartitionCountsExec")
    }
}

impl ExecutionPlan for PartitionCountsExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("PartitionCountsExec requires one input");
        };
        Ok(Arc::new(Self::new(input.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output_schema = schema.clone();
        let future = async move {
            let mut count = 0;
            while let Some(batch) = input.try_next().await? {
                count = checked_end(count, batch.num_rows() as u64)?;
            }
            Ok(RecordBatch::try_new(
                output_schema,
                vec![
                    Arc::new(UInt64Array::from(vec![partition as u64])),
                    Arc::new(UInt64Array::from(vec![count])),
                ],
            )?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(future),
        )))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::catalog::MemTable;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![("value", Arc::new(Int64Array::from(values)) as _)])
            .unwrap()
    }

    #[tokio::test]
    async fn materialization_preserves_batches_and_releases_memory_and_spills() -> Result<()> {
        for budget in [usize::MAX, 0] {
            let environment = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(budget)))
                .build_arc()?;
            let context = Arc::new(TaskContext::default().with_runtime(environment.clone()));
            let table = MemTable::try_new(
                batch(vec![]).schema(),
                vec![
                    vec![batch(vec![10, 11]), batch(vec![]), batch(vec![12])],
                    vec![],
                    vec![batch(vec![13]), batch(vec![14, 15])],
                ],
            )?;
            let input = table
                .scan(&SessionContext::new().state(), None, &[], None)
                .await?;
            let sequence = DistributedSequenceIdExec::try_new(input, "index".into(), None)?;
            // Consumers may request partitions in any order, including reading one again.
            for (partition, expected) in [
                (2, vec![(13, 3), (14, 4), (15, 5)]),
                (0, vec![(10, 0), (11, 1), (12, 2)]),
                (1, vec![]),
                (2, vec![(13, 3), (14, 4), (15, 5)]),
            ] {
                let batches = sequence
                    .execute(partition, context.clone())?
                    .try_collect::<Vec<_>>()
                    .await?;
                let actual = batches
                    .iter()
                    .flat_map(|batch| {
                        let values = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let ids = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        values
                            .values()
                            .iter()
                            .copied()
                            .zip(ids.values().iter().copied())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
            if budget == 0 {
                assert!(
                    environment
                        .disk_manager
                        .spilling_progress()
                        .active_files_count
                        > 0
                );
            } else {
                assert!(environment.memory_pool.reserved() > 0);
            }
            drop(sequence);
            assert_eq!(environment.memory_pool.reserved(), 0);
            assert_eq!(
                environment
                    .disk_manager
                    .spilling_progress()
                    .active_files_count,
                0
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_materialization_releases_resources() -> Result<()> {
        for budget in [usize::MAX, 0] {
            let environment = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(budget)))
                .build_arc()?;
            let context = Arc::new(TaskContext::default().with_runtime(environment.clone()));
            let batch = batch(vec![1, 2]);
            let input = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                stream::iter(vec![Ok(batch), Err(exec_datafusion_err!("input failed"))]),
            ));
            let result = cache_partition(input, 0, context, ExecutionPlanMetricsSet::new()).await;
            assert!(result.is_err());
            assert_eq!(environment.memory_pool.reserved(), 0);
            assert_eq!(
                environment
                    .disk_manager
                    .spilling_progress()
                    .active_files_count,
                0
            );
        }
        Ok(())
    }

    #[test]
    fn sequence_overflow_is_checked() {
        let max = i64::MAX as u64;
        assert_eq!(checked_end(max, 1).unwrap(), max + 1);
        assert!(checked_end(max, 2).is_err());
        assert!(checked_end(u64::MAX, 1).is_err());
    }
}
