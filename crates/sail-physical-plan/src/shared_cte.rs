use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{
    DataFusionError, Result, Statistics, internal_datafusion_err, internal_err,
};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

/// The size of the batches of a shared CTE that are kept in memory when the memory pool
/// is bounded. The other batches are written to disk, as the operators that can spill
/// do under a bounded pool, since the memory of the batches cannot be given back to the
/// operators of the query that cannot spill.
pub const SHARED_CTE_MEMORY_LIMIT: usize = 1024 * 1024;

/// Returns the size of the batches of a shared CTE that are kept in memory, in total
/// across its partitions. In cluster mode, where each partition of a shared CTE is
/// produced by its own task, the size applies to each partition.
///
/// A shared CTE is kept in memory in full when the memory pool is unbounded, where no
/// operator writes to disk, so that disk is never required for a shared CTE that the
/// query would not require otherwise.
pub fn shared_cte_memory_limit(context: &TaskContext) -> usize {
    match context.memory_pool().memory_limit() {
        MemoryLimit::Infinite => usize::MAX,
        MemoryLimit::Finite(_) | MemoryLimit::Unknown => SHARED_CTE_MEMORY_LIMIT,
    }
}

/// The batches of a stream, which are read any number of times after the stream ends.
///
/// The first batches are kept in memory, up to a size and within the memory pool, and
/// the other batches are written to disk, so that the buffer does not hold the memory
/// of other operators. The size is unlimited under an unbounded memory pool (see
/// [`shared_cte_memory_limit`]). All batches stay in memory if they cannot be written
/// to disk.
pub struct ReplayBuffer {
    spill: SpillManager,
    chunks: Vec<ReplayChunk>,
    /// The size of the batches that are read from disk.
    batch_size: usize,
    /// The memory of the batches that are not spilled.
    _reservation: MemoryReservation,
}

#[derive(Clone)]
enum ReplayChunk {
    Memory(Arc<Vec<RecordBatch>>),
    Spilled(Arc<dyn SpillFile>),
}

impl ReplayBuffer {
    /// Reads the stream to its end. The batches are kept in memory while their size fits
    /// in `memory`, the number of bytes that the buffers of a shared CTE can still keep
    /// in memory.
    pub async fn try_new(
        mut stream: SendableRecordBatchStream,
        context: &TaskContext,
        name: String,
        metrics: SpillMetrics,
        memory: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let spill = SpillManager::new(context.runtime_env(), metrics, stream.schema())
            .with_compression_type(context.session_config().spill_compression());
        let reservation = MemoryConsumer::new(name)
            .with_can_spill(true)
            .register(context.memory_pool());
        let batch_size = context.session_config().batch_size().max(1);
        let mut chunks = vec![];
        let mut batches = vec![];
        let mut file = None;
        // Whether the batches can be written to disk.
        let mut spillable = true;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            // The size of the rows of the batch, which can be a slice of larger arrays.
            let size = batch
                .columns()
                .iter()
                .map(|column| column.to_data().get_slice_memory_size())
                .sum::<std::result::Result<usize, _>>()?;
            if file.is_none() && spillable {
                let fits = memory
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |memory| {
                        memory.checked_sub(size)
                    })
                    .is_ok();
                if fits && reservation.try_grow(size).is_ok() {
                    batches.push(batch);
                    continue;
                }
                if fits {
                    memory.fetch_add(size, Ordering::Relaxed);
                }
                match spill.create_in_progress_file("SharedCteExec") {
                    Ok(created) => {
                        if !batches.is_empty() {
                            chunks
                                .push(ReplayChunk::Memory(Arc::new(std::mem::take(&mut batches))));
                        }
                        file = Some(created);
                    }
                    Err(_) => spillable = false,
                }
            }
            match &mut file {
                Some(file) => {
                    file.append_batch(&batch)?;
                }
                None => {
                    reservation.grow(size);
                    batches.push(batch);
                }
            }
        }
        if !batches.is_empty() {
            chunks.push(ReplayChunk::Memory(Arc::new(batches)));
        }
        if let Some(written) = file
            .as_mut()
            .map(|file| file.finish())
            .transpose()?
            .flatten()
        {
            chunks.push(ReplayChunk::Spilled(written));
        }
        Ok(Self {
            spill,
            chunks,
            batch_size,
            _reservation: reservation,
        })
    }

    /// Returns the batches in the order of the stream.
    pub fn stream(self: &Arc<Self>) -> stream::BoxStream<'static, Result<RecordBatch>> {
        let buffer = Arc::clone(self);
        stream::iter(self.chunks.clone())
            .flat_map(move |chunk| match chunk {
                ReplayChunk::Memory(batches) => {
                    stream::iter((0..batches.len()).map(move |i| Ok(batches[i].clone()))).boxed()
                }
                ReplayChunk::Spilled(file) => match buffer.spill.read_spill_as_stream(file, None) {
                    Ok(stream) => {
                        let batch_size = buffer.batch_size;
                        stream
                            .flat_map(move |batch| stream::iter(compact(batch, batch_size)))
                            .boxed()
                    }
                    Err(error) => stream::once(async move { Err(error) }).boxed(),
                },
            })
            .boxed()
    }
}

/// Copies a batch that is read from disk into new batches of at most the given size.
/// The arrays of a batch that is read from disk share the memory of the whole batch,
/// and an operator that receives a batch larger than the size can keep slices of it,
/// so that the operators would account for more memory than they keep.
fn compact(batch: Result<RecordBatch>, batch_size: usize) -> Vec<Result<RecordBatch>> {
    let batch = match batch {
        Ok(batch) => batch,
        Err(error) => return vec![Err(error)],
    };
    let rows = batch.num_rows();
    (0..rows)
        .step_by(batch_size)
        .map(|offset| {
            let length = batch_size.min(rows - offset);
            let indices = UInt32Array::from_iter_values(offset as u32..(offset + length) as u32);
            let columns = batch
                .columns()
                .iter()
                .map(|column| {
                    // An array of a type that cannot be copied keeps the memory of the batch.
                    let column = take(column, &indices, None)
                        .unwrap_or_else(|_| column.slice(offset, length));
                    match column.data_type() {
                        DataType::Utf8View => Arc::new(column.as_string_view().gc()) as ArrayRef,
                        DataType::BinaryView => Arc::new(column.as_binary_view().gc()),
                        _ => column,
                    }
                })
                .collect();
            let options = RecordBatchOptions::new().with_row_count(Some(length));
            Ok(RecordBatch::try_new_with_options(
                batch.schema(),
                columns,
                &options,
            )?)
        })
        .collect()
}

/// The output of a shared CTE, which is produced once and read by every reference.
///
/// The output is produced in full before it is read, since references that are inputs
/// of the same operator (e.g. a self join) are not read at the same pace. It is
/// produced when a reference is first read, and released when the plan that contains
/// the references has returned its output.
pub struct SharedCte {
    id: usize,
    name: String,
    /// The partitioning of the output, which the references report as their own.
    partitioning: Partitioning,
    state: Mutex<SharedCteState>,
}

#[derive(Default)]
struct SharedCteState {
    producer: Option<SharedCteProducer>,
    output: Option<Shared<BoxFuture<'static, SharedCteResult>>>,
}

type SharedCteResult = std::result::Result<Arc<Vec<Arc<ReplayBuffer>>>, Arc<DataFusionError>>;

#[derive(Clone)]
struct SharedCteProducer {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    metrics: SpillMetrics,
}

impl std::fmt::Debug for SharedCte {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCte")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish()
    }
}

impl SharedCte {
    /// Creates the shared CTE with an identifier that is unique in its plan, and with
    /// the partitioning of the plan that produces it.
    pub fn new(id: usize, name: String, partitioning: Partitioning) -> Self {
        Self {
            id,
            name,
            partitioning,
            state: Mutex::new(SharedCteState::default()),
        }
    }

    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, SharedCteState>> {
        self.state
            .lock()
            .map_err(|_| internal_datafusion_err!("shared CTE state is poisoned"))
    }

    /// Sets the plan that produces the output. The plan is repartitioned if it does not
    /// have the partitioning of the shared CTE anymore, since each partition of a
    /// reference is a partition of the output.
    fn produce(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<()> {
        let mut state = self.state()?;
        if state.producer.is_none() {
            // The comparison is by name, since unknown columns are not equal to themselves.
            let partitioning = plan.output_partitioning();
            let plan = if partitioning.to_string() == self.partitioning.to_string() {
                Arc::clone(plan)
            } else {
                // The references only report a partitioning by columns.
                let partitions = self.partitioning.partition_count();
                let partitioning = match &self.partitioning {
                    Partitioning::Hash(keys, _) if keys.iter().all(|key| key.is::<Column>()) => {
                        self.partitioning.clone()
                    }
                    _ => Partitioning::RoundRobinBatch(partitions),
                };
                Arc::new(RepartitionExec::try_new(Arc::clone(plan), partitioning)?)
            };
            state.producer = Some(SharedCteProducer {
                plan,
                context: Arc::clone(context),
                metrics: SpillMetrics::new(metrics, self.id),
            });
        }
        Ok(())
    }

    /// Returns the output, which is produced when it is first requested.
    fn output(&self) -> Result<Shared<BoxFuture<'static, SharedCteResult>>> {
        let mut state = self.state()?;
        if let Some(output) = &state.output {
            return Ok(output.clone());
        }
        let Some(producer) = state.producer.clone() else {
            return internal_err!("shared CTE {}#{} has no producer", self.name, self.id);
        };
        let output = async move {
            // The task outlives the reference that requests the output first.
            match SpawnedTask::spawn(producer.run()).join().await {
                Ok(output) => output.map(Arc::new).map_err(Arc::new),
                Err(error) => Err(Arc::new(DataFusionError::ExecutionJoin(Box::new(error)))),
            }
        }
        .boxed()
        .shared();
        state.output = Some(output.clone());
        Ok(output)
    }

    /// Releases the output and stops producing it. The output is produced again if it
    /// is requested after that.
    fn release(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.output = None;
        }
    }
}

/// Releases the outputs of the shared CTEs of a plan when every partition of the plan
/// is read or dropped.
struct SharedCteOutputGuard {
    ctes: Vec<Arc<SharedCte>>,
    /// The number of partitions of the plan that are read or dropped.
    finished: Arc<AtomicUsize>,
    partitions: usize,
}

impl Drop for SharedCteOutputGuard {
    fn drop(&mut self) {
        if self.finished.fetch_add(1, Ordering::SeqCst) + 1 >= self.partitions {
            self.finished.store(0, Ordering::SeqCst);
            for cte in &self.ctes {
                cte.release();
            }
        }
    }
}

impl SharedCteProducer {
    async fn run(self) -> Result<Vec<Arc<ReplayBuffer>>> {
        let Self {
            plan,
            context,
            metrics,
        } = self;
        let partitions = plan.output_partitioning().partition_count();
        // The partitions share the memory of the CTE.
        let memory = Arc::new(AtomicUsize::new(shared_cte_memory_limit(&context)));
        let tasks = (0..partitions)
            .map(|partition| {
                let stream = plan.execute(partition, Arc::clone(&context))?;
                let context = Arc::clone(&context);
                let metrics = metrics.clone();
                let memory = Arc::clone(&memory);
                Ok(SpawnedTask::spawn(async move {
                    ReplayBuffer::try_new(
                        stream,
                        &context,
                        format!("SharedCteExec[{partition}]"),
                        metrics,
                        memory,
                    )
                    .await
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut buffers = Vec::with_capacity(tasks.len());
        for task in tasks {
            let buffer = task
                .join()
                .await
                .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))??;
            buffers.push(Arc::new(buffer));
        }
        Ok(buffers)
    }
}

/// Produces the output of shared CTEs for the references in its input and in the
/// CTEs themselves, and returns the output of its input.
///
/// The first child is the input and the other children are the plans of the shared
/// CTEs, where a CTE can refer to the CTEs before it. A CTE is executed when one of
/// its references is first read.
#[derive(Debug)]
pub struct SharedCteExec {
    input: Arc<dyn ExecutionPlan>,
    ctes: Vec<(Arc<SharedCte>, Arc<dyn ExecutionPlan>)>,
    /// The number of partitions of the output that are read or dropped.
    finished: Arc<AtomicUsize>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl SharedCteExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        ctes: Vec<(Arc<SharedCte>, Arc<dyn ExecutionPlan>)>,
    ) -> Self {
        let properties = Arc::new(input.properties().as_ref().clone());
        Self {
            input,
            ctes,
            finished: Arc::new(AtomicUsize::new(0)),
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn ctes(&self) -> &[(Arc<SharedCte>, Arc<dyn ExecutionPlan>)] {
        &self.ctes
    }
}

impl DisplayAs for SharedCteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedCteExec: ctes=[")?;
        for (i, (cte, _)) in self.ctes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}#{}", cte.name, cte.id)?;
        }
        write!(f, "]")
    }
}

impl ExecutionPlan for SharedCteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        let mut order = vec![false; self.ctes.len() + 1];
        order[0] = true;
        order
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.ctes.len() + 1]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        std::iter::once(&self.input)
            .chain(self.ctes.iter().map(|(_, plan)| plan))
            .collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        if children.len() != self.ctes.len() + 1 {
            return internal_err!("{} requires {} children", self.name(), self.ctes.len() + 1);
        }
        let mut children = children.into_iter();
        let Some(input) = children.next() else {
            return internal_err!("{} requires an input", self.name());
        };
        let ctes = self
            .ctes
            .iter()
            .zip(children)
            .map(|((cte, _), plan)| (Arc::clone(cte), plan))
            .collect();
        Ok(Arc::new(Self::new(input, ctes)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        for (cte, plan) in &self.ctes {
            cte.produce(plan, &context, &self.metrics)?;
        }
        let guard = SharedCteOutputGuard {
            ctes: self.ctes.iter().map(|(cte, _)| Arc::clone(cte)).collect(),
            finished: Arc::clone(&self.finished),
            partitions: self.properties.output_partitioning().partition_count(),
        };
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let _ = &guard;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        match input_stats.first() {
            Some(statistics) => Ok(Arc::clone(statistics)),
            None => Ok(Arc::new(Statistics::new_unknown(&self.schema()))),
        }
    }
}

/// Reads the output of a shared CTE for one reference.
#[derive(Debug)]
pub struct SharedCteScanExec {
    cte: Arc<SharedCte>,
    /// The columns of the shared CTE that the reference reads, or `None` for all columns.
    projection: Option<Vec<usize>>,
    /// The statistics of the output of all partitions.
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl SharedCteScanExec {
    /// Creates the plan for a reference to a shared CTE. The reference has the given
    /// output schema and reads the given columns of the CTE, which has the given number
    /// of partitions and statistics.
    pub fn new(
        cte: Arc<SharedCte>,
        schema: SchemaRef,
        statistics: &Statistics,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let partitioning = cte.partitioning();
        // Each partition of the reference is a partition of the CTE, so the rows are
        // partitioned by the same columns if the reference reads them.
        let partitions = partitioning.partition_count();
        let partitioning = match partitioning {
            Partitioning::Hash(keys, _) => keys
                .iter()
                .map(|key| {
                    let index = key.downcast_ref::<Column>()?.index();
                    let position = match &projection {
                        Some(projection) => projection.iter().position(|&x| x == index)?,
                        None => index,
                    };
                    let name = schema.fields().get(position)?.name();
                    Some(Arc::new(Column::new(name, position)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Option<Vec<_>>>()
                .map(|keys| Partitioning::Hash(keys, partitions)),
            _ => None,
        }
        .unwrap_or(Partitioning::UnknownPartitioning(partitions));
        // The column statistics of the CTE are not those of the declared columns.
        let mut output = Statistics::new_unknown(&schema);
        output.num_rows = statistics.num_rows;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            cte,
            statistics: Arc::new(output),
            projection,
            properties,
        }
    }

    pub fn cte(&self) -> &Arc<SharedCte> {
        &self.cte
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }
}

impl DisplayAs for SharedCteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SharedCteScanExec: cte={}#{}",
            self.cte.name, self.cte.id
        )?;
        if let Some(projection) = &self.projection {
            write!(f, ", projection={projection:?}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SharedCteScanExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        if !children.is_empty() {
            return internal_err!("{} should have no children", self.name());
        }
        Ok(self)
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(match args.partition() {
            Some(_) => Arc::new(Statistics::new_unknown(&self.schema())),
            None => Arc::clone(&self.statistics),
        })
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.properties.output_partitioning().partition_count();
        if partition >= partitions {
            return internal_err!(
                "{}: partition index {} out of range ({})",
                self.name(),
                partition,
                partitions
            );
        }
        let output = self.cte.output()?;
        let projection = self.projection.clone();
        let schema = self.schema();
        let stream = stream::once(async move {
            let output = output.await.map_err(DataFusionError::Shared)?;
            // Each partition of the reference is a partition of the output.
            let buffer = output.get(partition).cloned();
            Ok::<_, DataFusionError>(stream::iter(buffer).flat_map(|buffer| buffer.stream()).map(
                move |batch| {
                    let batch = batch?;
                    let columns = match &projection {
                        Some(projection) => projection
                            .iter()
                            .map(|&index| Arc::clone(batch.column(index)))
                            .collect(),
                        None => batch.columns().to_vec(),
                    };
                    // The reference declares the schema of the CTE definition.
                    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                    Ok(RecordBatch::try_new_with_options(
                        Arc::clone(&schema),
                        columns,
                        &options,
                    )?)
                },
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::union::UnionExec;

    use super::*;

    fn plan(partitions: usize) -> Result<(SchemaRef, Arc<dyn ExecutionPlan>)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let partitions = (0..partitions as i64)
            .map(|partition| {
                (0..4)
                    .map(|batch| {
                        let a = (0..1024).map(|row| partition * 10000 + batch * 1024 + row);
                        Ok(RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(Int64Array::from_iter_values(a.clone())),
                                Arc::new(Int64Array::from_iter_values(a.map(|x| -x))),
                            ],
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        Ok((schema, plan))
    }

    async fn read(memory_limit: Option<usize>, disk: bool) -> Result<(Vec<i64>, usize)> {
        let (schema, producer) = plan(3)?;
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(3),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan = |column: usize| -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(SharedCteScanExec::new(
                Arc::clone(&cte),
                Arc::new(schema.project(&[column])?),
                &statistics,
                Some(vec![column]),
            )))
        };
        let input = UnionExec::try_new(vec![scan(1)?, scan(0)?])?;
        let shared = Arc::new(SharedCteExec::new(
            input,
            vec![(Arc::clone(&cte), producer)],
        ));
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(memory_limit) = memory_limit {
            runtime = runtime.with_memory_limit(memory_limit, 1.0);
        }
        if !disk {
            runtime = runtime.with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            );
        }
        let context = Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
        let batches = collect(Arc::clone(&shared) as Arc<dyn ExecutionPlan>, context).await?;
        // The output is released once every partition of every reference is read.
        assert!(cte.state()?.output.is_none());
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|x| x.values().to_vec())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        let spilled = shared
            .metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0);
        Ok((values, spilled))
    }

    #[tokio::test]
    async fn test_shared_cte_is_read_by_every_reference() -> Result<()> {
        let mut expected = (0..3)
            .flat_map(|partition| (0..4096).map(move |row| partition * 10000 + row))
            .flat_map(|x| [x, -x])
            .collect::<Vec<i64>>();
        expected.sort_unstable();
        let (values, spilled) = read(None, true).await?;
        assert_eq!(values, expected);
        assert_eq!(spilled, 0);
        // The output does not fit in memory and is spilled to disk.
        let (values, spilled) = read(Some(64 * 1024), true).await?;
        assert_eq!(values, expected);
        assert!(spilled > 0);
        // The output stays in memory when temporary files are disabled.
        let (values, spilled) = read(Some(64 * 1024), false).await?;
        assert_eq!(values, expected);
        assert_eq!(spilled, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_error_reaches_every_reference() -> Result<()> {
        use datafusion::arrow::array::StringArray;
        use datafusion::physical_expr::expressions::{CastExpr, Column};
        use datafusion::physical_plan::projection::ProjectionExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["1", "x"]))],
        )?;
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        // The cast fails when the CTE is produced.
        let cast = Arc::new(CastExpr::new(
            Arc::new(Column::new("a", 0)),
            DataType::Int64,
            None,
        ));
        let producer: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![(cast as _, "a".to_string())],
            source,
        )?);
        let schema = producer.schema();
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(1),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan = || -> Arc<dyn ExecutionPlan> {
            Arc::new(SharedCteScanExec::new(
                Arc::clone(&cte),
                Arc::clone(&schema),
                &statistics,
                None,
            ))
        };
        let input = UnionExec::try_new(vec![scan(), scan()])?;
        let shared = Arc::new(SharedCteExec::new(input, vec![(cte, producer)]));
        let context = Arc::new(TaskContext::default());
        for partition in 0..2 {
            let stream = shared.execute(partition, Arc::clone(&context))?;
            let result = datafusion::physical_plan::common::collect(stream).await;
            assert!(result.is_err());
        }
        Ok(())
    }

    #[test]
    fn test_shared_cte_reference_partitioning() -> Result<()> {
        let (schema, _) = plan(1)?;
        let partitioning = Partitioning::Hash(vec![Arc::new(Column::new("b", 1))], 3);
        let cte = Arc::new(SharedCte::new(0, "c".to_string(), partitioning));
        let statistics = Statistics::new_unknown(&schema);
        let scan = |projection: Option<Vec<usize>>| -> Result<Partitioning> {
            let schema = match &projection {
                Some(projection) => Arc::new(schema.project(projection)?),
                None => Arc::clone(&schema),
            };
            let scan = SharedCteScanExec::new(Arc::clone(&cte), schema, &statistics, projection);
            Ok(scan.properties().output_partitioning().clone())
        };
        // The reference is partitioned by the hash key of the CTE if it reads the key.
        assert_eq!(scan(None)?.to_string(), "Hash([b@1], 3)");
        assert_eq!(scan(Some(vec![1, 0]))?.to_string(), "Hash([b@0], 3)");
        assert_eq!(scan(Some(vec![0]))?.to_string(), "UnknownPartitioning(3)");
        Ok(())
    }

    /// Reads twice a shared CTE of three partitions with the given numbers of batches of
    /// 1 MiB, and returns the number of rows and the number of rows written to disk.
    async fn read_large(batches: [usize; 3], runtime: RuntimeEnvBuilder) -> Result<(usize, usize)> {
        const ROWS: usize = 1024 * 1024 / 8;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64))],
        )?;
        let partitions = batches.map(|n| vec![batch.clone(); n]);
        let producer = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(3),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan = || -> Arc<dyn ExecutionPlan> {
            Arc::new(SharedCteScanExec::new(
                Arc::clone(&cte),
                Arc::clone(&schema),
                &statistics,
                None,
            ))
        };
        let input = UnionExec::try_new(vec![scan(), scan()])?;
        let shared = Arc::new(SharedCteExec::new(
            input,
            vec![(Arc::clone(&cte), producer)],
        ));
        let context = Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
        let mut rows = 0;
        for partition in 0..6 {
            let mut stream = shared.execute(partition, Arc::clone(&context))?;
            while let Some(batch) = stream.next().await {
                rows += batch?.num_rows();
            }
        }
        let spilled = shared
            .metrics()
            .and_then(|metrics| metrics.spilled_rows())
            .unwrap_or(0);
        Ok((rows / ROWS, spilled / ROWS))
    }

    #[tokio::test]
    async fn test_shared_cte_stays_in_memory_under_an_unbounded_pool() -> Result<()> {
        // A CTE of 128 MiB is not written to disk.
        let (rows, spilled) = read_large([64, 32, 32], RuntimeEnvBuilder::new()).await?;
        assert_eq!((rows, spilled), (256, 0));
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_memory_is_bounded_in_total_under_a_bounded_pool() -> Result<()> {
        let limit = SHARED_CTE_MEMORY_LIMIT / (1024 * 1024);
        // Only a prefix of the CTE stays in memory, whatever the sizes of its partitions,
        // although the pool has room for more.
        let runtime = RuntimeEnvBuilder::new().with_memory_limit(1024 * 1024 * 1024, 1.0);
        let (rows, spilled) = read_large([16, 8, 8], runtime).await?;
        assert_eq!((rows, spilled), (64, 32 - limit));
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_reads_new_batches_from_disk() -> Result<()> {
        use datafusion::arrow::array::{StringArray, StringViewArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8View, false),
        ]));
        let strings = (0..20000).map(|x| format!("{x:064}")).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..20000)),
                Arc::new(StringArray::from_iter_values(&strings)),
                Arc::new(StringViewArray::from_iter_values(&strings)),
            ],
        )?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let context = TaskContext::default();
        let stream = input.execute(0, Arc::new(TaskContext::default()))?;
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        // The batch is written to disk, since no memory is left for the CTE.
        let memory = Arc::new(AtomicUsize::new(0));
        let buffer = ReplayBuffer::try_new(stream, &context, "c".to_string(), metrics, memory);
        let batches = Arc::new(buffer.await?)
            .stream()
            .try_collect::<Vec<_>>()
            .await?;
        let rows = batches.iter().map(|x| x.num_rows()).collect::<Vec<_>>();
        assert_eq!(rows, vec![8192, 8192, 3616]);
        // Each array holds the memory of its own values, and not that of the batch.
        for batch in &batches {
            let rows = batch.num_rows();
            assert!(batch.column(0).get_array_memory_size() < rows * 8 + 1024);
            assert!(batch.column(1).get_array_memory_size() < rows * 72 + 1024);
            assert!(batch.column(2).get_array_memory_size() < rows * 80 + 1024);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_spill_failure_is_an_error() -> Result<()> {
        // The temporary files of the CTE exceed the size that they are allowed to have.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(1024 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(1024 * 1024),
            );
        let result = read_large([16, 0, 0], runtime).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_keeps_its_partitioning() -> Result<()> {
        // The plan that produces the CTE has three partitions when it is executed, while
        // the references were planned for two.
        let (schema, producer) = plan(3)?;
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(2),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan: Arc<dyn ExecutionPlan> = Arc::new(SharedCteScanExec::new(
            Arc::clone(&cte),
            Arc::clone(&schema),
            &statistics,
            None,
        ));
        assert_eq!(scan.output_partitioning().partition_count(), 2);
        let shared = Arc::new(SharedCteExec::new(scan, vec![(cte, producer)]));
        let batches = collect(shared, Arc::new(TaskContext::default())).await?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(rows, 3 * 4096);
        Ok(())
    }
}
