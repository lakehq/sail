use std::fmt::Formatter;
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::{SpillManager, get_record_batch_memory_size};
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

/// The output of a shared CTE, which is produced once and read by every reference.
///
/// The output is produced in full before it is read, since references that are inputs
/// of the same operator (e.g. a self join) are not read at the same pace. A bounded
/// part of each partition is kept in memory, and the rest on disk.
pub struct SharedCte {
    id: usize,
    name: String,
    producer: Mutex<Option<SharedCteProducer>>,
    output: OnceLock<Shared<BoxFuture<'static, SharedCteResult>>>,
}

type SharedCteResult = std::result::Result<Arc<SharedCteOutput>, Arc<DataFusionError>>;

struct SharedCteProducer {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    metrics: SpillMetrics,
}

struct SharedCteOutput {
    spill: SpillManager,
    partitions: Vec<SharedCtePartition>,
}

struct SharedCtePartition {
    chunks: Vec<SharedCteChunk>,
    /// The memory of the batches that are not spilled.
    _reservation: MemoryReservation,
}

#[derive(Clone)]
enum SharedCteChunk {
    Memory(Arc<Vec<RecordBatch>>),
    Spilled(Arc<dyn SpillFile>),
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
    /// Creates the shared CTE with an identifier that is unique in its plan.
    pub fn new(id: usize, name: String) -> Self {
        Self {
            id,
            name,
            producer: Mutex::new(None),
            output: OnceLock::new(),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn produce(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<()> {
        let mut producer = self
            .producer
            .lock()
            .map_err(|_| internal_datafusion_err!("shared CTE producer is poisoned"))?;
        if producer.is_none() && self.output.get().is_none() {
            *producer = Some(SharedCteProducer {
                plan: Arc::clone(plan),
                context: Arc::clone(context),
                metrics: SpillMetrics::new(metrics, self.id),
            });
        }
        Ok(())
    }

    /// Returns the output, which is produced when it is first requested.
    fn output(&self) -> Result<Shared<BoxFuture<'static, SharedCteResult>>> {
        let mut producer = self
            .producer
            .lock()
            .map_err(|_| internal_datafusion_err!("shared CTE producer is poisoned"))?;
        if let Some(output) = self.output.get() {
            return Ok(output.clone());
        }
        let Some(producer) = producer.take() else {
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
        Ok(self.output.get_or_init(|| output).clone())
    }
}

impl SharedCteProducer {
    async fn run(self) -> Result<SharedCteOutput> {
        let Self {
            plan,
            context,
            metrics,
        } = self;
        let spill = SpillManager::new(context.runtime_env(), metrics, plan.schema())
            .with_compression_type(context.session_config().spill_compression());
        // The output keeps in memory what a sort keeps in memory to sort in place.
        let partitions = plan.output_partitioning().partition_count();
        let memory_limit = context
            .session_config()
            .options()
            .execution
            .sort_in_place_threshold_bytes
            / partitions.max(1);
        let tasks = (0..partitions)
            .map(|partition| {
                let stream = plan.execute(partition, Arc::clone(&context))?;
                let reservation = MemoryConsumer::new(format!("SharedCteExec[{partition}]"))
                    .with_can_spill(true)
                    .register(context.memory_pool());
                Ok(SpawnedTask::spawn(Self::buffer(
                    stream,
                    reservation,
                    spill.clone(),
                    memory_limit,
                )))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut partitions = Vec::with_capacity(tasks.len());
        for task in tasks {
            let partition = task
                .join()
                .await
                .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))??;
            partitions.push(partition);
        }
        Ok(SharedCteOutput { spill, partitions })
    }

    /// Buffers a partition of the output. The first batches are kept in memory, up to
    /// the given size and within the memory pool, and the other batches are written to
    /// disk, so that the output of a CTE does not hold the memory of other operators.
    async fn buffer(
        mut stream: SendableRecordBatchStream,
        reservation: MemoryReservation,
        spill: SpillManager,
        memory_limit: usize,
    ) -> Result<SharedCtePartition> {
        let mut batches = vec![];
        let mut file = None;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            if file.is_none() {
                let size = get_record_batch_memory_size(&batch);
                if reservation.size() + size <= memory_limit && reservation.try_grow(size).is_ok() {
                    batches.push(batch);
                    continue;
                }
                file = Some(spill.create_in_progress_file("SharedCteExec")?);
            }
            if let Some(file) = &mut file {
                file.append_batch(&batch)?;
            }
        }
        let mut chunks = vec![];
        if !batches.is_empty() {
            chunks.push(SharedCteChunk::Memory(Arc::new(batches)));
        }
        if let Some(file) = file
            .as_mut()
            .map(|file| file.finish())
            .transpose()?
            .flatten()
        {
            chunks.push(SharedCteChunk::Spilled(file));
        }
        Ok(SharedCtePartition {
            chunks,
            _reservation: reservation,
        })
    }
}

impl SharedCteOutput {
    fn read(
        self: Arc<Self>,
        chunk: SharedCteChunk,
    ) -> stream::BoxStream<'static, Result<RecordBatch>> {
        match chunk {
            SharedCteChunk::Memory(batches) => {
                stream::iter((0..batches.len()).map(move |i| Ok(batches[i].clone()))).boxed()
            }
            SharedCteChunk::Spilled(file) => match self.spill.read_spill_as_stream(file, None) {
                Ok(stream) => stream.boxed(),
                Err(error) => stream::once(async move { Err(error) }).boxed(),
            },
        }
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
        self.input.execute(partition, context)
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
        partitions: usize,
        statistics: &Statistics,
        projection: Option<Vec<usize>>,
    ) -> Self {
        // The column statistics of the CTE are not those of the declared columns.
        let mut output = Statistics::new_unknown(&schema);
        output.num_rows = statistics.num_rows;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitions),
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
            // The partitions of the output are the partitions of the reference.
            let chunks = output
                .partitions
                .iter()
                .skip(partition)
                .step_by(partitions)
                .flat_map(|partition| partition.chunks.iter().cloned())
                .collect::<Vec<_>>();
            Ok::<_, DataFusionError>(
                stream::iter(chunks)
                    .flat_map(move |chunk| Arc::clone(&output).read(chunk))
                    .map(move |batch| {
                        let batch = batch?;
                        let columns = match &projection {
                            Some(projection) => projection
                                .iter()
                                .map(|&index| Arc::clone(batch.column(index)))
                                .collect(),
                            None => batch.columns().to_vec(),
                        };
                        // The reference declares the schema of the CTE definition.
                        let options =
                            RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                        Ok(RecordBatch::try_new_with_options(
                            Arc::clone(&schema),
                            columns,
                            &options,
                        )?)
                    }),
            )
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

    async fn read(memory_limit: Option<usize>) -> Result<(Vec<i64>, usize)> {
        let (schema, producer) = plan(3)?;
        let cte = Arc::new(SharedCte::new(0, "c".to_string()));
        let statistics = Statistics::new_unknown(&schema);
        let scan = |column: usize| -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(SharedCteScanExec::new(
                Arc::clone(&cte),
                Arc::new(schema.project(&[column])?),
                3,
                &statistics,
                Some(vec![column]),
            )))
        };
        let input = UnionExec::try_new(vec![scan(1)?, scan(0)?])?;
        let shared = Arc::new(SharedCteExec::new(input, vec![(cte, producer)]));
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(memory_limit) = memory_limit {
            runtime = runtime.with_memory_limit(memory_limit, 1.0);
        }
        let context = Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
        let batches = collect(Arc::clone(&shared) as Arc<dyn ExecutionPlan>, context).await?;
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
        let (values, spilled) = read(None).await?;
        assert_eq!(values, expected);
        assert_eq!(spilled, 0);
        // The output does not fit in memory and is spilled to disk.
        let (values, spilled) = read(Some(64 * 1024)).await?;
        assert_eq!(values, expected);
        assert!(spilled > 0);
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
        let cte = Arc::new(SharedCte::new(0, "c".to_string()));
        let statistics = Statistics::new_unknown(&schema);
        let scan = || -> Arc<dyn ExecutionPlan> {
            Arc::new(SharedCteScanExec::new(
                Arc::clone(&cte),
                Arc::clone(&schema),
                1,
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
}
