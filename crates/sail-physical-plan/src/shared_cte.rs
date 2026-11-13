use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::MemoryLimit;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, Statistics, internal_datafusion_err, internal_err};
use futures::StreamExt;

use crate::shared_cte::tee::{Tee, TeeReader};

pub mod blocking;
pub mod tee;

/// The output of a shared CTE, which is produced once and read by every reference.
///
/// Each partition of the output is a [`Tee`]: it is produced when a reference asks for
/// rows that no reference has read yet, and the rows are kept until every reference has
/// read them, since references that are inputs of the same operator (e.g. a self join)
/// are not read at the same pace. The output is released when the plan that contains
/// the references has returned its output.
pub struct SharedCte {
    id: usize,
    name: String,
    /// The partitioning of the output, which the references report as their own.
    partitioning: Partitioning,
    /// Whether the references must read one evaluation of the CTE.
    volatile: bool,
    state: Mutex<SharedCteState>,
}

#[derive(Default)]
struct SharedCteState {
    producer: Option<SharedCteProducer>,
    /// The partitions of the output.
    output: Option<Arc<Vec<Arc<Tee>>>>,
}

#[derive(Clone)]
struct SharedCteProducer {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    metrics: SpillMetrics,
    /// The number of references, each of which reads every partition of the output.
    references: usize,
    /// Whether a reference is in the build side of a nested loop join, which the join can
    /// execute again.
    rereadable: bool,
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
            volatile: false,
            state: Mutex::new(SharedCteState::default()),
        }
    }

    /// Returns the shared CTE whose references must read one evaluation, if it is
    /// volatile.
    pub fn with_volatile(mut self, volatile: bool) -> Self {
        self.volatile = volatile;
        self
    }

    pub fn volatile(&self) -> bool {
        self.volatile
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

    /// Sets the plan that produces the output for the given number of references. The
    /// plan is repartitioned if it does not have the partitioning of the shared CTE
    /// anymore, since each partition of a reference is a partition of the output.
    fn produce(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
        references: usize,
        rereadable: bool,
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
                references,
                rereadable,
            });
        }
        Ok(())
    }

    /// Returns the partitions of the output, which are produced when they are read.
    fn output(&self) -> Result<Arc<Vec<Arc<Tee>>>> {
        let mut state = self.state()?;
        if let Some(output) = &state.output {
            return Ok(Arc::clone(output));
        }
        let Some(producer) = state.producer.clone() else {
            return internal_err!("shared CTE {}#{} has no producer", self.name, self.id);
        };
        let SharedCteProducer {
            plan,
            context,
            metrics,
            references,
            rereadable,
        } = producer;
        // A nested loop join executes its build side again only to spill it after the
        // memory pool refuses to keep it in memory.
        let runtime = context.runtime_env();
        let keep_all = rereadable
            && matches!(runtime.memory_pool.memory_limit(), MemoryLimit::Finite(_))
            && runtime.disk_manager.tmp_files_enabled();
        let output = (0..plan.output_partitioning().partition_count())
            .map(|partition| {
                let source = {
                    let plan = Arc::clone(&plan);
                    let context = Arc::clone(&context);
                    Box::new(move || plan.execute(partition, context)) as _
                };
                Arc::new(Tee::new(
                    format!("SharedCteExec[{}#{}][{partition}]", self.name, self.id),
                    plan.schema(),
                    source,
                    references,
                    keep_all,
                    &context,
                    metrics.clone(),
                ))
            })
            .collect::<Vec<_>>();
        let output = Arc::new(output);
        state.output = Some(Arc::clone(&output));
        Ok(output)
    }

    /// Releases the output and stops producing it. The output is produced again if it
    /// is read after that.
    fn release(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.output = None;
        }
    }
}

/// Returns the identifiers of the shared CTEs that the plan may read again from the
/// beginning: a nested loop join executes its build side again to spill it, after the
/// memory pool refuses to keep it in memory.
pub fn ctes_read_again(plan: &Arc<dyn ExecutionPlan>) -> HashSet<usize> {
    let mut ids = HashSet::new();
    // The closures never fail.
    let _ = plan.apply(|plan| {
        if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
            let _ = join.left().apply(|plan| {
                if let Some(scan) = plan.downcast_ref::<SharedCteScanExec>() {
                    ids.insert(scan.cte.id());
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ids
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
    /// The number of references of each CTE in the input and in the CTEs, which are
    /// numbered by [`SharedCteScanExec::with_reference`].
    references: Vec<usize>,
    /// Whether each CTE is read in the build side of a nested loop join.
    rereadable: Vec<bool>,
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
        let references = ctes
            .iter()
            .map(|(cte, _)| {
                let mut count = 0;
                for plan in std::iter::once(&input).chain(ctes.iter().map(|(_, plan)| plan)) {
                    // The closure never fails.
                    let _ = plan.apply(|plan| {
                        if let Some(scan) = plan.downcast_ref::<SharedCteScanExec>()
                            && Arc::ptr_eq(&scan.cte, cte)
                        {
                            count = count.max(scan.reference + 1);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    });
                }
                count
            })
            .collect();
        let read_again = std::iter::once(&input)
            .chain(ctes.iter().map(|(_, plan)| plan))
            .flat_map(ctes_read_again)
            .collect::<HashSet<_>>();
        let rereadable = ctes
            .iter()
            .map(|(cte, _)| read_again.contains(&cte.id()))
            .collect();
        Self {
            input,
            ctes,
            references,
            rereadable,
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

    /// Returns the plan in which the given references, as pairs of the identifier of the
    /// shared CTE and the number of the reference, evaluate their CTE themselves.
    pub fn inline_references(
        &self,
        references: &HashSet<(usize, usize)>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let bodies = self
            .ctes
            .iter()
            .map(|(cte, body)| (cte.id(), Arc::clone(body)))
            .collect::<HashMap<_, _>>();
        let inline = |plan: &Arc<dyn ExecutionPlan>| {
            Arc::clone(plan)
                .transform_up(|plan| match plan.downcast_ref::<SharedCteScanExec>() {
                    Some(scan) if references.contains(&(scan.cte.id(), scan.reference)) => {
                        match bodies.get(&scan.cte.id()) {
                            Some(body) => Ok(Transformed::yes(project(
                                Arc::clone(body),
                                scan.projection(),
                            )?)),
                            None => internal_err!("shared CTE {} has no plan", scan.cte.id()),
                        }
                    }
                    _ => Ok(Transformed::no(plan)),
                })
                .data()
        };
        let input = inline(&self.input)?;
        let ctes = self
            .ctes
            .iter()
            .map(|(cte, body)| Ok((Arc::clone(cte), inline(body)?)))
            .collect::<Result<Vec<_>>>()?;
        share_ctes(input, ctes)
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
        for (((cte, plan), references), rereadable) in
            self.ctes.iter().zip(&self.references).zip(&self.rereadable)
        {
            cte.produce(plan, &context, &self.metrics, *references, *rereadable)?;
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
#[derive(Debug, Clone)]
pub struct SharedCteScanExec {
    cte: Arc<SharedCte>,
    /// The number of the reference among the references of the shared CTE.
    reference: usize,
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
        let output = project_statistics(statistics, projection.as_deref(), &schema);
        // A reader of kept batches is always ready, so the scan consumes the scheduling
        // budget of its task, as the other leaves do.
        let properties = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(schema),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        Self {
            cte,
            reference: 0,
            statistics: Arc::new(output),
            projection,
            properties,
        }
    }

    pub fn cte(&self) -> &Arc<SharedCte> {
        &self.cte
    }

    /// Returns the reference with the given number among the references of the shared
    /// CTE, which are numbered from zero in the plan that produces the shared CTE.
    pub fn with_reference(&self, reference: usize) -> Self {
        Self {
            reference,
            ..self.clone()
        }
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    /// Returns the number of the reference among the references of the shared CTE.
    pub fn reference(&self) -> usize {
        self.reference
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
        let Some(tee) = output.get(partition) else {
            return internal_err!(
                "{}: partition {partition} of the shared CTE is not produced",
                self.name()
            );
        };
        let reader: TeeReader = tee.reader(self.reference)?;
        let projection = self.projection.clone();
        let schema = self.schema();
        let stream = reader.map(move |batch| {
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
        });
        Ok(Box::pin(cooperative(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        ))))
    }
}

/// Returns the plan that produces the given shared CTEs for their references in the plan
/// and in the CTEs, where a CTE can refer to the CTEs before it. A CTE that is left with
/// fewer than two references is evaluated in place of its reference. The references of
/// each CTE are numbered (see [`SharedCteScanExec::with_reference`]).
pub fn share_ctes(
    plan: Arc<dyn ExecutionPlan>,
    ctes: Vec<(Arc<SharedCte>, Arc<dyn ExecutionPlan>)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plan = plan;
    let mut ctes = ctes;
    loop {
        let mut references: HashMap<usize, usize> = HashMap::new();
        for plan in std::iter::once(&plan).chain(ctes.iter().map(|(_, plan)| plan)) {
            plan.apply(|plan| {
                if let Some(scan) = plan.downcast_ref::<SharedCteScanExec>() {
                    *references.entry(scan.cte().id()).or_default() += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        let count = |cte: &SharedCte| references.get(&cte.id()).copied().unwrap_or(0);
        let Some(position) = ctes.iter().position(|(cte, _)| count(cte) < 2) else {
            break;
        };
        let (single, body) = ctes.remove(position);
        if count(&single) == 0 {
            continue;
        }
        let inline = |plan: Arc<dyn ExecutionPlan>| {
            plan.transform_up(|plan| match plan.downcast_ref::<SharedCteScanExec>() {
                Some(scan) if scan.cte().id() == single.id() => Ok(Transformed::yes(project(
                    Arc::clone(&body),
                    scan.projection(),
                )?)),
                _ => Ok(Transformed::no(plan)),
            })
            .data()
        };
        plan = inline(plan)?;
        for (_, cte) in &mut ctes {
            *cte = inline(Arc::clone(cte))?;
        }
    }
    if ctes.is_empty() {
        return Ok(plan);
    }
    // Each reference reads the output of its shared CTE as one of its numbered readers.
    let mut references: HashMap<usize, usize> = HashMap::new();
    let mut number = |plan: Arc<dyn ExecutionPlan>| {
        plan.transform_up(|plan| match plan.downcast_ref::<SharedCteScanExec>() {
            Some(scan) => {
                let count = references.entry(scan.cte().id()).or_default();
                let scan = scan.with_reference(*count);
                *count += 1;
                Ok(Transformed::yes(Arc::new(scan) as Arc<dyn ExecutionPlan>))
            }
            None => Ok(Transformed::no(plan)),
        })
        .data()
    };
    let plan = number(plan)?;
    let ctes = ctes
        .into_iter()
        .map(|(cte, body)| Ok((cte, number(body)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(SharedCteExec::new(plan, ctes)))
}

/// Returns the plan of a shared CTE for a reference that reads the given columns.
fn project(
    plan: Arc<dyn ExecutionPlan>,
    projection: Option<&[usize]>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(projection) = projection else {
        return Ok(plan);
    };
    let schema = plan.schema();
    let expressions = projection
        .iter()
        .map(|&index| {
            let name = schema.field(index).name();
            ProjectionExpr::new(Arc::new(Column::new(name, index)), name.clone())
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(expressions, plan)?))
}

/// Returns the statistics of a reference that reads the given columns of a shared CTE
/// with the given statistics, as those of a projection of the plan of the CTE.
fn project_statistics(
    statistics: &Statistics,
    projection: Option<&[usize]>,
    schema: &SchemaRef,
) -> Statistics {
    let columns = statistics.column_statistics.len();
    let projected = match projection {
        None => statistics.clone(),
        Some(projection) if projection.iter().all(|&index| index < columns) => {
            let mut projected = statistics.clone().project(Some(&projection));
            projected.calculate_total_byte_size(schema);
            projected
        }
        Some(_) => Statistics::new_unknown(schema),
    };
    if projected.column_statistics.len() == schema.fields().len() {
        projected
    } else {
        let mut output = Statistics::new_unknown(schema);
        output.num_rows = statistics.num_rows;
        output
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
        let scan = |column: usize, reference: usize| -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(
                SharedCteScanExec::new(
                    Arc::clone(&cte),
                    Arc::new(schema.project(&[column])?),
                    &statistics,
                    Some(vec![column]),
                )
                .with_reference(reference),
            ))
        };
        let input = UnionExec::try_new(vec![scan(1, 0)?, scan(0, 1)?])?;
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
        let batches = collect(
            Arc::clone(&shared) as Arc<dyn ExecutionPlan>,
            Arc::clone(&context),
        )
        .await?;
        // The output is released once every partition of every reference is read.
        assert!(cte.state()?.output.is_none());
        // The plan is executed again, and produces the shared CTE again.
        let again = collect(Arc::clone(&shared) as Arc<dyn ExecutionPlan>, context).await?;
        assert_eq!(
            again.iter().map(|x| x.num_rows()).sum::<usize>(),
            batches.iter().map(|x| x.num_rows()).sum::<usize>()
        );
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
        let (values, _) = read(Some(64 * 1024), true).await?;
        assert_eq!(values, expected);
        // The output stays in memory when temporary files are disabled.
        let (values, spilled) = read(Some(64 * 1024), false).await?;
        assert_eq!(values, expected);
        assert_eq!(spilled, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_in_the_build_side_of_a_nested_loop_join_is_read_again() -> Result<()> {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_plan::common::collect as common_collect;

        let (schema, producer) = plan(1)?;
        let statistics = Statistics::new_unknown(&schema);
        let (_, right) = plan(1)?;
        for nested_loop_join in [true, false] {
            let cte = Arc::new(SharedCte::new(
                0,
                "c".to_string(),
                Partitioning::UnknownPartitioning(1),
            ));
            let scan = |reference: usize| -> Arc<dyn ExecutionPlan> {
                Arc::new(
                    SharedCteScanExec::new(
                        Arc::clone(&cte),
                        Arc::clone(&schema),
                        &statistics,
                        None,
                    )
                    .with_reference(reference),
                )
            };
            let build = scan(0);
            let input: Arc<dyn ExecutionPlan> = if nested_loop_join {
                let join = NestedLoopJoinExec::try_new(
                    Arc::clone(&build),
                    Arc::clone(&right),
                    None,
                    &JoinType::LeftSemi,
                    None,
                )?;
                UnionExec::try_new(vec![Arc::new(join), scan(1)])?
            } else {
                UnionExec::try_new(vec![Arc::clone(&build), scan(1)])?
            };
            let shared = Arc::new(SharedCteExec::new(
                Arc::clone(&input),
                vec![(Arc::clone(&cte), Arc::clone(&producer))],
            ));
            let runtime = RuntimeEnvBuilder::new().with_memory_limit(64 * 1024 * 1024, 1.0);
            let context = Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
            // The other reference reads every row before the build side is read.
            let other = input.output_partitioning().partition_count() - 1;
            let rows = common_collect(shared.execute(other, Arc::clone(&context))?).await?;
            assert_eq!(rows.iter().map(|x| x.num_rows()).sum::<usize>(), 4096);
            // The build side is read, and read again from the beginning, as the join does
            // after the memory pool refuses to keep it in memory.
            let first = common_collect(build.execute(0, Arc::clone(&context))?).await?;
            assert_eq!(first.iter().map(|x| x.num_rows()).sum::<usize>(), 4096);
            let again = build.execute(0, Arc::clone(&context));
            if nested_loop_join {
                let again = common_collect(again?).await?;
                assert_eq!(again.iter().map(|x| x.num_rows()).sum::<usize>(), 4096);
            } else {
                let failed = match again {
                    Ok(again) => common_collect(again).await.is_err(),
                    Err(_) => true,
                };
                assert!(failed);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_reference_yields_when_its_budget_is_spent() -> Result<()> {
        use std::task::Poll;

        use datafusion_common::DataFusionError;

        use datafusion::physical_plan::common::collect as common_collect;

        const BATCHES: usize = 512;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..8))],
        )?;
        let producer =
            MemorySourceConfig::try_new_exec(&[vec![batch; BATCHES]], Arc::clone(&schema), None)?;
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(1),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan = |reference: usize| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                SharedCteScanExec::new(Arc::clone(&cte), Arc::clone(&schema), &statistics, None)
                    .with_reference(reference),
            )
        };
        let input = UnionExec::try_new(vec![scan(0), scan(1)])?;
        let shared = Arc::new(SharedCteExec::new(
            input,
            vec![(Arc::clone(&cte), producer)],
        ));
        let context = Arc::new(TaskContext::default());
        // The second reference reads every batch, which is kept for the first one.
        let rows = common_collect(shared.execute(1, Arc::clone(&context))?).await?;
        assert_eq!(rows.len(), BATCHES);
        // The first reference reads kept batches, which are always ready, and yields to
        // other tasks when the scheduling budget of its task is spent.
        let mut stream = shared.execute(0, Arc::clone(&context))?;
        let mut read = 0;
        let ready = std::future::poll_fn(|cx| {
            loop {
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(batch)) => {
                        batch?;
                        read += 1;
                    }
                    Poll::Ready(None) => return Poll::Ready(Ok::<_, DataFusionError>(false)),
                    Poll::Pending => return Poll::Ready(Ok(true)),
                }
            }
        })
        .await?;
        assert!(ready, "read {read} batches without yielding");
        assert!(read < BATCHES);
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
        let scan = |reference: usize| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                SharedCteScanExec::new(Arc::clone(&cte), Arc::clone(&schema), &statistics, None)
                    .with_reference(reference),
            )
        };
        let input = UnionExec::try_new(vec![scan(0), scan(1)])?;
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
        let scan = |reference: usize| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                SharedCteScanExec::new(Arc::clone(&cte), Arc::clone(&schema), &statistics, None)
                    .with_reference(reference),
            )
        };
        let input = UnionExec::try_new(vec![scan(0), scan(1)])?;
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
    async fn test_shared_cte_copies_batches_larger_than_the_batch_size() -> Result<()> {
        use datafusion::arrow::array::StringArray;
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
        use datafusion::prelude::SessionConfig;

        const BATCH_SIZE: usize = 1024;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let rows = 4 * BATCH_SIZE + 1;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|x| format!("value {x}")),
                )),
            ],
        )?;
        // A source that returns the batch as it is, as a window does with a whole partition.
        #[derive(Debug)]
        struct OneBatch(RecordBatch);
        impl PartitionStream for OneBatch {
            fn schema(&self) -> &SchemaRef {
                self.0.schema_ref()
            }

            fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
                let stream = futures::stream::iter(vec![Ok(self.0.clone())]);
                Box::pin(RecordBatchStreamAdapter::new(self.0.schema(), stream))
            }
        }
        let producer: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(OneBatch(batch.clone()))],
            None,
            vec![],
            false,
            None,
        )?);
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(1),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan = |reference: usize| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                SharedCteScanExec::new(Arc::clone(&cte), Arc::clone(&schema), &statistics, None)
                    .with_reference(reference),
            )
        };
        let input = UnionExec::try_new(vec![scan(0), scan(1)])?;
        let shared = Arc::new(SharedCteExec::new(
            input,
            vec![(Arc::clone(&cte), producer)],
        ));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(BATCH_SIZE)),
        );
        for partition in 0..2 {
            let mut stream = shared.execute(partition, Arc::clone(&context))?;
            let mut batches = vec![];
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            // Each batch that a reference reads is a copy of a part of the batch, which
            // does not keep the memory of the whole batch.
            assert!(batches.iter().all(|x| x.num_rows() <= BATCH_SIZE));
            let size = batches
                .iter()
                .map(|x| x.get_array_memory_size())
                .sum::<usize>();
            assert!(
                size * 2 <= batch.get_array_memory_size() * 3,
                "{size} {}",
                batch.get_array_memory_size()
            );
            let read = datafusion::arrow::compute::concat_batches(&schema, &batches)?;
            assert_eq!(read, batch);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_spills_under_a_bounded_pool() -> Result<()> {
        // The second reference reads after the first one, so the CTE is kept for it. Under
        // a memory limit, the kept batches are written to disk, even if the pool has room
        // for them, so that they take no memory that other operators may need.
        for limit in [1024 * 1024 * 1024, 16 * 1024 * 1024] {
            let runtime = RuntimeEnvBuilder::new().with_memory_limit(limit, 1.0);
            let (rows, spilled) = read_large([16, 8, 8], runtime).await?;
            assert_eq!(rows, 64);
            assert!(spilled >= 16, "spilled {spilled}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_stays_in_memory_without_temporary_files() -> Result<()> {
        use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};

        // The CTE is kept outside the pool when it cannot be spilled, which is not an error.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(4 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            );
        let (rows, spilled) = read_large([16, 8, 8], runtime).await?;
        assert_eq!((rows, spilled), (64, 0));
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_cte_spill_failure_is_an_error() -> Result<()> {
        // The temporary files of the CTE exceed the size that they are allowed to have.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(4 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(1024 * 1024),
            );
        let result = read_large([16, 0, 0], runtime).await;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_shared_cte_reference_statistics() -> Result<()> {
        use datafusion::common::stats::Precision;
        use datafusion::common::{ColumnStatistics, ScalarValue};

        let (schema, _) = plan(1)?;
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            Partitioning::UnknownPartitioning(1),
        ));
        let column = |min: i64, max: i64| ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
            null_count: Precision::Exact(0),
            distinct_count: Precision::Inexact(10),
            ..ColumnStatistics::new_unknown()
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(160),
            column_statistics: vec![column(0, 9), column(-9, 0)],
        };
        let scan = |projection: Option<Vec<usize>>| -> Result<Arc<Statistics>> {
            let schema = match &projection {
                Some(projection) => Arc::new(schema.project(projection)?),
                None => Arc::clone(&schema),
            };
            let scan = SharedCteScanExec::new(Arc::clone(&cte), schema, &statistics, projection);
            scan.statistics_from_inputs(&[], &StatisticsArgs::new())
        };
        // A reference has the statistics of the CTE, projected to the columns it reads.
        assert_eq!(*scan(None)?, statistics);
        let projected = scan(Some(vec![1]))?;
        assert_eq!(projected.num_rows, Precision::Exact(10));
        assert_eq!(projected.total_byte_size, Precision::Exact(80));
        assert_eq!(projected.column_statistics, vec![column(-9, 0)]);
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
