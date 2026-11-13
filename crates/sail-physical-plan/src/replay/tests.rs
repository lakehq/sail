use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::internal_datafusion_err;
use futures::{Stream, TryStreamExt};
use sail_common_datafusion::replay::ReplayEpoch;
use uuid::Uuid;

use super::*;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Complete(usize),
    FailAfterOne,
    PendingAfterOne,
    Stateful,
    Counter,
}

#[derive(Debug)]
struct ProbeExec {
    properties: Arc<PlanProperties>,
    executions: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    resets: Arc<AtomicUsize>,
    cursor: Arc<AtomicUsize>,
    mode: Mode,
}

impl ProbeExec {
    fn new(partitions: usize, mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::new(Schema::new(vec![Field::new(
                    "v",
                    DataType::Int64,
                    false,
                )]))),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            executions: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            resets: Arc::new(AtomicUsize::new(0)),
            cursor: Arc::new(AtomicUsize::new(0)),
            mode,
        })
    }
}

impl DisplayAs for ProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ProbeExec")
    }
}
impl ExecutionPlan for ProbeExec {
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
        children.zero()?;
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        if !matches!(self.mode, Mode::Stateful) {
            return Ok(self);
        }
        self.resets.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            executions: self.executions.clone(),
            drops: self.drops.clone(),
            resets: self.resets.clone(),
            cursor: Arc::new(AtomicUsize::new(0)),
            mode: self.mode,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let invocation = self.executions.fetch_add(1, Ordering::SeqCst);
        // In a multipartition failure test, a successful partition can finish
        // before the last partition fails. Neither may become reader-visible.
        let mode = if matches!(self.mode, Mode::Stateful) {
            // Like a lazy generator, the cursor belongs to the plan, not the
            // stream. Cloning the plan cannot make an exhausted source fresh.
            Mode::Complete(usize::from(self.cursor.fetch_add(1, Ordering::SeqCst) == 0))
        } else if partition == 0 && self.properties().output_partitioning().partition_count() > 1 {
            Mode::Complete(2)
        } else {
            self.mode
        };
        Ok(Box::pin(ProbeStream {
            schema: self.schema(),
            drops: self.drops.clone(),
            mode,
            emitted: 0,
            invocation,
        }))
    }
}

struct ProbeStream {
    schema: SchemaRef,
    drops: Arc<AtomicUsize>,
    mode: Mode,
    emitted: usize,
    invocation: usize,
}
impl Stream for ProbeStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.mode {
            Mode::Complete(n) if self.emitted >= n => return Poll::Ready(None),
            Mode::Counter if self.emitted >= 1 => return Poll::Ready(None),
            Mode::FailAfterOne if self.emitted > 0 => {
                return Poll::Ready(Some(exec_err!("injected source failure")));
            }
            Mode::PendingAfterOne if self.emitted > 0 => return Poll::Pending,
            _ => {}
        }
        let value = if matches!(self.mode, Mode::Counter) {
            (self.invocation + 1) as i64
        } else {
            (self.invocation * 1000 + self.emitted) as i64
        };
        self.emitted += 1;
        Poll::Ready(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .map_err(Into::into),
        ))
    }
}
impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for ProbeStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn context() -> Result<Arc<TaskContext>> {
    Ok(Arc::new(
        TaskContext::default().with_runtime(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024)))
                .build_arc()?,
        ),
    ))
}
fn id(ordinal: u32) -> ReplayId {
    ReplayId {
        scope: Uuid::new_v4(),
        ordinal,
    }
}
fn registry(epoch: u64) -> Arc<ReplayRegistry> {
    Arc::new(ReplayRegistry::new(
        ReplayEpoch {
            session_id: "test-session".into(),
            job_id: epoch,
        },
        Default::default(),
    ))
}
fn replay(id: ReplayId, source: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(ReplayExec::try_new(id, source)?))
}
async fn values(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Result<Vec<i64>> {
    let batches = execute_stream(plan, context)?
        .try_collect::<Vec<_>>()
        .await?;
    let mut values = vec![];
    for batch in batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal_datafusion_err!("expected i64"))?;
        values.extend_from_slice(array.values());
    }
    values.sort_unstable();
    Ok(values)
}
fn assert_clean(context: &TaskContext) {
    assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
}

#[tokio::test]
async fn replay_local_stateful_leaf_is_reset_for_each_epoch_only() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Stateful);
    // Exhaust the original prepared source, proving clone alone is insufficient.
    assert_eq!(values(source.clone(), context.clone()).await?, vec![0]);
    assert!(values(source.clone(), context.clone()).await?.is_empty());
    let prepared = replay(id(0), source.clone())?;
    for epoch in 1..=2 {
        let binding = LocalReplayBinding::try_new(prepared.clone(), registry(epoch), &context)?;
        binding.prepare(context.clone()).await?;
        let expected = vec![(epoch as i64 + 1) * 1000];
        assert_eq!(values(binding.plan(), context.clone()).await?, expected);
        assert_eq!(
            values(reset_plan_states(binding.plan())?, context.clone()).await?,
            expected
        );
        assert_eq!(source.resets.load(Ordering::SeqCst), epoch as usize);
        drop(binding);
        assert_clean(&context);
    }
    assert_eq!(source.executions.load(Ordering::SeqCst), 4);
    assert_eq!(source.cursor.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn replay_local_prepared_hash_join_resets_consumers_between_epochs() -> Result<()> {
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

    for abandon_first in [false, true] {
        let context = context()?;
        let source = ProbeExec::new(1, Mode::Counter);
        let right = RecordBatch::try_new(
            source.schema(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )?;
        let right = MemorySourceConfig::try_new_exec(&[vec![right]], source.schema(), None)?;
        let key = col("v", &source.schema())?;
        let prepared: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            replay(id(0), source.clone())?,
            right,
            vec![(key.clone(), key)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        for epoch in 1..=2 {
            let binding = LocalReplayBinding::try_new(prepared.clone(), registry(epoch), &context)?;
            binding.prepare(context.clone()).await?;
            let root = execute_stream(binding.plan(), context.clone())?;
            let root = binding.finish(root);
            if epoch == 1 && abandon_first {
                drop(root);
            } else {
                let batches = root.try_collect::<Vec<_>>().await?;
                let actual = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .into_iter()
                            .flat_map(|x| x.values().iter().copied())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, vec![epoch as i64]);
            }
            assert_clean(&context);
        }
        assert_eq!(source.executions.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[tokio::test]
async fn replay_local_corrupt_dependency_retires_epoch_without_recompute() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(2));
    let source_id = id(0);
    let intent_id = id(1);
    let registry = registry(1);
    let first = replay(source_id, source.clone())?;
    let mut binding =
        LocalReplayBinding::try_new(replay(intent_id, first)?, registry.clone(), &context)?;
    // Pause at the actual dependency boundary: source sealed, intent not started.
    let intent = binding
        .producers
        .pop()
        .ok_or_else(|| internal_datafusion_err!("missing intent producer"))?;
    binding.prepare(context.clone()).await?;
    let mut corrupted_files = 0;
    for directory in context.runtime_env().disk_manager.temp_dir_paths() {
        for entry in std::fs::read_dir(directory)? {
            std::fs::OpenOptions::new()
                .write(true)
                .open(entry?.path())?
                .set_len(0)?;
            corrupted_files += 1;
        }
    }
    assert_eq!(corrupted_files, 1);
    binding.producers = vec![intent];
    assert!(binding.prepare(context.clone()).await.is_err());
    assert!(
        registry
            .open_reader(intent_id, 0, &context.runtime_env())
            .is_err()
    );
    assert!(
        registry
            .open_reader(source_id, 0, &context.runtime_env())
            .is_err()
    );
    assert!(binding.prepare(context.clone()).await.is_err());
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    drop(binding);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_local_named_readers_and_reset_execute_producer_once() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(3));
    let named = replay(id(0), source.clone())?;
    assert!(named.execute(0, context.clone()).is_err());
    let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![named.clone(), named])?;
    let binding = LocalReplayBinding::try_new(plan, registry(1), &context)?;
    binding.prepare(context.clone()).await?;
    let expected = vec![0, 0, 1, 1, 2, 2];
    assert_eq!(values(binding.plan(), context.clone()).await?, expected);
    let reset = reset_plan_states(binding.plan())?;
    assert_eq!(values(reset, context.clone()).await?, expected);
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    assert_eq!(source.drops.load(Ordering::SeqCst), 1);
    drop(binding);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_local_same_prepared_plan_has_independent_epochs() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(1));
    let plan = replay(id(0), source.clone())?;
    for epoch in 1..=2 {
        let binding = LocalReplayBinding::try_new(plan.clone(), registry(epoch), &context)?;
        binding.prepare(context.clone()).await?;
        assert_eq!(
            values(binding.plan(), context.clone()).await?,
            vec![(epoch as i64 - 1) * 1000]
        );
        let stream = execute_stream(binding.plan(), context.clone())?;
        binding.finish(stream).try_collect::<Vec<_>>().await?;
        assert_clean(&context);
    }
    assert_eq!(source.executions.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn replay_local_scalar_scope_runs_before_intent_and_resets_between_epochs() -> Result<()> {
    use datafusion::logical_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion::physical_plan::projection::ProjectionExec;
    let context = context()?;
    let scalar_source = ProbeExec::new(1, Mode::Complete(1));
    let scalar_input = replay(id(0), scalar_source.clone())?;
    let results = ScalarSubqueryResults::new(1);
    let expression: Arc<dyn PhysicalExpr> = Arc::new(ScalarSubqueryExpr::new(
        DataType::Int64,
        true,
        SubqueryIndex::new(0),
        results.clone(),
    ));
    let input = ProbeExec::new(1, Mode::Complete(2));
    let projected = Arc::new(ProjectionExec::try_new(
        vec![(expression, "v".to_string())],
        input,
    )?);
    let intent = replay(id(1), projected)?;
    let consumers = UnionExec::try_new(vec![intent.clone(), intent])?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
        consumers,
        vec![ScalarSubqueryLink {
            plan: scalar_input,
            index: SubqueryIndex::new(0),
        }],
        results,
    ));
    for epoch in 1..=2 {
        let binding = LocalReplayBinding::try_new(plan.clone(), registry(epoch), &context)?;
        binding.prepare(context.clone()).await?;
        assert_eq!(
            values(binding.plan(), context.clone()).await?,
            vec![(epoch as i64 - 1) * 1000; 4]
        );
        assert_eq!(
            values(reset_plan_states(binding.plan())?, context.clone()).await?,
            vec![(epoch as i64 - 1) * 1000; 4]
        );
        assert_eq!(
            scalar_source.executions.load(Ordering::SeqCst),
            epoch as usize
        );
        drop(binding);
        assert_clean(&context);
    }
    Ok(())
}

#[tokio::test]
async fn replay_local_dependency_dag_is_built_once_in_order() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(2));
    let first = replay(id(0), source.clone())?;
    let second = replay(id(1), first.clone())?;
    let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![second.clone(), first, second])?;
    let binding = LocalReplayBinding::try_new(plan, registry(1), &context)?;
    binding.prepare(context.clone()).await?;
    assert_eq!(
        values(binding.plan(), context.clone()).await?,
        vec![0, 0, 0, 1, 1, 1]
    );
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    drop(binding);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_local_source_error_never_publishes_partial_partitions() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(2, Mode::FailAfterOne);
    let id = id(0);
    let registry = registry(1);
    let binding =
        LocalReplayBinding::try_new(replay(id, source.clone())?, registry.clone(), &context)?;
    assert!(binding.prepare(context.clone()).await.is_err());
    assert!(registry.open_reader(id, 0, &context.runtime_env()).is_err());
    assert!(binding.prepare(context.clone()).await.is_err());
    assert_eq!(source.executions.load(Ordering::SeqCst), 2);
    assert_eq!(source.drops.load(Ordering::SeqCst), 2);
    drop(binding);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_local_cancel_and_drop_during_build_remove_files() -> Result<()> {
    for cancel in [true, false] {
        let context = context()?;
        let source = ProbeExec::new(1, Mode::PendingAfterOne);
        let registry = registry(1);
        let binding = LocalReplayBinding::try_new(
            replay(id(0), source.clone())?,
            registry.clone(),
            &context,
        )?;
        let mut preparation = Box::pin(binding.prepare(context.clone()));
        assert!(futures::poll!(&mut preparation).is_pending());
        assert!(context.runtime_env().disk_manager.used_disk_space() > 0);
        if cancel {
            registry.cancellation().cancel();
            assert!(preparation.as_mut().await.is_err());
        }
        drop(preparation);
        drop(binding);
        assert_eq!(source.drops.load(Ordering::SeqCst), 1);
        assert_clean(&context);
    }
    Ok(())
}

#[tokio::test]
async fn replay_local_abandoned_root_retires_readers_not_producer() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(3));
    let registry = registry(1);
    let id = id(0);
    let binding =
        LocalReplayBinding::try_new(replay(id, source.clone())?, registry.clone(), &context)?;
    binding.prepare(context.clone()).await?;
    let retained_plan = binding.plan();
    let stream = execute_stream(binding.plan(), context.clone())?;
    let mut root = binding.finish(stream);
    assert!(root.next().await.transpose()?.is_some());
    drop(root);
    assert!(retained_plan.execute(0, context.clone()).is_err());
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_local_repartition_drains_all_outputs_without_deadlock() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(200));
    let partitioned: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        source.clone(),
        Partitioning::RoundRobinBatch(2),
    )?);
    let binding = LocalReplayBinding::try_new(replay(id(0), partitioned)?, registry(1), &context)?;
    tokio::time::timeout(Duration::from_secs(10), binding.prepare(context.clone()))
        .await
        .map_err(|e| internal_datafusion_err!("replay deadlocked: {e}"))??;
    assert_eq!(
        values(binding.plan(), context.clone()).await?,
        (0..200).collect::<Vec<i64>>()
    );
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    drop(binding);
    assert_clean(&context);
    Ok(())
}

#[test]
fn replay_local_conflicting_partitioning_fails_before_source_execution() -> Result<()> {
    let context = context()?;
    let id = id(0);
    let first = ProbeExec::new(1, Mode::Complete(1));
    let second = ProbeExec::new(2, Mode::Complete(1));
    let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
        replay(id, first.clone())?,
        replay(id, second.clone())?,
    ])?;
    assert!(LocalReplayBinding::try_new(plan, registry(1), &context).is_err());
    assert_eq!(first.executions.load(Ordering::SeqCst), 0);
    assert_eq!(second.executions.load(Ordering::SeqCst), 0);
    assert_clean(&context);
    Ok(())
}

#[test]
fn replay_local_rejects_cyclic_names() -> Result<()> {
    let context = context()?;
    let source = ProbeExec::new(1, Mode::Complete(1));
    let id = id(0);
    let plan = replay(id, replay(id, source.clone())?)?;
    assert!(LocalReplayBinding::try_new(plan, registry(1), &context).is_err());
    assert_eq!(source.executions.load(Ordering::SeqCst), 0);
    assert_clean(&context);
    Ok(())
}
