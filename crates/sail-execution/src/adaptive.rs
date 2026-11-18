// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::arrow::array::{
    Array, MutableArrayData, RecordBatch, RecordBatchOptions, make_array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PartitioningSatisfaction;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, HashJoinExecBuilder, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{
    ChildSatisfactionOptions, Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    displayable, replace_children_if_necessary,
};
use futures::StreamExt;
use futures::future::{Either, select};
use log::{info, warn};
use sail_common_datafusion::OPTIMIZER_SEMI_JOIN_REDUCTION_KEY_PREFIX;

use crate::id::JobId;
use crate::job_graph::{ExternalStageInput, HandoffOutput, OutputDistribution};
use crate::plan::HandoffExec;

pub(crate) struct AdaptiveJoinOptions {
    /// Build sides below this many bytes are broadcast, as in DataFusion's `JoinSelection`.
    pub broadcast_threshold: usize,
    pub target_partitions: usize,
    pub config: Arc<ConfigOptions>,
}

impl AdaptiveJoinOptions {
    pub fn new(config: Arc<ConfigOptions>) -> Self {
        Self {
            broadcast_threshold: config.optimizer.hash_join_single_partition_threshold,
            target_partitions: config.execution.target_partitions,
            config,
        }
    }
}

/// Runs join inputs as jobs of their own for adaptive join selection.
#[tonic::async_trait]
pub(crate) trait SideRunner: Send + Sync {
    /// Runs `plan` as its own job. With `handoff`, the job's final stage is written in that
    /// distribution so that a later job can read it through the returned input, once the
    /// job is handed off.
    async fn run(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        handoff: Option<HandoffOutput>,
    ) -> Result<(SendableRecordBatchStream, Option<ExternalStageInput>)>;

    /// Keeps a job running after its stream is dropped, for a later job to read.
    async fn hand_off(&self, job_id: JobId) -> bool;

    /// Cleans up a handed-off job.
    fn release(&self, job_id: JobId);
}

/// The handed-off jobs of a query. A hand-off output has one reader, so each is read
/// by at most one job.
#[derive(Default)]
pub(crate) struct Handoffs {
    state: Mutex<HandoffState>,
}

#[derive(Default)]
struct HandoffState {
    jobs: Vec<JobId>,
    used: HashSet<JobId>,
}

impl Handoffs {
    fn state(&self) -> MutexGuard<'_, HandoffState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn add(&self, job_id: JobId) {
        self.state().jobs.push(job_id);
    }

    /// Releases the job of a hand-off that the plan does not use.
    fn discard(&self, materialized: &Materialized, runner: &dyn SideRunner) {
        if let Materialized::Large {
            handoff: Some((job_id, _)),
            ..
        } = materialized
        {
            self.state().jobs.retain(|x| x != job_id);
            runner.release(*job_id);
        }
    }

    /// Takes the jobs to release once the query is done with them.
    pub fn take(&self) -> Vec<JobId> {
        std::mem::take(&mut self.state().jobs)
    }

    /// Lets the job running `plan` read each hand-off in it, unless another job has read
    /// it already; the subtree the hand-off replaced is then computed again.
    pub fn resolve(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut state = self.state();
        Ok(plan
            .transform_down(|node| match node.downcast_ref::<HandoffExec>() {
                Some(handoff) if !state.used.insert(handoff.source().job_id) => {
                    Ok(Transformed::yes(Arc::clone(handoff.original())))
                }
                _ => Ok(Transformed::no(node)),
            })?
            .data)
    }
}

/// Chooses between broadcast and partitioned hash joins from the actual size of build sides.
///
/// The deepest candidate join whose build size is only estimated has its build side run
/// first through `runner`:
/// - For a partitioned join, a result below the broadcast threshold replaces the build side,
///   and the join becomes a broadcast join without the probe-side shuffle.
/// - A speculative semijoin reduction is already a broadcast join. A result below the
///   threshold replaces its build side.
/// - A semijoin reduction whose keys reach the threshold is removed.
/// - A partitioned join whose build side is too large can broadcast its probe side
///   instead, with the sides swapped. Both sides run at the same time, so that a large
///   build side does not delay the probe side.
///
/// A side that reaches the threshold is not run again: its job is handed off, and a join
/// that stays partitioned reads the rest of its output. Broadcast joins compute their
/// probe side again instead, in the task of the join, where the join's dynamic filter
/// prunes it.
pub(crate) async fn select_joins(
    mut plan: Arc<dyn ExecutionPlan>,
    options: &AdaptiveJoinOptions,
    runner: &dyn SideRunner,
    handoffs: &Handoffs,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Joins that stay partitioned are kept as the same node, so their address identifies them.
    let mut settled = HashSet::new();
    while let Some((node, candidate)) = find_candidate(&plan, &settled)? {
        settled.insert(address(&node));
        let Some(join) = node.downcast_ref::<HashJoinExec>() else {
            continue;
        };
        let description = displayable(join).one_line().to_string();
        let description = description.trim_end();
        let estimate = statistics(join.left().as_ref())?;
        // Hand-offs the replacement reads, released if the replacement is not used.
        let mut used = vec![];
        let (replacement, decision) = match candidate {
            Candidate::Partitioned { build, probe } => {
                let (build, probe) =
                    materialize_sides(join, build, probe, options, runner, handoffs).await?;
                match (build, probe) {
                    (Materialized::Small(build), probe) => {
                        handoffs.discard(&probe, runner);
                        let actual = statistics(build.as_ref())?;
                        let decision = format!(
                            "broadcast, build side rows {:?}, bytes {:?}",
                            actual.num_rows, actual.total_byte_size
                        );
                        (broadcast_join(join, build)?, decision)
                    }
                    // A partitioned reduction would shuffle its input to save nothing.
                    (build, probe) if is_reduction(join) => {
                        handoffs.discard(&build, runner);
                        used.extend(probe.handoff_job());
                        let input = probe.handoff().unwrap_or_else(|| Arc::clone(join.right()));
                        (
                            remove_reduction(join, input)?,
                            format!("removed partitioned reduction, keys {build}"),
                        )
                    }
                    (build, Materialized::Small(probe)) => {
                        handoffs.discard(&build, runner);
                        let actual = statistics(probe.as_ref())?;
                        let decision = format!(
                            "broadcast with sides swapped ({build}), probe side rows {:?}, bytes {:?}",
                            actual.num_rows, actual.total_byte_size
                        );
                        (swapped_broadcast_join(join, probe)?, decision)
                    }
                    (build, probe) => {
                        if build.handoff().is_none() && probe.handoff().is_none() {
                            info!(
                                "adaptive join selection: partitioned ({build}; probe side {probe}) (estimated build rows {:?}, bytes {:?}): {description}",
                                estimate.num_rows, estimate.total_byte_size,
                            );
                            continue;
                        }
                        used.extend(build.handoff_job());
                        used.extend(probe.handoff_job());
                        let children = vec![
                            build.handoff().unwrap_or_else(|| Arc::clone(join.left())),
                            probe.handoff().unwrap_or_else(|| Arc::clone(join.right())),
                        ];
                        let replacement =
                            join.builder().with_new_children(children)?.build_exec()?;
                        settled.insert(address(&replacement));
                        (
                            replacement,
                            format!("partitioned, handed off ({build}; probe side {probe})"),
                        )
                    }
                }
            }
            Candidate::Reduction => {
                let (input, distinct) = split_distinct_keys(join.left());
                match materialize(&input, options, runner, handoffs).await? {
                    Materialized::Small(build) => {
                        let actual = statistics(build.as_ref())?;
                        let decision = format!(
                            "kept reduction, key input rows {:?}, bytes {:?}",
                            actual.num_rows, actual.total_byte_size
                        );
                        let build = match distinct {
                            Some(distinct) => {
                                let build = if build.output_partitioning().partition_count() == 1 {
                                    build
                                } else {
                                    Arc::new(CoalescePartitionsExec::new(build))
                                };
                                replace_children_if_necessary(distinct, vec![build])?
                            }
                            None => build,
                        };
                        let join = join
                            .builder()
                            .with_new_children(vec![build, Arc::clone(join.right())])?
                            .build_exec()?;
                        // The distinct keys have estimated statistics again.
                        settled.insert(address(&join));
                        (join, decision)
                    }
                    materialized => {
                        handoffs.discard(&materialized, runner);
                        (
                            remove_reduction(join, Arc::clone(join.right()))?,
                            format!("removed reduction, keys {materialized}"),
                        )
                    }
                }
            }
        };
        let candidate = replace_node(&plan, &node, &replacement, options)?.unwrap_or(replacement);
        if let Err(error) = SanityCheckPlan::new().optimize(Arc::clone(&candidate), &options.config)
        {
            warn!(
                "adaptive join selection: keeping the join unchanged instead of {decision}: {error}"
            );
            for job_id in used {
                handoffs.discard(
                    &Materialized::Large {
                        rows: 0,
                        bytes: 0,
                        handoff: Some((job_id, Arc::clone(&node))),
                    },
                    runner,
                );
            }
            continue;
        }
        info!(
            "adaptive join selection: {decision} (estimated rows {:?}, bytes {:?}): {description}",
            estimate.num_rows, estimate.total_byte_size,
        );
        plan = candidate;
    }
    Ok(plan)
}

fn statistics(plan: &dyn ExecutionPlan) -> Result<Arc<Statistics>> {
    StatisticsContext::new().compute(plan, &StatisticsArgs::new())
}

fn address(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan).cast::<()>() as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Candidate {
    /// A partitioned join that may become a broadcast join, with the sides that may be
    /// broadcast: the build side as is, or the probe side with the sides swapped.
    Partitioned { build: bool, probe: bool },
    /// A speculative semijoin reduction, broadcast whatever its estimated size.
    Reduction,
}

/// Splits the distinct keys of a reduction from their input, which is what gets sized: the
/// aggregate emits nothing until it has read all of its input, in one task, and the keys
/// are never more than their input.
fn split_distinct_keys(
    keys: &Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, Option<Arc<dyn ExecutionPlan>>) {
    let Some(aggregate) = keys.downcast_ref::<AggregateExec>() else {
        return (Arc::clone(keys), None);
    };
    if *aggregate.mode() != AggregateMode::Single || !aggregate.aggr_expr().is_empty() {
        return (Arc::clone(keys), None);
    }
    let mut input = aggregate.input();
    while let Some(coalesce) = input.downcast_ref::<CoalescePartitionsExec>() {
        input = coalesce.input();
    }
    (Arc::clone(input), Some(Arc::clone(keys)))
}

/// Whether `join` is a semijoin reduction inserted by the physical optimizer.
fn is_reduction(join: &HashJoinExec) -> bool {
    join.join_type == JoinType::RightSemi
        && join.left().schema().fields().first().is_some_and(|field| {
            field
                .name()
                .starts_with(OPTIMIZER_SEMI_JOIN_REDUCTION_KEY_PREFIX)
        })
}

/// Returns the first candidate join in post-order, which has no candidates below it.
fn find_candidate(
    plan: &Arc<dyn ExecutionPlan>,
    settled: &HashSet<usize>,
) -> Result<Option<(Arc<dyn ExecutionPlan>, Candidate)>> {
    // Their inputs read results provided by the node itself, so they cannot run alone.
    if plan.is::<ScalarSubqueryExec>() || plan.is::<RecursiveQueryExec>() {
        return Ok(None);
    }
    for child in plan.children() {
        if let Some(found) = find_candidate(child, settled)? {
            return Ok(Some(found));
        }
    }
    if settled.contains(&address(plan)) {
        return Ok(None);
    }
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    let estimated = |side: &Arc<dyn ExecutionPlan>| -> Result<bool> {
        Ok(!matches!(
            statistics(side.as_ref())?.total_byte_size,
            Precision::Exact(_)
        ))
    };
    let candidate = match join.mode {
        // Null-aware anti joins must keep their sides.
        PartitionMode::Partitioned if !join.null_aware => {
            let build = broadcastable(join.join_type) && estimated(join.left())?;
            let probe = join.join_type.supports_swap()
                && broadcastable(join.join_type.swap())
                && estimated(join.right())?;
            if !build && !probe {
                return Ok(None);
            }
            Candidate::Partitioned { build, probe }
        }
        // Removing a reduction must not change which rows a limit keeps.
        PartitionMode::CollectLeft
            if is_reduction(join) && join.fetch().is_none() && estimated(join.left())? =>
        {
            Candidate::Reduction
        }
        _ => return Ok(None),
    };
    Ok(Some((Arc::clone(plan), candidate)))
}

/// The distributed planner partitions broadcast joins that emit unmatched build rows.
fn broadcastable(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
    )
}

/// The probe side of a reduction, which holds every row the reduction can return.
/// `probe` is the join's probe side or a plan that produces the same rows.
fn remove_reduction(
    join: &HashJoinExec,
    probe: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(projection) = join.projection.as_ref() else {
        return Ok(probe);
    };
    let schema = probe.schema();
    let exprs = projection
        .iter()
        .map(|&index| {
            ProjectionExpr::new(
                Arc::new(Column::new(schema.field(index).name(), index)),
                schema.field(index).name().clone(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(exprs, probe)?))
}

enum Materialized {
    Small(Arc<dyn ExecutionPlan>),
    /// Reached the threshold. A handed-off side carries its job and the plan that reads it.
    Large {
        rows: usize,
        bytes: usize,
        handoff: Option<(JobId, Arc<dyn ExecutionPlan>)>,
    },
    Failed,
    Skipped,
}

impl Materialized {
    fn handoff(&self) -> Option<Arc<dyn ExecutionPlan>> {
        match self {
            Materialized::Large {
                handoff: Some((_, plan)),
                ..
            } => Some(Arc::clone(plan)),
            _ => None,
        }
    }

    fn handoff_job(&self) -> Option<JobId> {
        match self {
            Materialized::Large {
                handoff: Some((job_id, _)),
                ..
            } => Some(*job_id),
            _ => None,
        }
    }
}

impl std::fmt::Display for Materialized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Materialized::Small(_) => write!(f, "small"),
            Materialized::Large {
                rows,
                bytes,
                handoff,
            } => {
                write!(f, "reached {bytes} bytes after {rows} rows")?;
                if let Some((job_id, _)) = handoff {
                    write!(f, ", handed off as job {job_id}")?;
                }
                Ok(())
            }
            Materialized::Failed => write!(f, "failed to run"),
            Materialized::Skipped => write!(f, "not tried"),
        }
    }
}

/// Runs the requested sides of `join` at the same time. A small build side is used as
/// is, so the probe side is then abandoned; otherwise both run to a decision.
async fn materialize_sides(
    join: &HashJoinExec,
    build: bool,
    probe: bool,
    options: &AdaptiveJoinOptions,
    runner: &dyn SideRunner,
    handoffs: &Handoffs,
) -> Result<(Materialized, Materialized)> {
    match (build, probe) {
        (true, true) => {
            let build = pin!(materialize(join.left(), options, runner, handoffs));
            let probe = pin!(materialize(join.right(), options, runner, handoffs));
            match select(build, probe).await {
                Either::Left((build, probe)) => {
                    let build = build?;
                    // Dropping the future cancels the probe side's job.
                    if matches!(build, Materialized::Small(_)) {
                        return Ok((build, Materialized::Skipped));
                    }
                    Ok((build, probe.await?))
                }
                Either::Right((probe, build)) => Ok((build.await?, probe?)),
            }
        }
        (true, false) => Ok((
            materialize(join.left(), options, runner, handoffs).await?,
            Materialized::Skipped,
        )),
        (false, true) => Ok((
            Materialized::Skipped,
            materialize(join.right(), options, runner, handoffs).await?,
        )),
        (false, false) => Ok((Materialized::Skipped, Materialized::Skipped)),
    }
}

/// Splits a join side into the plan its job runs and the distribution the job writes,
/// which is the side's own partitioning: the hash partitioning of a repartition on top,
/// or else the partitions of the side as they are.
fn split_side(side: &Arc<dyn ExecutionPlan>) -> (Arc<dyn ExecutionPlan>, OutputDistribution) {
    let mut input = Arc::clone(side);
    let mut distribution = OutputDistribution::RoundRobinBatch { channels: 1 };
    let mut top = true;
    while let Some(repartition) = input.downcast_ref::<RepartitionExec>() {
        if top {
            match repartition.partitioning() {
                Partitioning::Hash(keys, channels) => {
                    distribution = OutputDistribution::Hash {
                        keys: keys.clone(),
                        channels: *channels,
                    };
                }
                // The side's partitioning is not reproduced by writing the input as it is.
                _ => return (Arc::clone(side), distribution),
            }
        }
        top = false;
        input = Arc::clone(repartition.input());
    }
    (input, distribution)
}

/// Runs `side` without the exchanges above it, keeping its result if it is small enough
/// to broadcast, and handing its job off if not.
async fn materialize(
    side: &Arc<dyn ExecutionPlan>,
    options: &AdaptiveJoinOptions,
    runner: &dyn SideRunner,
    handoffs: &Handoffs,
) -> Result<Materialized> {
    let (input, distribution) = split_side(side);
    let input = handoffs.resolve(input)?;
    let schema = input.schema();
    // A failure here would recur in the main job, which reports it with the full plan.
    // Stream memory counts whole arrays, up to twice the sliced bytes of the threshold, so
    // a side below the threshold always ends within the budget and gets decided.
    let handoff = HandoffOutput {
        distribution,
        budget: options.broadcast_threshold.saturating_mul(4),
    };
    let (mut stream, handoff) = match runner.run(input, Some(handoff)).await {
        Ok(x) => x,
        Err(error) => {
            warn!("adaptive join selection: failed to run a join input: {error}");
            return Ok(Materialized::Failed);
        }
    };
    let mut batches = vec![];
    let (mut rows, mut bytes) = (0, 0);
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                warn!("adaptive join selection: failed to run a join input: {error}");
                return Ok(Materialized::Failed);
            }
        };
        rows += batch.num_rows();
        // Received batches can share buffers, so only the sliced bytes are counted here.
        bytes += sliced_size(&batch)?;
        if bytes >= options.broadcast_threshold {
            let handoff = match handoff {
                Some(source) => {
                    let job_id = source.job_id;
                    // Recorded first, so that the job is released even if this future is dropped.
                    handoffs.add(job_id);
                    if runner.hand_off(job_id).await {
                        let exec: Arc<dyn ExecutionPlan> =
                            Arc::new(HandoffExec::new(source, Arc::clone(side)));
                        Some((job_id, exec))
                    } else {
                        None
                    }
                }
                None => None,
            };
            // Dropping the stream cancels the job, unless it has been handed off.
            return Ok(Materialized::Large {
                rows,
                bytes,
                handoff,
            });
        }
        batches.push(batch);
    }
    let batch = compact(&concat_batches(&schema, &batches)?)?;
    let build = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    match statistics(build.as_ref())?.total_byte_size {
        Precision::Exact(bytes) if bytes < options.broadcast_threshold => {
            Ok(Materialized::Small(build))
        }
        _ => Ok(Materialized::Large {
            rows,
            bytes,
            handoff: None,
        }),
    }
}

/// Copies a batch into buffers of its own size, so that its memory size is what
/// a broadcast of it costs.
fn compact(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            let mut copy = MutableArrayData::new(vec![&data], false, data.len());
            copy.try_extend(0, 0, data.len())?;
            Ok(make_array(copy.freeze()))
        })
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

fn sliced_size(batch: &RecordBatch) -> Result<usize> {
    let mut size = 0;
    for column in batch.columns() {
        size += column.to_data().get_slice_memory_size()?;
    }
    Ok(size)
}

/// A broadcast join reads its probe side in any partitioning.
fn without_hash_repartition(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut plan = Arc::clone(plan);
    while let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        if !matches!(repartition.partitioning(), Partitioning::Hash(..)) {
            break;
        }
        plan = Arc::clone(repartition.input());
    }
    plan
}

fn broadcast_join(
    join: &HashJoinExec,
    build: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    join.builder()
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_new_children(vec![build, without_hash_repartition(join.right())])?
        .build_exec()
}

/// Broadcasts the materialized probe side, restoring the join's output columns.
fn swapped_broadcast_join(
    join: &HashJoinExec,
    probe: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // The join's dynamic filter targets the probe side, which no longer runs. Only a
    // join without one can be swapped.
    HashJoinExecBuilder::new(
        without_hash_repartition(join.left()),
        probe,
        join.on.clone(),
        join.join_type,
    )
    .with_partition_mode(PartitionMode::Partitioned)
    .with_filter(join.filter.clone())
    .with_projection_ref(join.projection.clone())
    .with_null_equality(join.null_equality)
    .with_null_aware(join.null_aware)
    .with_fetch(join.fetch())
    .build()?
    .swap_inputs(PartitionMode::CollectLeft)
}

/// Replaces `target` with `replacement`, restoring the input distributions that
/// the ancestors of `target` require.
fn replace_node(
    plan: &Arc<dyn ExecutionPlan>,
    target: &Arc<dyn ExecutionPlan>,
    replacement: &Arc<dyn ExecutionPlan>,
    options: &AdaptiveJoinOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if Arc::ptr_eq(plan, target) {
        return Ok(Some(Arc::clone(replacement)));
    }
    let mut changed = vec![];
    let mut children = vec![];
    for child in plan.children() {
        match replace_node(child, target, replacement, options)? {
            Some(child) => {
                changed.push(true);
                children.push(child);
            }
            None => {
                changed.push(false);
                children.push(Arc::clone(child));
            }
        }
    }
    if !changed.contains(&true) {
        return Ok(None);
    }
    let requirements = plan.input_distribution_requirements();
    let required = |index: usize| {
        requirements
            .child_distribution(index)
            .cloned()
            .unwrap_or(Distribution::UnspecifiedDistribution)
    };
    // Key-partitioned inputs keep the partition count of the unchanged ones,
    // which may be co-partitioned with them.
    let partitions = children
        .iter()
        .enumerate()
        .find_map(|(index, child)| {
            (!changed[index] && matches!(required(index), Distribution::KeyPartitioned(_)))
                .then(|| child.output_partitioning().partition_count())
        })
        .unwrap_or(options.target_partitions);
    let children = children
        .into_iter()
        .enumerate()
        .map(|(index, child)| {
            if !changed[index] {
                return Ok(child);
            }
            let satisfied = requirements.child_satisfaction(
                index,
                child.as_ref(),
                ChildSatisfactionOptions::new(),
            )? == PartitioningSatisfaction::Exact;
            match required(index) {
                Distribution::SinglePartition if !satisfied => {
                    Ok(Arc::new(CoalescePartitionsExec::new(child)) as Arc<dyn ExecutionPlan>)
                }
                Distribution::KeyPartitioned(exprs)
                    if !satisfied
                        || child.output_partitioning().partition_count() != partitions =>
                {
                    Ok(Arc::new(RepartitionExec::try_new(
                        child,
                        Partitioning::Hash(exprs, partitions),
                    )?) as Arc<dyn ExecutionPlan>)
                }
                _ => Ok(child),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(replace_children_if_necessary(
        Arc::clone(plan),
        children,
    )?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::NullEquality;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::{collect, execute_stream};

    use super::*;

    const PARTITIONS: usize = 4;

    fn schema(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(format!("{name}_key"), DataType::Int64, true),
            Field::new(format!("{name}_value"), DataType::Int64, true),
        ]))
    }

    /// A multi-partition source whose rows are `(key, key * 10)` for the given keys.
    fn source(name: &str, keys: &[i64]) -> Arc<dyn ExecutionPlan> {
        let schema = schema(name);
        let partitions = keys
            .chunks(keys.len().div_ceil(PARTITIONS).max(1))
            .map(|keys| {
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(keys.to_vec())),
                            Arc::new(Int64Array::from_iter_values(keys.iter().map(|k| k * 10))),
                        ],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap()
    }

    /// Filtered so that its statistics are only estimates.
    fn filtered(plan: Arc<dyn ExecutionPlan>, min_key: i64) -> Arc<dyn ExecutionPlan> {
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(plan.schema().field(0).name(), 0)),
            Operator::GtEq,
            lit(min_key),
        ));
        Arc::new(FilterExec::try_new(predicate, plan).unwrap())
    }

    fn hash(plan: Arc<dyn ExecutionPlan>, column: usize) -> Arc<dyn ExecutionPlan> {
        let name = plan.schema().field(column).name().clone();
        Arc::new(
            RepartitionExec::try_new(
                plan,
                Partitioning::Hash(vec![Arc::new(Column::new(&name, column))], PARTITIONS),
            )
            .unwrap(),
        )
    }

    fn partitioned_join(
        left: Arc<dyn ExecutionPlan>,
        left_key: usize,
        right: Arc<dyn ExecutionPlan>,
        right_key: usize,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            Arc::new(Column::new(left.schema().field(left_key).name(), left_key))
                as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new(
                right.schema().field(right_key).name(),
                right_key,
            )) as Arc<dyn PhysicalExpr>,
        )];
        Arc::new(
            HashJoinExec::try_new(
                hash(left, left_key),
                hash(right, right_key),
                on,
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn options(broadcast_threshold: usize) -> AdaptiveJoinOptions {
        let mut config = ConfigOptions::default();
        config.optimizer.hash_join_single_partition_threshold = broadcast_threshold;
        config.execution.target_partitions = PARTITIONS;
        AdaptiveJoinOptions::new(Arc::new(config))
    }

    /// Replaces every hand-off with the subtree it replaced.
    fn without_handoffs(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|node| match node.downcast_ref::<HandoffExec>() {
                Some(handoff) => Ok(Transformed::yes(Arc::clone(handoff.original()))),
                None => Ok(Transformed::no(node)),
            })?
            .data)
    }

    /// Runs sides locally. With `handoff`, every job can be handed off, and the plans
    /// reading a hand-off fall back to the subtree it replaced when they are run.
    #[derive(Default)]
    struct LocalRunner {
        handoff: bool,
        jobs: AtomicUsize,
        released: Mutex<Vec<JobId>>,
    }

    #[tonic::async_trait]
    impl SideRunner for LocalRunner {
        async fn run(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            handoff: Option<HandoffOutput>,
        ) -> Result<(SendableRecordBatchStream, Option<ExternalStageInput>)> {
            let job = self.jobs.fetch_add(1, Ordering::Relaxed);
            let source = match handoff {
                Some(HandoffOutput { distribution, .. }) if self.handoff => {
                    Some(ExternalStageInput {
                        job_id: JobId::from(job as u64),
                        stage: 0,
                        partitions: plan.output_partitioning().partition_count(),
                        channels: distribution.channels(),
                        mode: crate::job_graph::InputMode::Shuffle,
                    })
                }
                _ => None,
            };
            let plan = without_handoffs(plan)?;
            Ok((
                execute_stream(plan, Arc::new(TaskContext::default()))?,
                source,
            ))
        }

        async fn hand_off(&self, _job_id: JobId) -> bool {
            self.handoff
        }

        fn release(&self, job_id: JobId) {
            self.released.lock().unwrap().push(job_id);
        }
    }

    async fn select_with(
        plan: &Arc<dyn ExecutionPlan>,
        options: &AdaptiveJoinOptions,
        runner: &LocalRunner,
    ) -> Arc<dyn ExecutionPlan> {
        let handoffs = Handoffs::default();
        let plan = select_joins(Arc::clone(plan), options, runner, &handoffs)
            .await
            .unwrap();
        handoffs.resolve(plan).unwrap()
    }

    async fn select(
        plan: &Arc<dyn ExecutionPlan>,
        options: &AdaptiveJoinOptions,
    ) -> (Arc<dyn ExecutionPlan>, usize) {
        let runner = LocalRunner::default();
        let plan = select_with(plan, options, &runner).await;
        (plan, runner.jobs.load(Ordering::Relaxed))
    }

    fn handoff_jobs(plan: &Arc<dyn ExecutionPlan>) -> Vec<u64> {
        let mut jobs = vec![];
        if let Some(handoff) = plan.downcast_ref::<HandoffExec>() {
            jobs.push(u64::from(handoff.source().job_id));
        }
        for child in plan.children() {
            jobs.extend(handoff_jobs(child));
        }
        jobs
    }

    #[tokio::test]
    async fn large_sides_are_handed_off_to_a_partitioned_join() {
        let plan = || {
            partitioned_join(
                filtered(source("b", &keys(0..1000)), 0),
                0,
                filtered(source("p", &keys(0..1000)), 0),
                0,
                JoinType::Inner,
            )
        };
        let runner = LocalRunner {
            handoff: true,
            ..Default::default()
        };
        let adapted = select_with(&plan(), &options(1024), &runner).await;
        assert_eq!(runner.jobs.load(Ordering::Relaxed), 2);
        assert_eq!(joins(&adapted), vec![PartitionMode::Partitioned]);
        let mut jobs = handoff_jobs(&adapted);
        jobs.sort();
        assert_eq!(jobs, vec![0, 1]);
        assert!(runner.released.lock().unwrap().is_empty());
        SanityCheckPlan::new()
            .optimize(Arc::clone(&adapted), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            sorted_output(without_handoffs(adapted).unwrap()).await,
            sorted_output(plan()).await
        );
    }

    #[tokio::test]
    async fn a_broadcast_join_releases_the_probe_side_hand_off() {
        // The probe side reaches the threshold first, then the build side is small.
        let plan = partitioned_join(
            filtered(source("b", &keys(0..1000)), 995),
            0,
            filtered(source("p", &keys(0..100_000)), 0),
            0,
            JoinType::Inner,
        );
        let runner = LocalRunner {
            handoff: true,
            ..Default::default()
        };
        let adapted = select_with(&plan, &options(1024), &runner).await;
        assert_eq!(joins(&adapted), vec![PartitionMode::CollectLeft]);
        assert!(handoff_jobs(&adapted).is_empty());
        assert_eq!(sorted_output(adapted).await, sorted_output(plan).await);
    }

    #[tokio::test]
    async fn a_hand_off_is_read_once() {
        let runner = LocalRunner {
            handoff: true,
            ..Default::default()
        };
        let source = ExternalStageInput {
            job_id: JobId::from(7),
            stage: 0,
            partitions: PARTITIONS,
            channels: PARTITIONS,
            mode: crate::job_graph::InputMode::Shuffle,
        };
        let original = hash(source_plan(), 0);
        let handoff: Arc<dyn ExecutionPlan> =
            Arc::new(HandoffExec::new(source, Arc::clone(&original)));
        let plan = partitioned_join_of(Arc::clone(&handoff), Arc::clone(&handoff));
        let handoffs = Handoffs::default();
        let resolved = handoffs.resolve(plan).unwrap();
        assert_eq!(handoff_jobs(&resolved), vec![7]);
        drop(runner);
    }

    fn source_plan() -> Arc<dyn ExecutionPlan> {
        source("s", &keys(0..10))
    }

    fn partitioned_join_of(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            Arc::new(Column::new(left.schema().field(0).name(), 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new(right.schema().field(0).name(), 0)) as Arc<dyn PhysicalExpr>,
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    async fn sorted_output(plan: Arc<dyn ExecutionPlan>) -> Vec<String> {
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let mut lines = pretty_format_batches(&batches)
            .unwrap()
            .to_string()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        lines.sort();
        lines
    }

    fn joins(plan: &Arc<dyn ExecutionPlan>) -> Vec<PartitionMode> {
        let mut modes = vec![];
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            modes.push(join.mode);
        }
        for child in plan.children() {
            modes.extend(joins(child));
        }
        modes
    }

    fn keys(range: std::ops::Range<i64>) -> Vec<i64> {
        range.collect()
    }

    #[tokio::test]
    async fn small_build_side_is_broadcast() {
        // Only 10 of 1,000 build rows pass the filter, which is estimated at 20%.
        let build = filtered(source("b", &keys(0..1000)), 990);
        let probe = source("p", &keys(0..5000));
        let plan = partitioned_join(build, 0, probe, 0, JoinType::Inner);
        let (adapted, jobs) = select(&plan, &options(1024)).await;
        assert_eq!(jobs, 1);
        assert_eq!(joins(&adapted), vec![PartitionMode::CollectLeft]);
        let join = adapted.downcast_ref::<HashJoinExec>().unwrap();
        assert!(join.right().downcast_ref::<RepartitionExec>().is_none());
        assert_eq!(
            statistics(join.left().as_ref()).unwrap().num_rows,
            Precision::Exact(10)
        );
        assert_eq!(sorted_output(adapted).await, sorted_output(plan).await);
    }

    #[tokio::test]
    async fn large_build_side_stays_partitioned() {
        let build = filtered(source("b", &keys(0..1000)), 0);
        let probe = source("p", &keys(0..5000));
        let plan = partitioned_join(build, 0, probe, 0, JoinType::Inner);
        let (adapted, jobs) = select(&plan, &options(1024)).await;
        assert_eq!(jobs, 1);
        assert!(Arc::ptr_eq(&adapted, &plan));
    }

    #[tokio::test]
    async fn joins_emitting_unmatched_build_rows_are_skipped() {
        for join_type in [JoinType::Left, JoinType::Full, JoinType::LeftSemi] {
            let build = filtered(source("b", &keys(0..1000)), 990);
            let probe = source("p", &keys(0..5000));
            let plan = partitioned_join(build, 0, probe, 0, join_type);
            let (adapted, jobs) = select(&plan, &options(1024)).await;
            assert_eq!(jobs, 0);
            assert!(Arc::ptr_eq(&adapted, &plan));
        }
    }

    /// The shape of TPC-H Q17: a selective join feeding a partitioned join on the same key.
    fn nested_joins() -> Arc<dyn ExecutionPlan> {
        let part = filtered(source("part", &keys(0..1000)), 995);
        let lineitem = source(
            "line",
            &keys(0..40_000).iter().map(|k| k % 1000).collect::<Vec<_>>(),
        );
        let inner = partitioned_join(part, 0, lineitem, 0, JoinType::Inner);
        let average = filtered(source("avg", &keys(0..1000)), 0);
        partitioned_join(inner, 0, average, 0, JoinType::Inner)
    }

    #[tokio::test]
    async fn small_probe_side_is_broadcast_with_sides_swapped() {
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ] {
            // 1,000 build rows against 10 of 1,000 probe rows.
            let plan = || {
                partitioned_join(
                    filtered(source("b", &keys(0..1000)), 0),
                    0,
                    filtered(source("p", &keys(0..1000)), 990),
                    0,
                    join_type,
                )
            };
            let original = plan();
            let (adapted, jobs) = select(&original, &options(1024)).await;
            // Only an inner join can broadcast its build side as well.
            let expected = if join_type == JoinType::Inner { 2 } else { 1 };
            assert_eq!(jobs, expected, "{join_type}");
            assert_eq!(
                joins(&adapted),
                vec![PartitionMode::CollectLeft],
                "{join_type}"
            );
            assert_eq!(adapted.schema(), original.schema(), "{join_type}");
            let mut join = Arc::clone(&adapted);
            while !join.is::<HashJoinExec>() {
                join = Arc::clone(join.children()[0]);
            }
            let join = join.downcast_ref::<HashJoinExec>().unwrap();
            assert_eq!(join.join_type, join_type.swap(), "{join_type}");
            assert_eq!(
                statistics(join.left().as_ref()).unwrap().num_rows,
                Precision::Exact(10),
                "{join_type}"
            );
            SanityCheckPlan::new()
                .optimize(Arc::clone(&adapted), &ConfigOptions::default())
                .unwrap();
            assert_eq!(
                sorted_output(adapted).await,
                sorted_output(plan()).await,
                "{join_type}"
            );
        }
    }

    #[tokio::test]
    async fn a_small_build_side_is_preferred_to_swapping() {
        let plan = partitioned_join(
            filtered(source("b", &keys(0..1000)), 995),
            0,
            filtered(source("p", &keys(0..1000)), 990),
            0,
            JoinType::Inner,
        );
        let (adapted, _) = select(&plan, &options(1024)).await;
        let join = adapted.downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(join.mode, PartitionMode::CollectLeft);
        assert_eq!(
            statistics(join.left().as_ref()).unwrap().num_rows,
            Precision::Exact(5)
        );
    }

    /// A speculative reduction of `probe` by the distinct keys of `keys`.
    fn reduction(
        keys: Arc<dyn ExecutionPlan>,
        probe: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };

        let name = format!("{OPTIMIZER_SEMI_JOIN_REDUCTION_KEY_PREFIX}0");
        let keys: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(keys));
        let key = Arc::new(Column::new(keys.schema().field(0).name(), 0));
        let distinct: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![(key, name.clone())]),
                vec![],
                vec![],
                Arc::clone(&keys),
                keys.schema(),
            )
            .unwrap(),
        );
        let probe_key = Arc::new(Column::new(probe.schema().field(0).name(), 0));
        Arc::new(
            HashJoinExec::try_new(
                distinct,
                probe,
                vec![(Arc::new(Column::new(&name, 0)) as _, probe_key as _)],
                None,
                &JoinType::RightSemi,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// A join whose probe side is reduced by the keys of its build side.
    fn reduced_join(min_key: i64) -> Arc<dyn ExecutionPlan> {
        let dimension = || filtered(source("dim", &keys(0..1000)), min_key);
        let facts = source(
            "fact",
            &keys(0..5000).iter().map(|k| k % 1000).collect::<Vec<_>>(),
        );
        partitioned_join(
            dimension(),
            0,
            reduction(dimension(), facts),
            0,
            JoinType::Inner,
        )
    }

    fn reductions(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<Statistics>> {
        let mut found = vec![];
        if let Some(join) = plan.downcast_ref::<HashJoinExec>()
            && is_reduction(join)
        {
            found.push(statistics(join.left().as_ref()).unwrap());
        }
        for child in plan.children() {
            found.extend(reductions(child));
        }
        found
    }

    #[tokio::test]
    async fn small_reduction_is_kept() {
        let (adapted, jobs) = select(&reduced_join(990), &options(1024)).await;
        // The reduction's keys, then both sides of the outer join at once.
        assert_eq!(jobs, 3);
        let found = reductions(&adapted);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].num_rows.get_value(), Some(&10));
        SanityCheckPlan::new()
            .optimize(Arc::clone(&adapted), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            sorted_output(adapted).await,
            sorted_output(reduced_join(990)).await
        );
    }

    #[tokio::test]
    async fn large_partitioned_reduction_is_removed() {
        let dimension = filtered(source("dim", &keys(0..1000)), 0);
        let name = format!("{OPTIMIZER_SEMI_JOIN_REDUCTION_KEY_PREFIX}0");
        let reduction_keys: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![ProjectionExpr::new(
                    Arc::new(Column::new("dim_key", 0)),
                    name,
                )],
                dimension,
            )
            .unwrap(),
        );
        let facts = || source("fact", &keys(0..5000));
        let plan = partitioned_join(reduction_keys, 0, facts(), 0, JoinType::RightSemi);
        let (adapted, jobs) = select(&plan, &options(1024)).await;
        assert_eq!(jobs, 1);
        assert!(reductions(&adapted).is_empty());
        assert!(joins(&adapted).is_empty());
        assert_eq!(sorted_output(adapted).await, sorted_output(facts()).await);
    }

    #[tokio::test]
    async fn reduction_is_sized_by_its_key_input() {
        // 10 distinct keys, but 2000 key rows: the keys are sized before deduplication.
        let duplicated = || {
            filtered(
                source(
                    "dim",
                    &keys(0..2000).iter().map(|k| k % 1000).collect::<Vec<_>>(),
                ),
                990,
            )
        };
        let facts = source(
            "fact",
            &keys(0..5000).iter().map(|k| k % 1000).collect::<Vec<_>>(),
        );
        let plan = || {
            partitioned_join(
                duplicated(),
                0,
                reduction(duplicated(), facts.clone()),
                0,
                JoinType::Inner,
            )
        };
        let (adapted, _) = select(&plan(), &options(128)).await;
        assert!(reductions(&adapted).is_empty());
        assert_eq!(sorted_output(adapted).await, sorted_output(plan()).await);
    }

    #[tokio::test]
    async fn large_reduction_is_removed() {
        let (adapted, jobs) = select(&reduced_join(0), &options(1024)).await;
        assert_eq!(jobs, 2);
        assert!(reductions(&adapted).is_empty());
        assert_eq!(joins(&adapted), vec![PartitionMode::Partitioned]);
        SanityCheckPlan::new()
            .optimize(Arc::clone(&adapted), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            sorted_output(adapted).await,
            sorted_output(reduced_join(0)).await
        );
    }

    #[tokio::test]
    async fn ancestors_keep_their_required_distribution() {
        // Large enough for the 5 inner build rows but not for the 200 rows of the outer join.
        let (adapted, jobs) = select(&nested_joins(), &options(2048)).await;
        // The inner build side, then both sides of the outer join (200 rows each).
        assert_eq!(jobs, 3);
        assert_eq!(
            joins(&adapted),
            vec![PartitionMode::Partitioned, PartitionMode::CollectLeft]
        );
        let outer = adapted.downcast_ref::<HashJoinExec>().unwrap();
        let repartition = outer.left().downcast_ref::<RepartitionExec>().unwrap();
        assert!(repartition.input().is::<HashJoinExec>());
        assert_eq!(
            repartition.partitioning().partition_count(),
            outer.right().output_partitioning().partition_count()
        );
        SanityCheckPlan::new()
            .optimize(Arc::clone(&adapted), &ConfigOptions::default())
            .unwrap();
        // Plans keep execution state, so the expected output comes from a separate plan.
        assert_eq!(
            sorted_output(adapted).await,
            sorted_output(nested_joins()).await
        );
    }
}
