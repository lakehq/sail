// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, MutableArrayData, RecordBatch, RecordBatchOptions, make_array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::common::stats::Precision;
use datafusion::common::{JoinType, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PartitioningSatisfaction;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{
    ChildSatisfactionOptions, Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    displayable, replace_children_if_necessary,
};
use futures::StreamExt;
use log::{info, warn};

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

/// Chooses between broadcast and partitioned hash joins from the actual size of build sides.
///
/// The deepest partitioned hash join whose build size is only estimated has its build side
/// run first through `run`. A result below the broadcast threshold replaces the build side,
/// and the join becomes a broadcast join without the probe-side shuffle. A build side that
/// reaches the threshold is abandoned at that point, and the join stays partitioned.
pub(crate) async fn select_joins<F, Fut>(
    mut plan: Arc<dyn ExecutionPlan>,
    options: &AdaptiveJoinOptions,
    run: F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Fut,
    Fut: Future<Output = Result<SendableRecordBatchStream>>,
{
    // Joins that stay partitioned are kept as the same node, so their address identifies them.
    let mut settled = HashSet::new();
    while let Some(node) = find_candidate(&plan, &settled)? {
        settled.insert(address(&node));
        let Some(join) = node.downcast_ref::<HashJoinExec>() else {
            continue;
        };
        let description = displayable(join).one_line().to_string();
        let estimate = statistics(join.left().as_ref())?;
        let build = match materialize_build_side(join, options, &run).await? {
            Materialized::Small(build) => build,
            Materialized::Large { rows, bytes } => {
                info!(
                    "adaptive join selection: partitioned, build side reached {bytes} bytes after {rows} rows (estimated rows {:?}, bytes {:?}): {}",
                    estimate.num_rows,
                    estimate.total_byte_size,
                    description.trim_end()
                );
                continue;
            }
            Materialized::Failed => continue,
        };
        let actual = statistics(build.as_ref())?;
        let broadcast = broadcast_join(join, build)?;
        let candidate = replace_node(&plan, &node, &broadcast, options)?.unwrap_or(broadcast);
        if let Err(error) = SanityCheckPlan::new().optimize(Arc::clone(&candidate), &options.config)
        {
            warn!("adaptive join selection: keeping the join partitioned: {error}");
            continue;
        }
        info!(
            "adaptive join selection: broadcast, build side rows {:?}, bytes {:?} (estimated rows {:?}, bytes {:?}): {}",
            actual.num_rows,
            actual.total_byte_size,
            estimate.num_rows,
            estimate.total_byte_size,
            description.trim_end()
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

/// Returns the first candidate join in post-order, which has no candidates below it.
fn find_candidate(
    plan: &Arc<dyn ExecutionPlan>,
    settled: &HashSet<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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
    // The distributed planner partitions broadcast joins that emit unmatched build rows.
    let broadcastable = matches!(
        join.join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
    );
    if join.mode != PartitionMode::Partitioned || join.null_aware || !broadcastable {
        return Ok(None);
    }
    if matches!(
        statistics(join.left().as_ref())?.total_byte_size,
        Precision::Exact(_)
    ) {
        return Ok(None);
    }
    Ok(Some(Arc::clone(plan)))
}

enum Materialized {
    Small(Arc<dyn ExecutionPlan>),
    Large { rows: usize, bytes: usize },
    Failed,
}

async fn materialize_build_side<F, Fut>(
    join: &HashJoinExec,
    options: &AdaptiveJoinOptions,
    run: &F,
) -> Result<Materialized>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Fut,
    Fut: Future<Output = Result<SendableRecordBatchStream>>,
{
    let mut input = Arc::clone(join.left());
    while let Some(repartition) = input.downcast_ref::<RepartitionExec>() {
        input = Arc::clone(repartition.input());
    }
    if input.output_partitioning().partition_count() > 1 {
        input = Arc::new(CoalescePartitionsExec::new(input));
    }
    let schema = input.schema();
    // A failure here would recur in the main job, which reports it with the full plan.
    let mut stream = match run(input).await {
        Ok(stream) => stream,
        Err(error) => {
            warn!("adaptive join selection: failed to run the build side: {error}");
            return Ok(Materialized::Failed);
        }
    };
    let mut batches = vec![];
    let (mut rows, mut bytes) = (0, 0);
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                warn!("adaptive join selection: failed to run the build side: {error}");
                return Ok(Materialized::Failed);
            }
        };
        rows += batch.num_rows();
        // Received batches can share buffers, so only the sliced bytes are counted here.
        bytes += sliced_size(&batch)?;
        if bytes >= options.broadcast_threshold {
            // Dropping the stream cancels the job.
            return Ok(Materialized::Large { rows, bytes });
        }
        batches.push(batch);
    }
    let batch = compact(&concat_batches(&schema, &batches)?)?;
    let build = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    match statistics(build.as_ref())?.total_byte_size {
        Precision::Exact(bytes) if bytes < options.broadcast_threshold => {
            Ok(Materialized::Small(build))
        }
        _ => Ok(Materialized::Large { rows, bytes }),
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

fn broadcast_join(
    join: &HashJoinExec,
    build: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // A broadcast join reads its probe side in any partitioning.
    let mut probe = Arc::clone(join.right());
    while let Some(repartition) = probe.downcast_ref::<RepartitionExec>() {
        if !matches!(repartition.partitioning(), Partitioning::Hash(..)) {
            break;
        }
        probe = Arc::clone(repartition.input());
    }
    join.builder()
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_new_children(vec![build, probe])?
        .build_exec()
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
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
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

    async fn select(
        plan: &Arc<dyn ExecutionPlan>,
        options: &AdaptiveJoinOptions,
    ) -> (Arc<dyn ExecutionPlan>, usize) {
        let jobs = AtomicUsize::new(0);
        let plan = select_joins(Arc::clone(plan), options, |plan| {
            jobs.fetch_add(1, Ordering::Relaxed);
            async move { execute_stream(plan, Arc::new(TaskContext::default())) }
        })
        .await
        .unwrap();
        (plan, jobs.load(Ordering::Relaxed))
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
    async fn ancestors_keep_their_required_distribution() {
        // Large enough for the 5 inner build rows but not for the 200 rows of the outer join.
        let (adapted, jobs) = select(&nested_joins(), &options(2048)).await;
        assert_eq!(jobs, 2);
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
