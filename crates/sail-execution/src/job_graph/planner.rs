use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    with_new_children_if_necessary, ExecutionPlan, ExecutionPlanProperties,
};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::plan::{ShuffleConsumption, StageInputExec};

impl JobGraph {
    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Self> {
        let plan = ensure_single_partition_for_fetch(plan)?;
        let mut graph = Self {
            stages: vec![],
            schema: plan.schema(),
        };
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph)?;
        let (last, inputs) = rewrite_inputs(last)?;
        graph.stages.push(Stage {
            inputs,
            plan: last,
            group: String::new(),
            mode: OutputMode::Pipelined,
            distribution: OutputDistribution::RoundRobin { channels: 1 },
            placement: TaskPlacement::Worker,
        });
        Ok(graph)
    }
}

fn ensure_single_partition_for_fetch(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // Rewrite *all* `GlobalLimitExec` nodes in the tree to ensure their input is single-partition
    let result = plan.transform(|node| {
        if let Some(gl) = node.as_any().downcast_ref::<GlobalLimitExec>() {
            let rebuilt = rebuild_global_limit(gl)?;
            Ok(Transformed::yes(rebuilt))
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok(result.data()?)
}

fn rebuild_global_limit(
    gl: &GlobalLimitExec,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let skip = gl.skip();
    let fetch = gl.fetch();
    // If there is neither LIMIT nor OFFSET, return as-is.
    if fetch.is_none() && skip == 0 {
        return Ok(Arc::new(gl.clone()));
    }

    // Keep `LocalLimitExec` (if any) to preserve the per-partition "top-k" optimization, but make
    // sure the input to `GlobalLimitExec` is single-partition.
    let mut input: Arc<dyn ExecutionPlan> = gl.input().clone();

    if input.output_partitioning().partition_count() > 1 {
        input = Arc::new(CoalescePartitionsExec::new(input));
    }

    Ok(Arc::new(GlobalLimitExec::new(input, skip, fetch)))
}

/// A flag to indicate how the partitions from physical plan execution are used.
#[derive(Clone, Copy)]
enum PartitionUsage {
    /// Each partition of the plan is only used once.
    Once,
    /// The same partition may be used multiple times when producing partitions
    /// for the parent physical plan.
    ///
    /// This is typically needed for an optimized join operation where
    /// the build-side data (small) of only one partition is gathered via `plan.execute(0)`
    /// for each partition of the probe-side data.
    /// For single-host execution, DataFusion uses `OnceAsync` to ensure the
    /// build-side is only evaluated once. In the distributed setting, we use this
    /// usage information to create materialized shuffle data that can be
    /// consumed multiple times.
    Shared,
}

fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // Recursively build the job graph for the children first
    // and propagate partition usage information.
    let children = if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let (left, right) = join.children().two()?;
        match join.mode {
            PartitionMode::Partitioned => {
                vec![
                    build_job_graph(left.clone(), usage, graph)?,
                    build_job_graph(right.clone(), usage, graph)?,
                ]
            }
            PartitionMode::CollectLeft => {
                vec![
                    build_job_graph(left.clone(), PartitionUsage::Shared, graph)?,
                    build_job_graph(right.clone(), usage, graph)?,
                ]
            }
            PartitionMode::Auto => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unresolved auto partition mode in hash join"
                )));
            }
        }
    } else if plan.as_any().is::<NestedLoopJoinExec>()
        || plan.as_any().is::<CrossJoinExec>()
        || plan.as_any().is::<PiecewiseMergeJoinExec>()
    {
        let (left, right) = plan.children().two()?;
        vec![
            build_job_graph(left.clone(), PartitionUsage::Shared, graph)?,
            build_job_graph(right.clone(), usage, graph)?,
        ]
    } else if plan.as_any().is::<RepartitionExec>() || plan.as_any().is::<CoalescePartitionsExec>()
    {
        let child = plan.children().one()?;
        // At the shuffle boundary, we only expect to use the child partition once
        // since the shuffle writer can materialize the data for multiple consumption.
        vec![build_job_graph(child.clone(), PartitionUsage::Once, graph)?]
    } else {
        plan.children()
            .into_iter()
            .map(|x| build_job_graph(x.clone(), usage, graph))
            .collect::<ExecutionResult<Vec<_>>>()?
    };
    let plan = with_new_children_if_necessary(plan, children)?;

    let consumption = match usage {
        PartitionUsage::Once => ShuffleConsumption::Single,
        PartitionUsage::Shared => ShuffleConsumption::Multiple,
    };
    let plan = if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        let child = plan.children().one()?;
        match repartition.partitioning() {
            Partitioning::UnknownPartitioning(n) => {
                create_shuffle(child, graph, Partitioning::RoundRobinBatch(*n), consumption)?
            }
            x @ Partitioning::RoundRobinBatch(_) | x @ Partitioning::Hash(_, _) => {
                create_shuffle(child, graph, x.clone(), consumption)?
            }
        }
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        let child = plan.children().one()?;
        let partitioning = coalesce.properties().partitioning.clone();
        let fetch = coalesce.fetch();
        let shuffled = create_shuffle(child, graph, partitioning, consumption)?;
        if let Some(f) = fetch {
            Arc::new(GlobalLimitExec::new(shuffled, 0, Some(f))) as Arc<dyn ExecutionPlan>
        } else {
            shuffled
        }
    } else if plan.as_any().is::<SortPreservingMergeExec>() {
        let child = plan.children().one()?;
        plan.clone()
            .with_new_children(vec![create_merge_input(child, graph)?])?
    } else {
        plan
    };
    Ok(plan)
}

fn create_merge_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let partitioning = plan.output_partitioning().clone();
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Merge,
        },
        schema,
        partitioning,
    )))
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    partitioning: Partitioning,
    consumption: ShuffleConsumption,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = match partitioning.clone() {
        Partitioning::RoundRobinBatch(channels) | Partitioning::UnknownPartitioning(channels) => {
            OutputDistribution::RoundRobin { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
    };
    let schema = plan.schema();
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution,
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(Arc::new(StageInputExec::new(
        StageInput { stage: s, mode },
        schema,
        partitioning.clone(),
    )))
}

fn rewrite_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<(Arc<dyn ExecutionPlan>, Vec<StageInput>)> {
    let mut inputs = vec![];
    let result = plan.transform(|node| {
        if let Some(placeholder) = node.as_any().downcast_ref::<StageInputExec<StageInput>>() {
            let index = inputs.len();
            inputs.push(placeholder.input().clone());
            let placeholder = StageInputExec::new(
                index,
                placeholder.schema(),
                placeholder.properties().output_partitioning().clone(),
            );
            Ok(Transformed::yes(Arc::new(placeholder)))
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok((result.data()?, inputs))
}
