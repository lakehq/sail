use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{ExecutionError, ExecutionResult};
use crate::plan::{ShuffleConsumption, ShuffleReadExec, ShuffleWriteExec};

pub struct JobGraph {
    stages: Vec<Arc<dyn ExecutionPlan>>,
}

impl Display for JobGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (i, stage) in self.stages.iter().enumerate() {
            let displayable = DisplayableExecutionPlan::new(stage.as_ref());
            writeln!(f, "=== stage {i} ===")?;
            writeln!(f, "{}", displayable.indent(true))?;
        }
        Ok(())
    }
}

impl JobGraph {
    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Self> {
        let mut graph = Self { stages: vec![] };
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph)?;
        graph.stages.push(last);
        Ok(graph)
    }

    pub fn stages(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.stages
    }
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
        create_shuffle(child, graph, partitioning, consumption)?
    } else {
        plan
    };
    Ok(plan)
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    partitioning: Partitioning,
    consumption: ShuffleConsumption,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let stage = graph.stages.len();

    let writer = Arc::new(ShuffleWriteExec::new(
        stage,
        plan.clone(),
        partitioning.clone(),
        consumption,
    ));
    graph.stages.push(writer);

    Ok(Arc::new(ShuffleReadExec::new(
        stage,
        plan.schema(),
        partitioning,
    )))
}
