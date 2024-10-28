use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::JobId;
use crate::plan::{ShuffleReadExec, ShuffleWriteExec};

pub struct JobGraph {
    job_id: JobId,
    stages: Vec<Arc<dyn ExecutionPlan>>,
}

impl Display for JobGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "= JobGraph =")?;
        for (i, stage) in self.stages.iter().enumerate() {
            let displayable = DisplayableExecutionPlan::new(stage.as_ref());
            writeln!(f, "== Stage {i} ==")?;
            writeln!(f, "{}", displayable.indent(true))?;
        }
        Ok(())
    }
}

impl JobGraph {
    pub fn try_new(job_id: JobId, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Self> {
        let mut graph = Self {
            job_id,
            stages: vec![],
        };
        let root = build_job_graph(plan, &mut graph)?;
        // TODO: Should we coalesce partitions here?
        //   This would make the final stage run in a single task.
        let root = if root.properties().output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(root))
        } else {
            root
        };
        graph.stages.push(root);
        Ok(graph)
    }
}

fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|x| build_job_graph(x.clone(), graph))
        .collect::<ExecutionResult<Vec<_>>>()?;
    let plan = with_new_children_if_necessary(plan, children)?;

    let plan = if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match repartition.partitioning() {
            Partitioning::UnknownPartitioning(_) | Partitioning::RoundRobinBatch(_) => {
                get_one_child_plan(&plan)?
            }
            partitioning => {
                let child = get_one_child_plan(&plan)?;
                create_shuffle(&child, graph, partitioning.clone())?
            }
        }
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
        || plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .is_some()
    {
        let child = get_one_child_plan(&plan)?;
        let partitioning = plan.properties().output_partitioning();
        let child = create_shuffle(&child, graph, partitioning.clone())?;
        with_new_children_if_necessary(plan, vec![child])?
    } else {
        plan
    };
    Ok(plan)
}

fn get_one_child_plan(plan: &Arc<dyn ExecutionPlan>) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    if plan.children().len() != 1 {
        return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
            "expected one child but got {}: {:?}",
            plan.children().len(),
            plan
        )));
    }
    Ok(plan.children()[0].clone())
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    partitioning: Partitioning,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let stage = graph.stages.len();

    let writer = Arc::new(ShuffleWriteExec::new(
        graph.job_id,
        stage,
        plan.clone(),
        partitioning.clone(),
    ));
    graph.stages.push(writer);

    Ok(Arc::new(ShuffleReadExec::new(
        graph.job_id,
        stage,
        plan.schema(),
        partitioning,
    )))
}
