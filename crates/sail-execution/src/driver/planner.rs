use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

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
        let last = build_job_graph(plan, &mut graph)?;
        graph.stages.push(last);
        Ok(graph)
    }

    pub fn stages(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.stages
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
                // TODO: should we shuffle here?
                get_one_child_plan(&plan)?
            }
            partitioning => {
                let child = get_one_child_plan(&plan)?;
                create_shuffle(
                    &child,
                    graph,
                    partitioning.clone(),
                    ShuffleConsumption::Single,
                )?
            }
        }
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        let child = get_one_child_plan(&plan)?;
        let partitioning = child.properties().output_partitioning();
        let child = create_shuffle(
            &child,
            graph,
            partitioning.clone(),
            ShuffleConsumption::Multiple,
        )?;
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
