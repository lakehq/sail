use std::collections::{HashMap, HashSet, VecDeque};

use datafusion::physical_plan::ExecutionPlanProperties;
use indexmap::IndexSet;

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{InputMode, JobGraph, OutputMode};

#[derive(Debug)]
#[readonly::make]
pub struct JobTopology {
    pub regions: Vec<TaskRegionTopology>,
    pub stages: Vec<StageTopology>,
}

#[derive(Debug)]
pub struct TaskRegionTopology {
    pub tasks: Vec<TaskTopology>,
    /// A set of regions that this region depends on.
    /// The indices refer to the indices of regions in [`JobTopology`].
    pub dependencies: IndexSet<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskTopology {
    pub stage: usize,
    pub partition: usize,
}

#[derive(Debug)]
pub struct StageTopology {
    /// A list of stages that consume the output of this stage.
    /// The indices refer to the indices of stages in [`JobTopology`],
    /// which is the same as the indices of stages in [`JobGraph`].
    pub consumers: Vec<usize>,
}

impl JobTopology {
    /// Groups pipelined stages into components and builds the topology of task regions and stages.
    pub fn try_new(graph: &JobGraph) -> ExecutionResult<Self> {
        let mut stages = (0..graph.stages().len())
            .map(|_| StageTopology { consumers: vec![] })
            .collect::<Vec<_>>();

        let mut pipelined_adjacency = vec![vec![]; graph.stages().len()];

        for (s, stage) in graph.stages().iter().enumerate() {
            for input in &stage.inputs {
                stages[input.stage].consumers.push(s);
                if matches!(&graph.stages()[input.stage].mode, OutputMode::Pipelined) {
                    pipelined_adjacency[s].push(input.stage);
                    pipelined_adjacency[input.stage].push(s);
                }
            }
        }

        // validate final stages (stages that are not consumed by any other stage)
        for (s, stage) in stages.iter().enumerate() {
            if stage.consumers.is_empty()
                && graph.stages()[s].plan.schema().as_ref() != graph.schema().as_ref()
            {
                return Err(ExecutionError::InternalError(
                    "the job graph must have final stages with the same schema".to_string(),
                ));
            }
        }

        // find pipelined components
        let mut visited = vec![false; stages.len()];
        let mut components = vec![];
        for s in 0..stages.len() {
            if !visited[s] {
                let mut component = vec![];
                let mut queue = VecDeque::new();
                queue.push_back(s);
                visited[s] = true;
                while let Some(u) = queue.pop_front() {
                    component.push(u);
                    if let Some(neighbors) = pipelined_adjacency.get(u) {
                        for &v in neighbors {
                            if !visited[v] {
                                visited[v] = true;
                                queue.push_back(v);
                            }
                        }
                    }
                }
                components.push(component);
            }
        }

        let mut regions = vec![];

        for component in components {
            // check if all inputs within component are forward inputs
            let mut all_forward = true;
            for &u in &component {
                for input in &graph.stages()[u].inputs {
                    if component.contains(&input.stage) && !matches!(input.mode, InputMode::Forward)
                    {
                        all_forward = false;
                        break;
                    }
                }
                if !all_forward {
                    break;
                }
            }

            if all_forward {
                // create regions by "slicing" the stages by partition
                let partitions = component
                    .iter()
                    .map(|c| {
                        graph.stages()[*c]
                            .plan
                            .output_partitioning()
                            .partition_count()
                    })
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let partitions = match partitions.as_slice() {
                    [p] => *p,
                    _ => return Err(ExecutionError::InternalError(
                        "task region with all forward inputs must have the same partition count"
                            .to_string(),
                    )),
                };
                for p in 0..partitions {
                    let mut tasks = vec![];
                    for &s in &component {
                        tasks.push(TaskTopology {
                            stage: s,
                            partition: p,
                        });
                    }
                    regions.push(TaskRegionTopology {
                        tasks,
                        dependencies: IndexSet::new(),
                    });
                }
            } else {
                // create a single region consisting of all partitions of the stages
                let mut tasks = vec![];
                for &s in &component {
                    let partitions = graph.stages()[s]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    for p in 0..partitions {
                        tasks.push(TaskTopology {
                            stage: s,
                            partition: p,
                        });
                    }
                }
                regions.push(TaskRegionTopology {
                    tasks,
                    dependencies: IndexSet::new(),
                });
            }
        }

        // build region dependencies
        let mut task_to_region = HashMap::new();
        for (r, region) in regions.iter().enumerate() {
            for t in &region.tasks {
                task_to_region.insert(t.clone(), r);
            }
        }
        for (r, region) in regions.iter_mut().enumerate() {
            for task in &region.tasks {
                for input in &graph.stages()[task.stage].inputs {
                    let input_partitions = graph.stages()[input.stage]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    let consumer_partitions = graph.stages()[task.stage]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    if matches!(input.mode, InputMode::Forward)
                        && input_partitions == consumer_partitions
                    {
                        if let Some(&d) = task_to_region.get(&TaskTopology {
                            stage: input.stage,
                            partition: task.partition,
                        }) && d != r
                        {
                            region.dependencies.insert(d);
                        }
                    } else {
                        // A multi-input plan such as UnionExec maps consumer stage partitions to
                        // child-local partition numbers. When the stage partition counts differ,
                        // that mapping is not represented by StageInput, so conservatively wait
                        // for every producer partition.
                        for p in 0..input_partitions {
                            if let Some(&d) = task_to_region.get(&TaskTopology {
                                stage: input.stage,
                                partition: p,
                            }) && d != r
                            {
                                region.dependencies.insert(d);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self { regions, stages })
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::union::UnionExec;

    use super::JobTopology;
    use crate::job_graph::{JobGraph, JobGraphOptions};
    use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};

    fn repartitioned_input() -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        Arc::new(
            RepartitionExec::try_new(
                Arc::new(EmptyExec::new(schema)),
                Partitioning::RoundRobinBatch(4),
            )
            .unwrap(),
        )
    }

    #[test]
    fn union_stage_waits_for_every_blocking_forward_input() {
        let graph = JobGraph::try_new(
            UnionExec::try_new(vec![repartitioned_input(), repartitioned_input()]).unwrap(),
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Storage {
                    path: None,
                    max_file_size: 1,
                    compression: ShuffleCompression::None,
                },
            },
        )
        .unwrap();
        let topology = JobTopology::try_new(&graph).unwrap();
        let final_stage = graph.stages().len() - 1;
        let blocking_input_stages = graph.stages()[final_stage]
            .inputs
            .iter()
            .map(|input| input.stage)
            .collect::<Vec<_>>();
        assert_eq!(blocking_input_stages.len(), 2);

        let blocking_input_regions = blocking_input_stages
            .iter()
            .map(|stage| {
                topology
                    .regions
                    .iter()
                    .position(|region| region.tasks.iter().any(|task| task.stage == *stage))
                    .expect("blocking input region")
            })
            .collect::<Vec<_>>();

        for region in topology
            .regions
            .iter()
            .filter(|region| region.tasks.iter().any(|task| task.stage == final_stage))
        {
            for input_region in &blocking_input_regions {
                assert!(region.dependencies.contains(input_region));
            }
        }
    }
}
