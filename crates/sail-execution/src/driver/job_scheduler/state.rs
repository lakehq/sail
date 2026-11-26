// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::{JobOutputManager, JobOutputOutcome};
use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{JobGraph, OutputDistribution, OutputMode, Stage};
use crate::proto::{
    RemoteExecutionCodec, encode_remote_partitioning, encode_remote_physical_expr,
    encode_remote_physical_plan,
};
use crate::task::definition::{TaskOutputDistribution, TaskOutputLocator};

/// The parts of a task definition that are independent of task assignments.
#[derive(Debug)]
pub struct EncodedStage {
    pub plan: Arc<[u8]>,
    pub distribution: TaskOutputDistribution,
}

impl EncodedStage {
    fn try_new(stage: &Stage) -> ExecutionResult<Self> {
        let plan = encode_remote_physical_plan(&RemoteExecutionCodec, stage.plan.clone())?.into();
        let distribution = match &stage.distribution {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|expr| {
                        let expr = encode_remote_physical_expr(&RemoteExecutionCodec, expr)?;
                        Ok(Arc::from(expr))
                    })
                    .collect::<ExecutionResult<Vec<Arc<[u8]>>>>()?;
                TaskOutputDistribution::Hash {
                    keys,
                    channels: *channels,
                }
            }
            OutputDistribution::Range {
                partitioning,
                channels,
            } => {
                let channels = *channels;
                let partitioning = encode_remote_partitioning(
                    &RemoteExecutionCodec,
                    &Partitioning::Range(partitioning.clone()),
                )?;
                TaskOutputDistribution::Range {
                    partitioning: Arc::from(partitioning),
                    channels,
                }
            }
            OutputDistribution::RoundRobinBatch { channels } => {
                TaskOutputDistribution::RoundRobinBatch {
                    channels: *channels,
                }
            }
            OutputDistribution::RoundRobinRow { channels } => {
                TaskOutputDistribution::RoundRobinRow {
                    channels: *channels,
                }
            }
            OutputDistribution::Channel { channels } => TaskOutputDistribution::Channel {
                channels: *channels,
            },
        };
        Ok(Self { plan, distribution })
    }
}

/// A graph and its task dependencies, built together on the compute runtime.
#[derive(Debug)]
pub struct PlannedJob {
    pub graph: Arc<JobGraph>,
    pub topology: JobTopology,
    stages: Vec<EncodedStage>,
}

impl TryFrom<JobGraph> for PlannedJob {
    type Error = ExecutionError;

    fn try_from(graph: JobGraph) -> ExecutionResult<Self> {
        let topology = JobTopology::try_new(&graph)?;
        let stages = graph
            .stages()
            .iter()
            .map(EncodedStage::try_new)
            .collect::<ExecutionResult<_>>()?;
        Ok(Self {
            graph: Arc::new(graph),
            topology,
            stages,
        })
    }
}

#[derive(Debug)]
pub struct RangeShuffleUpdate {
    graph: Arc<JobGraph>,
    topology: JobTopology,
    changed: Vec<(usize, EncodedStage)>,
}

impl RangeShuffleUpdate {
    pub fn try_new(graph: &JobGraph, shuffle: usize, partitions: usize) -> ExecutionResult<Self> {
        let mut graph = graph.clone();
        let changed = graph.resize_range_shuffle(shuffle, partitions)?;
        let topology = JobTopology::try_new(&graph)?;
        let changed = changed
            .into_iter()
            .map(|s| Ok((s, EncodedStage::try_new(&graph.stages()[s])?)))
            .collect::<ExecutionResult<_>>()?;
        Ok(Self {
            graph: Arc::new(graph),
            topology,
            changed,
        })
    }
}

/// Tracks graph/topology and runtime state for a single job.
pub struct JobDescriptor {
    pub context: Arc<TaskContext>,
    pub graph: Arc<JobGraph>,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub regions: Vec<TaskRegionDescriptor>,
    /// The state of each range shuffle of the job graph.
    pub range_shuffles: Vec<RangeShuffleState>,
    pub state: JobState,
}

/// How far the driver is in finding the partitions of a range shuffle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeShuffleState {
    /// The split points are not picked yet.
    Sampling,
    /// The driver reads the split points.
    Reading,
    /// The shuffle and the stages that read it have their partitions.
    Resolved,
}

pub enum JobState {
    Running { output: JobOutputManager },
    Draining,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    pub fn finish_output(&mut self, outcome: JobOutputOutcome) {
        match self {
            JobState::Running { .. } | JobState::Draining => {
                *self = match outcome {
                    JobOutputOutcome::Completed => JobState::Succeeded,
                    JobOutputOutcome::Failed => JobState::Failed,
                    JobOutputOutcome::Canceled => JobState::Canceled,
                };
            }
            JobState::Succeeded | JobState::Failed | JobState::Canceled => {}
        }
    }

    pub fn status(&self) -> &'static str {
        match self {
            JobState::Running { .. } => "RUNNING",
            JobState::Draining => "DRAINING",
            JobState::Succeeded => "SUCCEEDED",
            JobState::Failed => "FAILED",
            JobState::Canceled => "CANCELED",
        }
    }
}

impl JobDescriptor {
    pub fn new(plan: PlannedJob, state: JobState, context: Arc<TaskContext>) -> Self {
        let PlannedJob {
            graph,
            topology,
            stages,
        } = plan;
        let stages = graph
            .stages()
            .iter()
            .zip(stages)
            .map(|(stage, definition)| StageDescriptor {
                tasks: TaskDescriptor::new_tasks(
                    stage.plan.output_partitioning().partition_count(),
                ),
                state: StageState::Active(definition),
            })
            .collect();
        let regions = TaskRegionDescriptor::new_regions(&topology);
        let range_shuffles = vec![RangeShuffleState::Sampling; graph.range_shuffles().len()];
        Self {
            context,
            graph,
            topology,
            stages,
            regions,
            range_shuffles,
            state,
        }
    }

    /// Gives the range shuffle its number of partitions, and the stages that read it
    /// theirs, which have no task attempts yet. The task regions are found again for the
    /// new partitions. Their states follow from the task attempts.
    pub fn resolve_range_shuffle(
        &mut self,
        shuffle: usize,
        update: RangeShuffleUpdate,
    ) -> ExecutionResult<Vec<usize>> {
        let RangeShuffleUpdate {
            graph,
            topology,
            changed,
        } = update;
        let mut updated = Vec::with_capacity(changed.len());
        for (s, definition) in changed {
            let stage = &mut self.stages[s];
            if stage.tasks.iter().any(|task| !task.attempts.is_empty()) {
                return Err(ExecutionError::InternalError(format!(
                    "stage {s} has run before the partitions of range shuffle {shuffle} are known"
                )));
            }
            stage.state = StageState::Active(definition);
            updated.push(s);
            let partitions = graph.stages()[s]
                .plan
                .output_partitioning()
                .partition_count();
            if stage.tasks.len() != partitions {
                stage.tasks = TaskDescriptor::new_tasks(partitions);
            }
        }
        self.graph = graph;
        self.topology = topology;
        self.regions = TaskRegionDescriptor::new_regions(&self.topology);
        self.range_shuffles[shuffle] = RangeShuffleState::Resolved;
        Ok(updated)
    }

    pub fn output_locator(&self, stage: usize) -> TaskOutputLocator {
        match self.graph.stages()[stage].mode {
            OutputMode::Pipelined => TaskOutputLocator::Pipelined {
                replicas: self.graph.replicas(stage),
                bounded: self.graph.bounded_output(stage),
            },
            OutputMode::Blocking => TaskOutputLocator::Blocking,
        }
    }

    /// Whether the stage waits for the partitions of a range shuffle.
    pub fn waits_for_range_shuffle(&self, stage: usize) -> bool {
        self.range_shuffles
            .iter()
            .enumerate()
            .any(|(shuffle, state)| {
                *state != RangeShuffleState::Resolved
                    && self.graph.waits_for_range_shuffle(stage, shuffle)
            })
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
    pub state: StageState,
}

#[derive(Debug)]
pub enum StageState {
    /// The tasks in the stage are not yet completed,
    /// or the task streams are still being consumed.
    Active(EncodedStage),
    /// The tasks in the stage will not be scheduled anymore,
    /// and the task streams are no longer being consumed.
    Inactive,
}

impl StageState {
    pub fn status(&self) -> &'static str {
        match self {
            StageState::Active(_) => "ACTIVE",
            StageState::Inactive => "INACTIVE",
        }
    }
}

#[derive(Debug)]
pub struct TaskRegionDescriptor {
    pub state: TaskRegionState,
}

impl TaskRegionDescriptor {
    fn new_regions(topology: &JobTopology) -> Vec<Self> {
        (0..topology.regions.len())
            .map(|_| Self {
                state: TaskRegionState::Running,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TaskRegionState {
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub attempts: Vec<TaskAttemptDescriptor>,
}

impl TaskDescriptor {
    fn new_tasks(partitions: usize) -> Vec<Self> {
        (0..partitions).map(|_| Self { attempts: vec![] }).collect()
    }
}

#[derive(Debug)]
pub struct TaskAttemptDescriptor {
    pub state: TaskState,
    pub messages: Vec<String>,
    pub cause: Option<CommonErrorCause>,
    /// Whether the task streams are fetched for the job output.
    /// This will always be false if the task does not belong to
    /// the final stages of the job.
    pub job_output_fetched: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    /// The task attempt has been created, but is not assigned to any worker.
    /// A task attempt is only created when the task region is eligible for scheduling.
    Created,
    /// The task attempt is scheduled to a worker, but its status is unknown.
    Scheduled,
    /// The task attempt is running on a worker.
    Running,
    /// The task attempt has succeeded.
    Succeeded,
    /// The task attempt has failed.
    Failed,
    /// The task attempt has been canceled.
    Canceled,
}

impl TaskState {
    pub fn status(&self) -> &'static str {
        match self {
            TaskState::Created => "CREATED",
            TaskState::Scheduled => "SCHEDULED",
            TaskState::Running => "RUNNING",
            TaskState::Succeeded => "SUCCEEDED",
            TaskState::Failed => "FAILED",
            TaskState::Canceled => "CANCELED",
        }
    }

    pub fn consolidate(&self, next: Self) -> Self {
        match (self, next) {
            (TaskState::Created, x) => x,
            (TaskState::Scheduled, TaskState::Created) => *self,
            (TaskState::Scheduled, x) => x,
            (TaskState::Running, TaskState::Created | TaskState::Scheduled) => *self,
            (TaskState::Running, x) => x,
            (TaskState::Succeeded | TaskState::Failed | TaskState::Canceled, _) => *self,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            TaskState::Succeeded | TaskState::Failed | TaskState::Canceled => true,
            TaskState::Created | TaskState::Scheduled | TaskState::Running => false,
        }
    }
}
