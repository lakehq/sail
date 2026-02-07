use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::session::job::{JobSnapshot, StageSnapshot, TaskSnapshot};

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::JobOutputManager;
use crate::error::ExecutionResult;
use crate::id::JobId;
use crate::job_graph::JobGraph;

pub struct JobDescriptor {
    pub graph: JobGraph,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub regions: Vec<TaskRegionDescriptor>,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

pub enum JobState {
    Running {
        output: JobOutputManager,
        context: Arc<TaskContext>,
    },
    Draining,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
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
    pub fn try_new(graph: JobGraph, state: JobState) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for stage in graph.stages().iter() {
            let mut descriptor = StageDescriptor {
                tasks: vec![],
                state: StageState::Active,
                created_at: Utc::now(),
                stopped_at: None,
            };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        let topology = JobTopology::try_new(&graph)?;
        let regions = (0..topology.regions.len())
            .map(|_| TaskRegionDescriptor {
                state: TaskRegionState::Running,
            })
            .collect();
        Ok(Self {
            graph,
            topology,
            stages,
            regions,
            state,
            created_at: Utc::now(),
            stopped_at: None,
        })
    }

    pub fn job_snapshot(&self, job_id: JobId) -> JobSnapshot {
        JobSnapshot {
            job_id: job_id.into(),
            status: self.state.status().to_string(),
            created_at: self.created_at,
            stopped_at: self.stopped_at,
        }
    }

    pub fn stage_snapshots(&self, job_id: JobId) -> Vec<StageSnapshot> {
        self.graph
            .stages()
            .iter()
            .zip(self.stages.iter())
            .enumerate()
            .map(|(s, (stage, descriptor))| {
                let inputs = stage
                    .inputs
                    .iter()
                    .map(|x| sail_common_datafusion::system::types::StageInput {
                        stage: x.stage as u64,
                        mode: x.mode.to_string(),
                    })
                    .collect();
                StageSnapshot {
                    job_id: job_id.into(),
                    stage: s as u64,
                    partitions: stage.plan.output_partitioning().partition_count() as u64,
                    inputs,
                    group: stage.group.clone(),
                    mode: stage.mode.to_string(),
                    distribution: stage.distribution.to_string(),
                    placement: stage.placement.to_string(),
                    status: descriptor.state.status().to_string(),
                    created_at: descriptor.created_at,
                    stopped_at: descriptor.stopped_at,
                }
            })
            .collect()
    }

    pub fn task_snapshots(&self, job_id: JobId) -> Vec<TaskSnapshot> {
        self.stages
            .iter()
            .enumerate()
            .flat_map(|(s, stage)| {
                stage.tasks.iter().enumerate().flat_map(move |(p, task)| {
                    task.attempts
                        .iter()
                        .enumerate()
                        .map(move |(a, attempt)| TaskSnapshot {
                            job_id: job_id.into(),
                            stage: s as u64,
                            partition: p as u64,
                            attempt: a as u64,
                            status: attempt.state.status().to_string(),
                            created_at: attempt.created_at,
                            stopped_at: attempt.stopped_at,
                        })
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
    pub state: StageState,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
pub enum StageState {
    /// The tasks in the stage are not yet completed,
    /// or the task streams are still being consumed.
    Active,
    /// The tasks in the stage will not be scheduled anymore,
    /// and the task streams are no longer being consumed.
    Inactive,
}

impl StageState {
    pub fn status(&self) -> &'static str {
        match self {
            StageState::Active => "ACTIVE",
            StageState::Inactive => "INACTIVE",
        }
    }
}

#[derive(Debug)]
pub struct TaskRegionDescriptor {
    pub state: TaskRegionState,
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

#[derive(Debug)]
pub struct TaskAttemptDescriptor {
    pub state: TaskState,
    pub messages: Vec<String>,
    pub cause: Option<CommonErrorCause>,
    /// Whether the task streams are fetched for the job output.
    /// This will always be false if the task does not belong to
    /// the final stages of the job.
    pub job_output_fetched: bool,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
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
