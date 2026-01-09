use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::JobOutputManager;
use crate::error::ExecutionResult;
use crate::job_graph::JobGraph;

pub struct JobDescriptor {
    pub graph: JobGraph,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub state: JobState,
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

impl JobDescriptor {
    pub fn try_new(graph: JobGraph, state: JobState) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for (_, stage) in graph.stages().iter().enumerate() {
            let mut descriptor = StageDescriptor { tasks: vec![] };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        let topology = JobTopology::try_new(&graph)?;
        Ok(Self {
            graph,
            topology,
            stages,
            state,
        })
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
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

    pub fn is_failure(&self) -> bool {
        match self {
            TaskState::Failed | TaskState::Canceled => true,
            TaskState::Created
            | TaskState::Scheduled
            | TaskState::Running
            | TaskState::Succeeded => false,
        }
    }
}
