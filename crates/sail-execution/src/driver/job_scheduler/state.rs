use datafusion::physical_plan::ExecutionPlanProperties;

use crate::driver::output::JobOutputHandle;
use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::job_graph::JobGraph;

pub struct JobDescriptor {
    pub graph: JobGraph,
    pub stages: Vec<StageDescriptor>,
    pub output: JobOutputHandle,
}

impl JobDescriptor {
    pub fn try_new(graph: JobGraph, output: JobOutputHandle) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for (_, stage) in graph.stages().iter().enumerate() {
            let mut descriptor = StageDescriptor { tasks: vec![] };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        Ok(Self {
            graph,
            stages,
            output,
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
}

#[derive(Debug, Clone)]
pub enum TaskState {
    /// The task has been created, but may not be eligible for scheduling.
    Created,
    /// The task is eligible for scheduling, but is not assigned to any worker.
    Pending,
    /// The task is scheduled to a worker, but its status is unknown.
    Scheduled {
        /// The worker ID to schedule the current task attempt.
        worker_id: WorkerId,
    },
    /// The task is running on a worker.
    Running {
        /// The worker ID for running the current task attempt.
        worker_id: WorkerId,
    },
    /// The task has succeeded.
    Succeeded {
        /// The worker ID for the task output.
        worker_id: WorkerId,
    },
    /// The task has failed.
    Failed,
    /// The task has been canceled.
    Canceled,
}

impl TaskState {
    pub fn pending(&self) -> Option<TaskState> {
        match self {
            TaskState::Created => Some(TaskState::Pending),
            TaskState::Pending => Some(TaskState::Pending),
            TaskState::Scheduled { .. } => None,
            TaskState::Running { .. } => None,
            TaskState::Succeeded { .. } => None,
            TaskState::Failed => None,
            TaskState::Canceled => None,
        }
    }

    pub fn run(&self) -> Option<TaskState> {
        match self {
            TaskState::Created => None,
            TaskState::Pending => None,
            TaskState::Scheduled { worker_id } => Some(TaskState::Running {
                worker_id: *worker_id,
            }),
            TaskState::Running { .. } => Some(self.clone()),
            TaskState::Succeeded { .. } => None,
            TaskState::Failed => None,
            TaskState::Canceled => None,
        }
    }

    pub fn succeed(&self) -> Option<TaskState> {
        match self {
            TaskState::Created => None,
            TaskState::Pending => None,
            TaskState::Scheduled { worker_id } | TaskState::Running { worker_id } => {
                Some(TaskState::Succeeded {
                    worker_id: *worker_id,
                })
            }
            TaskState::Succeeded { .. } => Some(self.clone()),
            TaskState::Failed => None,
            TaskState::Canceled => None,
        }
    }
}

impl TaskState {
    pub fn worker_id(&self) -> Option<WorkerId> {
        match self {
            TaskState::Scheduled { worker_id }
            | TaskState::Running { worker_id }
            | TaskState::Succeeded { worker_id } => Some(*worker_id),
            _ => None,
        }
    }
}
