use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use tokio::sync::mpsc;

use crate::driver::output::JobOutputCommand;
use crate::job_graph::JobGraph;

pub struct JobDescriptor {
    pub graph: JobGraph,
    pub stages: Vec<StageDescriptor>,
    pub state: JobState,
}

pub enum JobState {
    Running {
        output: mpsc::Sender<JobOutputCommand>,
        context: Arc<TaskContext>,
    },
    Succeeded,
    Failed,
    Canceled,
}

impl JobDescriptor {
    pub fn new(graph: JobGraph, state: JobState) -> Self {
        let mut stages = vec![];
        for (_, stage) in graph.stages().iter().enumerate() {
            let mut descriptor = StageDescriptor { tasks: vec![] };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        Self {
            graph,
            stages,
            state,
        }
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

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    /// The task has been created, but may not be eligible for scheduling.
    Created,
    /// The task is eligible for scheduling, but is not assigned to any worker.
    Pending,
    /// The task is scheduled to a worker, but its status is unknown.
    Scheduled,
    /// The task is running on a worker.
    Running,
    /// The task has succeeded.
    Succeeded,
    /// The task has failed.
    Failed,
    /// The task has been canceled.
    Canceled,
}

impl TaskState {
    pub fn consolidate(&self, next: Self) -> Self {
        match (self, next) {
            (TaskState::Created, x) => x,
            (TaskState::Pending, TaskState::Created) => *self,
            (TaskState::Pending, x) => x,
            (TaskState::Scheduled, TaskState::Created | TaskState::Pending) => *self,
            (TaskState::Scheduled, x) => x,
            (
                TaskState::Running,
                TaskState::Created | TaskState::Pending | TaskState::Scheduled,
            ) => *self,
            (TaskState::Running, x) => x,
            (TaskState::Succeeded | TaskState::Failed | TaskState::Canceled, _) => *self,
        }
    }
}
