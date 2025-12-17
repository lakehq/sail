use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::driver::planner::JobGraph;
use crate::error::ExecutionResult;
use crate::id::{IdGenerator, JobId, TaskId, TaskInstance, WorkerId};
use crate::stream::channel::ChannelName;

pub struct JobDescriptor {
    pub stages: Vec<JobStage>,
    pub tasks: HashMap<TaskId, TaskDescriptor>,
}

impl JobDescriptor {
    pub fn try_new(job_id: JobId, graph: JobGraph) -> ExecutionResult<Self> {
        let mut stages = vec![];
        let mut tasks = HashMap::new();
        let mut task_id_generator = IdGenerator::new();
        for (s, plan) in graph.stages().iter().enumerate() {
            let last = s == graph.stages().len() - 1;
            let mut stage = JobStage {
                plan: Arc::clone(plan),
                tasks: vec![],
            };
            for p in 0..plan.output_partitioning().partition_count() {
                let task_id = task_id_generator.next()?;
                let attempt = 0;
                let channel = if last {
                    Some(format!("job-{job_id}/task-{task_id}/attempt-{attempt}").into())
                } else {
                    None
                };
                tasks.insert(
                    task_id,
                    TaskDescriptor {
                        stage: s,
                        partition: p,
                        attempt,
                        mode: TaskMode::Pipelined,
                        state: TaskState::Created,
                        messages: vec![],
                        channel,
                    },
                );
                stage.tasks.push(task_id);
            }
            stages.push(stage);
        }
        Ok(Self { stages, tasks })
    }

    pub fn can_schedule_task(&self, task_id: TaskId) -> bool {
        let Some(task) = self.tasks.get(&task_id) else {
            return false;
        };
        self.stages.iter().take(task.stage).all(|stage| {
            stage.tasks.iter().all(|&task_id| {
                self.tasks.get(&task_id).is_some_and(|task| {
                    matches!(
                        task.state,
                        TaskState::Running { .. } | TaskState::Succeeded { .. }
                    )
                })
            })
        })
    }

    pub fn cancel_active_tasks(
        &mut self,
        job_id: JobId,
        reason: Option<String>,
    ) -> Vec<TaskInstance> {
        let mut result = vec![];
        for stage in self.stages.iter_mut() {
            for task_id in stage.tasks.iter_mut() {
                if let Some(task) = self.tasks.get_mut(task_id) {
                    match task.state {
                        TaskState::Scheduled { .. } | TaskState::Running { .. } => {
                            task.state = TaskState::Canceled;
                            if let Some(reason) = &reason {
                                task.messages.push(reason.clone());
                            }
                            result.push(TaskInstance {
                                job_id,
                                task_id: *task_id,
                                attempt: task.attempt,
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        result
    }
}

#[derive(Debug)]
pub struct JobStage {
    pub plan: Arc<dyn ExecutionPlan>,
    /// A list of task IDs for each partition of the stage.
    pub tasks: Vec<TaskId>,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
    #[allow(dead_code)]
    pub mode: TaskMode,
    pub state: TaskState,
    pub messages: Vec<String>,
    /// An optional channel for writing task output.
    /// This is used for sending the last stage output
    /// from the worker to the driver.
    pub channel: Option<ChannelName>,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskMode {
    #[allow(dead_code)]
    Blocking,
    Pipelined,
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
