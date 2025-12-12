use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use log::warn;
use tokio::time::Instant;

use crate::driver::gen;
use crate::error::ExecutionResult;
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};
use crate::stream::channel::ChannelName;

#[derive(Debug)]
pub struct DriverState {
    workers: HashMap<WorkerId, WorkerDescriptor>,
    jobs: HashMap<JobId, JobDescriptor>,
    tasks: HashMap<TaskId, TaskDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    task_id_generator: IdGenerator<TaskId>,
    worker_id_generator: IdGenerator<WorkerId>,
}

impl DriverState {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            tasks: HashMap::new(),
            job_id_generator: IdGenerator::new(),
            task_id_generator: IdGenerator::new(),
            worker_id_generator: IdGenerator::new(),
        }
    }

    pub fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn next_task_id(&mut self) -> ExecutionResult<TaskId> {
        self.task_id_generator.next()
    }

    pub fn next_worker_id(&mut self) -> ExecutionResult<WorkerId> {
        self.worker_id_generator.next()
    }

    pub fn add_worker(&mut self, worker_id: WorkerId, descriptor: WorkerDescriptor) {
        self.workers.insert(worker_id, descriptor);
    }

    pub fn get_worker(&self, worker_id: WorkerId) -> Option<&WorkerDescriptor> {
        self.workers.get(&worker_id)
    }

    pub fn list_workers(&self) -> Vec<(WorkerId, &WorkerDescriptor)> {
        self.workers
            .iter()
            .map(|(&worker_id, worker)| (worker_id, worker))
            .collect()
    }

    pub fn update_worker(
        &mut self,
        worker_id: WorkerId,
        state: WorkerState,
        message: Option<String>,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        if let Some(message) = message {
            worker.messages.push(message);
        }
        worker.state = state;
    }

    pub fn record_worker_heartbeat(&mut self, worker_id: WorkerId) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        if let WorkerState::Running { heartbeat_at, .. } = &mut worker.state {
            *heartbeat_at = Instant::now();
        }
    }

    pub fn add_job(&mut self, job_id: JobId, descriptor: JobDescriptor) {
        self.jobs.insert(job_id, descriptor);
    }

    pub fn get_job(&self, job_id: JobId) -> Option<&JobDescriptor> {
        self.jobs.get(&job_id)
    }

    pub fn add_task(&mut self, task_id: TaskId, descriptor: TaskDescriptor) {
        self.tasks.insert(task_id, descriptor);
    }

    pub fn get_task(&self, task_id: TaskId) -> Option<&TaskDescriptor> {
        self.tasks.get(&task_id)
    }

    pub fn can_schedule_task(&self, task_id: TaskId) -> bool {
        let Some(task) = self.tasks.get(&task_id) else {
            return false;
        };
        let Some(job) = self.jobs.get(&task.job_id) else {
            return false;
        };
        job.stages.iter().take(task.stage).all(|stage| {
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

    pub fn attach_task_to_worker(&mut self, task_id: TaskId) {
        let Some(task) = self.tasks.get(&task_id) else {
            warn!("task {task_id} not found");
            return;
        };
        let Some(worker_id) = task.state.worker_id() else {
            warn!("task {task_id} is not assigned to any worker");
            return;
        };
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match &mut worker.state {
            WorkerState::Running {
                tasks,
                jobs,
                updated_at,
                ..
            } => {
                tasks.insert(task_id);
                jobs.insert(task.job_id);
                *updated_at = Instant::now();
            }
            _ => {
                warn!("cannot assign task {task_id} to worker {worker_id} that is not running");
            }
        }
    }

    pub fn detach_task_from_worker(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
    ) -> Option<WorkerId> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return None;
        };
        match &mut worker.state {
            WorkerState::Running {
                tasks, updated_at, ..
            } => {
                tasks.remove(&task_id);
                *updated_at = Instant::now();
            }
            _ => {
                warn!("cannot unassign task {task_id} from worker {worker_id} that is not running");
                return None;
            }
        };
        Some(worker_id)
    }

    pub fn detach_job_from_workers(&mut self, job_id: JobId) -> Vec<WorkerId> {
        let mut out = vec![];
        for (&worker_id, worker) in self.workers.iter_mut() {
            if let WorkerState::Running {
                jobs, updated_at, ..
            } = &mut worker.state
            {
                jobs.remove(&job_id);
                *updated_at = Instant::now();
                out.push(worker_id);
            }
        }
        out
    }

    pub fn find_tasks_for_worker(&self, worker_id: WorkerId) -> Vec<(TaskId, &TaskDescriptor)> {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return vec![];
        };
        match &worker.state {
            WorkerState::Running { tasks, .. } => tasks
                .iter()
                .filter_map(|task_id| self.tasks.get(task_id).map(|task| (*task_id, task)))
                .collect(),
            _ => vec![],
        }
    }

    pub fn update_task(
        &mut self,
        task_id: TaskId,
        attempt: usize,
        state: TaskState,
        message: Option<String>,
    ) {
        let Some(task) = self.tasks.get_mut(&task_id) else {
            warn!("task {task_id} not found");
            return;
        };
        if task.attempt != attempt {
            warn!("task {task_id} attempt {attempt} is stale");
            return;
        }
        if let Some(message) = message {
            task.messages.push(message);
        }
        task.state = state;
    }

    pub fn find_active_tasks_for_job(&self, job_id: JobId) -> Vec<(TaskId, &TaskDescriptor)> {
        let Some(job) = self.jobs.get(&job_id) else {
            return vec![];
        };
        job.stages
            .iter()
            .flat_map(|stage| {
                stage
                    .tasks
                    .iter()
                    .filter_map(|task_id| self.tasks.get(task_id).map(|task| (*task_id, task)))
                    .filter(|(_, task)| {
                        matches!(
                            task.state,
                            TaskState::Scheduled { .. } | TaskState::Running { .. }
                        )
                    })
            })
            .collect()
    }

    pub fn count_active_workers(&self) -> usize {
        self.workers
            .values()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerState::Pending | WorkerState::Running { .. }
                )
            })
            .count()
    }

    pub fn count_active_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| {
                matches!(
                    task.state,
                    TaskState::Scheduled { .. } | TaskState::Running { .. }
                )
            })
            .count()
    }

    pub fn count_pending_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| matches!(task.state, TaskState::Created | TaskState::Pending))
            .count()
    }
}

#[derive(Debug)]
pub struct WorkerDescriptor {
    pub state: WorkerState,
    pub messages: Vec<String>,
}

#[derive(Debug)]
pub enum WorkerState {
    Pending,
    Running {
        host: String,
        port: u16,
        /// The tasks that are running on the worker.
        tasks: HashSet<TaskId>,
        /// The jobs that depend on the worker.
        /// This is used to support a naive version of the Spark "shuffle tracking" mechanism.
        /// A job depends on a worker if the tasks of the job are running on the worker,
        /// or if the worker owns a channel for the job output.
        /// The worker needs to be running for shuffle stream or job output stream consumption.
        jobs: HashSet<JobId>,
        updated_at: Instant,
        heartbeat_at: Instant,
    },
    Stopped,
    Failed,
}

#[derive(Debug)]
pub struct JobDescriptor {
    pub stages: Vec<JobStage>,
}

#[derive(Debug)]
pub struct JobStage {
    pub plan: Arc<dyn ExecutionPlan>,
    /// A list of task IDs for each partition of the stage.
    pub tasks: Vec<TaskId>,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub job_id: JobId,
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

/// The observed task status that drives the task state transition.
#[derive(Debug, Clone, Copy)]
pub enum TaskStatus {
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::Running => write!(f, "RUNNING"),
            TaskStatus::Succeeded => write!(f, "SUCCEEDED"),
            TaskStatus::Failed => write!(f, "FAILED"),
            TaskStatus::Canceled => write!(f, "CANCELED"),
        }
    }
}

impl From<gen::TaskStatus> for TaskStatus {
    fn from(value: gen::TaskStatus) -> Self {
        match value {
            gen::TaskStatus::Running => Self::Running,
            gen::TaskStatus::Succeeded => Self::Succeeded,
            gen::TaskStatus::Failed => Self::Failed,
            gen::TaskStatus::Canceled => Self::Canceled,
        }
    }
}

impl From<TaskStatus> for gen::TaskStatus {
    fn from(value: TaskStatus) -> Self {
        match value {
            TaskStatus::Running => gen::TaskStatus::Running,
            TaskStatus::Succeeded => gen::TaskStatus::Succeeded,
            TaskStatus::Failed => gen::TaskStatus::Failed,
            TaskStatus::Canceled => gen::TaskStatus::Canceled,
        }
    }
}
