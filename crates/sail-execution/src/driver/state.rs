use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use log::warn;

use crate::driver::gen;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};
use crate::stream::ChannelName;

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

    pub fn update_worker_status(&mut self, worker_id: WorkerId, status: WorkerStatus) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        worker.status = status;
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

    pub fn update_task_status(
        &mut self,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
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
        task.status = status;
    }

    pub fn find_schedulable_tasks_for_job(&self, job_id: JobId) -> Vec<(TaskId, &TaskDescriptor)> {
        let Some(job) = self.jobs.get(&job_id) else {
            return vec![];
        };
        for stage in &job.stages {
            let Some(tasks) = stage
                .tasks
                .iter()
                .map(|task_id| self.tasks.get(task_id).map(|task| (*task_id, task)))
                .collect::<Option<Vec<_>>>()
            else {
                return vec![];
            };
            if tasks
                .iter()
                .all(|(_, task)| matches!(task.status, TaskStatus::Running | TaskStatus::Succeeded))
            {
                continue;
            }
            if tasks
                .iter()
                .any(|(_, task)| matches!(task.status, TaskStatus::Failed | TaskStatus::Canceled))
            {
                return vec![];
            }
            return tasks
                .into_iter()
                .filter(|(_, task)| matches!(task.status, TaskStatus::Pending))
                .collect();
        }
        vec![]
    }

    pub fn find_running_tasks_for_job(&self, job_id: JobId) -> Vec<(TaskId, &TaskDescriptor)> {
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
                    .filter(|(_, task)| matches!(task.status, TaskStatus::Running))
            })
            .collect()
    }

    pub fn find_workers_for_job(&self, job_id: JobId) -> Vec<(WorkerId, &WorkerDescriptor)> {
        self.workers
            .iter()
            .filter(|(_, worker)| worker.job_id == job_id)
            .map(|(&worker_id, worker)| (worker_id, worker))
            .collect()
    }
}

pub struct WorkerDescriptor {
    // TODO: support worker reuse
    pub job_id: JobId,
    pub status: WorkerStatus,
}

pub enum WorkerStatus {
    Pending,
    Running { host: String, port: u16 },
    Stopped,
}

pub struct JobDescriptor {
    pub stages: Vec<JobStage>,
}

pub struct JobStage {
    pub plan: Arc<dyn ExecutionPlan>,
    /// A list of task IDs for each partition of the stage.
    pub tasks: Vec<TaskId>,
}

pub struct TaskDescriptor {
    pub job_id: JobId,
    #[allow(dead_code)]
    pub stage: usize,
    #[allow(dead_code)]
    pub partition: usize,
    pub attempt: usize,
    /// The worker ID for the current task attempt.
    pub worker_id: WorkerId,
    #[allow(dead_code)]
    pub mode: TaskMode,
    pub status: TaskStatus,
    pub messages: Vec<String>,
    /// An optional channel for writing task output.
    /// This is used for sending the last stage output
    /// from the worker to the driver.
    pub channel: Option<ChannelName>,
}

#[derive(Clone, Copy)]
pub enum TaskMode {
    #[allow(dead_code)]
    Blocking,
    Pipelined,
}

#[derive(Clone, Copy)]
pub enum TaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskStatus {
    pub fn completed(&self) -> bool {
        match self {
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled => true,
            TaskStatus::Pending | TaskStatus::Running => false,
        }
    }
}

impl TryFrom<gen::TaskStatus> for TaskStatus {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskStatus) -> Result<Self, Self::Error> {
        match value {
            gen::TaskStatus::Unknown => Err(ExecutionError::InvalidArgument(
                "unknown task status".to_string(),
            )),
            gen::TaskStatus::Pending => Ok(Self::Pending),
            gen::TaskStatus::Running => Ok(Self::Running),
            gen::TaskStatus::Succeeded => Ok(Self::Succeeded),
            gen::TaskStatus::Failed => Ok(Self::Failed),
            gen::TaskStatus::Canceled => Ok(Self::Canceled),
        }
    }
}

impl From<TaskStatus> for gen::TaskStatus {
    fn from(value: TaskStatus) -> Self {
        match value {
            TaskStatus::Pending => gen::TaskStatus::Pending,
            TaskStatus::Running => gen::TaskStatus::Running,
            TaskStatus::Succeeded => gen::TaskStatus::Succeeded,
            TaskStatus::Failed => gen::TaskStatus::Failed,
            TaskStatus::Canceled => gen::TaskStatus::Canceled,
        }
    }
}
