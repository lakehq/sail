use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::driver::gen;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};

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

    pub fn get_worker(&self, worker_id: &WorkerId) -> Option<&WorkerDescriptor> {
        self.workers.get(worker_id)
    }

    pub fn get_worker_mut(&mut self, worker_id: &WorkerId) -> Option<&mut WorkerDescriptor> {
        self.workers.get_mut(worker_id)
    }

    pub fn add_job(&mut self, job_id: JobId, descriptor: JobDescriptor) {
        self.jobs.insert(job_id, descriptor);
    }

    pub fn get_job(&self, job_id: JobId) -> ExecutionResult<&JobDescriptor> {
        self.jobs
            .get(&job_id)
            .ok_or_else(|| ExecutionError::InternalError(format!("job not found: {job_id}")))
    }

    pub fn find_job_stage_by_task(&self, task_id: TaskId) -> ExecutionResult<&JobStage> {
        let task = self.get_task(task_id)?;
        let job = self.get_job(task.job_id)?;
        job.stages.get(task.stage).ok_or_else(|| {
            ExecutionError::InternalError(format!("job stage not found for task: {task_id}"))
        })
    }

    pub fn add_task(&mut self, task_id: TaskId, descriptor: TaskDescriptor) {
        self.tasks.insert(task_id, descriptor);
    }

    pub fn get_task(&self, task_id: TaskId) -> ExecutionResult<&TaskDescriptor> {
        self.tasks
            .get(&task_id)
            .ok_or_else(|| ExecutionError::InternalError(format!("task not found: {task_id}")))
    }

    pub fn get_task_mut(&mut self, task_id: TaskId) -> ExecutionResult<&mut TaskDescriptor> {
        self.tasks
            .get_mut(&task_id)
            .ok_or_else(|| ExecutionError::InternalError(format!("task not found: {task_id}")))
    }
}

pub struct WorkerDescriptor {
    pub host: String,
    pub port: u16,
    pub active: bool,
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
    #[allow(dead_code)]
    pub mode: TaskMode,
    pub status: TaskStatus,
    pub messages: Vec<String>,
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
    Finished,
    Failed,
    Canceled,
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
            gen::TaskStatus::Finished => Ok(Self::Finished),
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
            TaskStatus::Finished => gen::TaskStatus::Finished,
            TaskStatus::Failed => gen::TaskStatus::Failed,
            TaskStatus::Canceled => gen::TaskStatus::Canceled,
        }
    }
}
