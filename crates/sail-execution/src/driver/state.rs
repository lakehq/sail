use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::driver::gen;
use crate::error::ExecutionError;
use crate::id::{JobId, TaskId, WorkerId};

pub struct DriverState {
    workers: HashMap<WorkerId, WorkerDescriptor>,
    jobs: HashMap<JobId, JobDescriptor>,
    tasks: HashMap<(TaskId, usize), TaskDescriptor>,
}

impl DriverState {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            tasks: HashMap::new(),
        }
    }

    pub fn add_worker(&mut self, worker_id: WorkerId, descriptor: WorkerDescriptor) {
        self.workers.insert(worker_id, descriptor);
    }

    pub fn add_job(&mut self, job_id: JobId, descriptor: JobDescriptor) {
        self.jobs.insert(job_id, descriptor);
    }

    pub fn add_task(&mut self, task_id: TaskId, partition: usize, job_id: JobId) {
        self.tasks.insert(
            (task_id, partition),
            TaskDescriptor {
                job_id,
                status: TaskStatus::Pending,
            },
        );
    }

    pub fn get_worker(&self, worker_id: &WorkerId) -> Option<&WorkerDescriptor> {
        self.workers.get(worker_id)
    }

    pub fn get_one_pending_task(&self) -> Option<(TaskId, usize)> {
        self.tasks
            .iter()
            .find(|(_, desc)| matches!(desc.status, TaskStatus::Pending))
            .map(|(key, _)| *key)
    }

    pub fn get_task_plan(&self, task_id: TaskId) -> Option<Arc<dyn ExecutionPlan>> {
        self.tasks
            .iter()
            .find(|(key, _)| key.0 == task_id)
            .and_then(|(_, desc)| self.jobs.get(&desc.job_id))
            .map(|desc| desc.plan.clone())
    }

    pub fn update_task(&mut self, task_id: TaskId, partition: usize, status: TaskStatus) {
        use std::collections::hash_map::Entry;
        if let Entry::Occupied(v) = self.tasks.entry((task_id, partition)) {
            *(&mut v.into_mut().status) = status;
        }
    }
}

pub struct WorkerDescriptor {
    pub host: String,
    pub port: u16,
    pub active: bool,
}

pub struct JobDescriptor {
    pub plan: Arc<dyn ExecutionPlan>,
    pub task_id: TaskId,
}

pub struct TaskDescriptor {
    pub job_id: JobId,
    pub status: TaskStatus,
}

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
