use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use log::warn;

use crate::driver::gen;
use crate::error::ExecutionResult;
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};
use crate::stream::ChannelName;

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
        sequence: Option<u64>,
    ) {
        let Some(task) = self.tasks.get_mut(&task_id) else {
            warn!("task {task_id} not found");
            return;
        };
        if task.attempt != attempt {
            warn!("task {task_id} attempt {attempt} is stale");
            return;
        }
        if let Some(sequence) = sequence {
            if task.sequence >= sequence {
                warn!("task {task_id} sequence {sequence} is stale");
                return;
            } else {
                task.sequence = sequence;
            }
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
                .filter(|(_, task)| {
                    matches!(task.status, TaskStatus::Created)
                        && self.get_worker(task.worker_id).is_some_and(|worker| {
                            matches!(worker.status, WorkerStatus::Running { .. })
                        })
                })
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

#[derive(Debug)]
pub struct WorkerDescriptor {
    // TODO: support worker reuse
    pub job_id: JobId,
    pub status: WorkerStatus,
}

#[derive(Debug)]
pub enum WorkerStatus {
    Pending,
    Running { host: String, port: u16 },
    Stopped,
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
    /// The sequence number corresponding to the last task status update from the worker.
    // TODO: clear the sequence number when assigning the task to a different worker.
    pub sequence: u64,
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

#[derive(Debug, Clone, Copy)]
pub enum TaskStatus {
    Created,
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskStatus {
    pub fn completed(&self) -> bool {
        match self {
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled => true,
            TaskStatus::Created | TaskStatus::Scheduled | TaskStatus::Running => false,
        }
    }
}

impl From<gen::TaskStatus> for TaskStatus {
    fn from(value: gen::TaskStatus) -> Self {
        match value {
            gen::TaskStatus::Created => Self::Created,
            gen::TaskStatus::Scheduled => Self::Scheduled,
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
            TaskStatus::Created => gen::TaskStatus::Created,
            TaskStatus::Scheduled => gen::TaskStatus::Scheduled,
            TaskStatus::Running => gen::TaskStatus::Running,
            TaskStatus::Succeeded => gen::TaskStatus::Succeeded,
            TaskStatus::Failed => gen::TaskStatus::Failed,
            TaskStatus::Canceled => gen::TaskStatus::Canceled,
        }
    }
}
