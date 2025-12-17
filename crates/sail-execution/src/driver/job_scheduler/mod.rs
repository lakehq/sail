mod core;
mod options;
mod state;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::ExecutionPlan;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;

use crate::driver::job_scheduler::state::JobDescriptor;
use crate::id::{IdGenerator, JobId, TaskInstance, WorkerId};
use crate::stream::channel::ChannelName;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: HashMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    /// The queue of tasks that need to be scheduled.
    /// A task is enqueued after all its dependencies in the previous job stage.
    task_queue: VecDeque<TaskInstance>,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        Self {
            options,
            jobs: HashMap::new(),
            job_id_generator: IdGenerator::new(),
            task_queue: VecDeque::new(),
        }
    }
}

pub enum TaskTimeout {
    Yes,
    No,
}

pub struct TaskSchedule {
    pub instance: TaskInstance,
    pub worker_id: WorkerId,
    pub plan: TaskSchedulePlan,
    pub partition: usize,
    pub channel: Option<ChannelName>,
}

pub enum TaskSchedulePlan {
    Valid(Arc<dyn ExecutionPlan>),
    Invalid {
        message: String,
        cause: Option<CommonErrorCause>,
    },
}

pub struct JobOutputMetadata {
    pub schema: Arc<Schema>,
    pub channels: Vec<JobOutputChannel>,
}

pub struct JobOutputChannel {
    pub worker_id: WorkerId,
    pub channel: ChannelName,
}
