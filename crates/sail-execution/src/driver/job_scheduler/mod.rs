mod core;
mod options;
mod state;

use std::collections::{HashMap, VecDeque};

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub use options::JobSchedulerOptions;

use crate::codec::RemoteExecutionCodec;
use crate::driver::job_scheduler::state::JobDescriptor;
use crate::id::{IdGenerator, JobId, TaskKey};

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: HashMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    /// The queue of jobs that need to be scheduled.
    job_queue: VecDeque<JobId>,
    physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        Self {
            options,
            jobs: HashMap::new(),
            job_id_generator: IdGenerator::new(),
            job_queue: VecDeque::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec),
        }
    }
}

pub enum JobSchedulerAction {
    ScheduleTasks { tasks: Vec<Vec<TaskKey>> },
    CancelTasks { tasks: Vec<TaskKey> },
    FailJob { job_id: JobId },
}
