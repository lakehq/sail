mod core;
mod options;
mod state;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub use options::JobSchedulerOptions;
pub use state::TaskState;

use crate::codec::RemoteExecutionCodec;
use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::task::TaskRegion;
use crate::id::{IdGenerator, JobId, StageKey, TaskKey};

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: HashMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: Box<dyn PhysicalExtensionCodec>,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        Self {
            options,
            jobs: HashMap::new(),
            job_id_generator: IdGenerator::new(),
            codec: Box::new(RemoteExecutionCodec),
        }
    }
}

pub enum JobSchedulerAction {
    ScheduleTasks { region: TaskRegion },
    CancelTasks { tasks: Vec<TaskKey> },
    RemoveLocalStreams { stage: StageKey },
    FailJob { job_id: JobId },
}
