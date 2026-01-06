mod core;
mod options;
mod state;

use std::collections::HashMap;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub use options::JobSchedulerOptions;
pub use state::TaskState;

use crate::codec::RemoteExecutionCodec;
use crate::driver::job_scheduler::state::JobDescriptor;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::task::scheduling::TaskRegion;

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

pub enum JobAction {
    ScheduleTasks {
        region: TaskRegion,
    },
    CancelTask {
        key: TaskKey,
    },
    FetchJobOutputStreams {
        keys: Vec<TaskStreamKey>,
        schema: SchemaRef,
    },
    RemoveStreams {
        job_id: JobId,
        stage: Option<usize>,
    },
}
