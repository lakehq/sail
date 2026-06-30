mod core;
mod observer;
mod options;
mod state;
mod topology;

use datafusion::arrow::datatypes::SchemaRef;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;
pub use state::TaskState;

use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::JobOutputHandle;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::proto::codec::RemoteExecutionCodec;
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: RemoteExecutionCodec,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        Self {
            options,
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec: RemoteExecutionCodec::default(),
        }
    }
}

#[derive(Debug)]
pub enum JobAction {
    ScheduleTaskRegion {
        region: TaskRegion,
    },
    CancelTask {
        key: TaskKey,
    },
    ExtendJobOutput {
        handle: JobOutputHandle,
        key: TaskStreamKey,
        schema: SchemaRef,
    },
    FailJobOutput {
        handle: JobOutputHandle,
        cause: CommonErrorCause,
    },
    CleanUpJob {
        job_id: JobId,
        stage: Option<usize>,
    },
}
