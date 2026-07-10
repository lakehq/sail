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
use crate::proto::{RemoteExecutionCodec, RemoteExecutionCodecConfig};
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: RemoteExecutionCodec,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        let codec = RemoteExecutionCodec::for_driver(RemoteExecutionCodecConfig {
            local_relation_inline_max_bytes: options.artifact_inline_max_bytes,
            local_relation_store_uri: options.artifact_store_uri.clone(),
        });
        Self {
            options,
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec,
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
