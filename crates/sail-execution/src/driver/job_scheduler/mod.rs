mod core;
mod options;
mod state;
mod topology;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;
pub use state::TaskState;

use crate::codec::RemoteExecutionCodec;
use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::{JobOutputFailureNotifier, JobOutputSender};
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: Box<dyn PhysicalExtensionCodec>,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        Self {
            options,
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec: Box::new(RemoteExecutionCodec),
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
    FailJobOutput {
        notifier: JobOutputFailureNotifier,
        cause: CommonErrorCause,
    },
    FetchJobOutputStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        sender: JobOutputSender,
    },
    RemoveStreams {
        job_id: JobId,
        stage: Option<usize>,
    },
}
