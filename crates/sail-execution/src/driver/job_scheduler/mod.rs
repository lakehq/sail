mod core;
mod observer;
mod options;
mod state;
mod topology;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;
pub use state::TaskState;

use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::JobOutputHandle;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::proto::RemoteExecutionCodec;
use crate::task::definition::{TaskOutput, TaskResources};
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: RemoteExecutionCodec,
    stage_task_templates: HashMap<(JobId, usize), StageTaskTemplate>,
}

#[derive(Clone)]
struct StageTaskTemplate {
    plan: Arc<[u8]>,
    resources: TaskResources,
    output: TaskOutput,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions) -> Self {
        let codec = RemoteExecutionCodec::for_driver();
        Self {
            options,
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec,
            stage_task_templates: HashMap::new(),
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
