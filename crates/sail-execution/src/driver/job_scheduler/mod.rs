mod core;
mod observer;
mod options;
mod state;
mod topology;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::cache_manager::CacheId;
use sail_common_datafusion::error::CommonErrorCause;
pub use state::TaskState;
use thiserror::Error;

use crate::codec::RemoteExecutionCodec;
use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::JobOutputHandle;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey, WorkerId};
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
/// Scheduler directive describing the next driver-side action to execute.
pub enum JobAction {
    ScheduleTaskRegion {
        region: TaskRegion,
    },
    FailTasks {
        keys: Vec<TaskKey>,
        error: CachePinError,
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

/// Typed failure reasons when resolving cache-pinned worker placement.
#[derive(Debug, Error)]
pub enum CachePinError {
    #[error("no worker location for cache {cache_id} partition {partition}")]
    MissingLocation { cache_id: CacheId, partition: usize },
    #[error(
        "cache {cache_id} partition {partition} is located on driver but task requires a worker"
    )]
    DriverLocationForWorkerTask { cache_id: CacheId, partition: usize },
    #[error("expected exactly one worker for cache {cache_id} partition {partition} but found {workers}")]
    MultipleLocations {
        cache_id: CacheId,
        partition: usize,
        workers: usize,
    },
    #[error("job {job_id} stage {stage} not found while resolving cache locations")]
    StageNotFound { job_id: JobId, stage: usize },
    #[error("cache reads require multiple workers ({existing} vs {actual}) for the same task set")]
    ConflictingWorkers {
        existing: WorkerId,
        actual: WorkerId,
    },
}
