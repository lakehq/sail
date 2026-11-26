// SPDX-License-Identifier: Apache-2.0

mod core;
mod options;
mod state;
mod topology;

#[cfg(test)]
pub(crate) use core::tests::event_reporter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::session::job::QueryCancellation;
use sail_telemetry::events::SystemEventReporter;
pub use state::{JobState, PlannedJob, RangeShuffleUpdate, TaskState};

use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::JobOutputHandle;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::proto::RemoteExecutionCodec;
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    shutdown: Arc<QueryCancellation>,
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: Box<dyn PhysicalExtensionCodec>,
    event_reporter: SystemEventReporter,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions, event_reporter: SystemEventReporter) -> Self {
        Self {
            options,
            shutdown: Arc::new(QueryCancellation::default()),
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec: Box::new(RemoteExecutionCodec),
            event_reporter,
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
        /// The cause of the task failures of the job so far, if the task attempt is a
        /// retry. It is the cause that the driver infers for a job that fails, which
        /// is not necessarily the failure that led to this attempt.
        retry_cause: Option<CommonErrorCause>,
    },
    FailJobOutput {
        handle: JobOutputHandle,
        cause: CommonErrorCause,
    },
    /// Reads the split points of a range shuffle on the driver, for the number of
    /// partitions of the shuffle (see [`crate::job_graph::RangeShuffle`]).
    ReadRangeSplitPoints {
        graph: Arc<crate::job_graph::JobGraph>,
        job_id: JobId,
        shuffle: usize,
        key: TaskStreamKey,
        schema: SchemaRef,
    },
    CleanUpJob {
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    },
}
