mod core;
mod monitor;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::driver::{DriverEvent, TaskStatus};
use crate::id::TaskKey;
use crate::local_cache_store::LocalCacheStore;
use crate::worker::{WorkerActor, WorkerEvent};

pub struct TaskRunner {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,
    pub(crate) cache_store: Arc<LocalCacheStore>,
    cache_write_worker_handle: Option<ActorHandle<WorkerActor>>,
}

impl TaskRunner {
    /// Sets the worker actor handle injected into cache write exec nodes.
    pub fn set_cache_write_worker_handle(
        &mut self,
        cache_write_worker_handle: Option<ActorHandle<WorkerActor>>,
    ) {
        self.cache_write_worker_handle = cache_write_worker_handle;
    }
}

pub trait TaskRunnerMessage {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> Self;
}

impl TaskRunnerMessage for DriverEvent {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> Self {
        DriverEvent::UpdateTask {
            key,
            status,
            message,
            cause,
            sequence: None,
        }
    }
}

impl TaskRunnerMessage for WorkerEvent {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> Self {
        WorkerEvent::ReportTaskStatus {
            key,
            status,
            message,
            cause,
        }
    }
}
