mod core;
mod monitor;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::{DriverEvent, TaskStatus};
use crate::id::TaskKey;
use crate::local_cache_store::LocalCacheStore;
use crate::plan::CachePartitionNotifier;
use crate::worker::WorkerEvent;

pub struct TaskRunner {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,
    pub(crate) cache_store: Arc<LocalCacheStore>,
    cache_notifier: Option<Arc<dyn CachePartitionNotifier>>,
}

impl TaskRunner {
    /// Sets the notifier used by cache write nodes to publish partition locations.
    pub fn set_cache_partition_notifier(
        &mut self,
        cache_notifier: Option<Arc<dyn CachePartitionNotifier>>,
    ) {
        self.cache_notifier = cache_notifier;
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
