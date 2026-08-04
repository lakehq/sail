mod core;
mod monitor;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::{DriverEvent, TaskStatus};
use crate::id::TaskKey;
use crate::worker::WorkerEvent;

pub struct TaskRunner {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,
}

pub trait TaskRunnerMessage {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        metrics_json: Option<String>,
    ) -> Self;
}

impl TaskRunnerMessage for DriverEvent {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        metrics_json: Option<String>,
    ) -> Self {
        DriverEvent::UpdateTask {
            key,
            status,
            message,
            cause,
            metrics_json,
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
        metrics_json: Option<String>,
    ) -> Self {
        WorkerEvent::ReportTaskStatus {
            key,
            status,
            message,
            cause,
            metrics_json,
        }
    }
}
