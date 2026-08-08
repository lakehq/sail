mod core;
mod monitor;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::{DriverMessage, TaskStatus};
use crate::id::TaskKey;
use crate::worker::WorkerMessage;

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
    ) -> Self;
}

impl TaskRunnerMessage for DriverMessage {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> Self {
        DriverMessage::UpdateTask {
            key,
            status,
            message,
            cause,
            sequence: None,
        }
    }
}

impl TaskRunnerMessage for WorkerMessage {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> Self {
        WorkerMessage::ReportTaskStatus {
            key,
            status,
            message,
            cause,
        }
    }
}
