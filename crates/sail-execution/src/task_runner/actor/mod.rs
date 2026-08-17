mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub(crate) use message::TaskRunnerMessage;
pub use options::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};
use tokio::sync::oneshot;

use crate::id::TaskKey;

pub struct TaskRunnerActor {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,
    extensions: TaskRunnerExtensions,
    placement: TaskRunnerPlacement,
}
