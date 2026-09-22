mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

pub(crate) use message::TaskRunnerMessage;
pub use options::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};
use tokio::sync::oneshot;

use crate::id::TaskKey;
use crate::stream::broadcast::BroadcastStreamManager;
use crate::task_runner::registry::TaskRegistry;

pub struct TaskRunnerActor {
    session_id: String,
    enable_shuffle_read_coalescing: bool,
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    tasks: TaskRegistry,
    broadcasts: BroadcastStreamManager,
    extensions: TaskRunnerExtensions,
    placement: TaskRunnerPlacement,
}
