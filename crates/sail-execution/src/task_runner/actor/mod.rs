mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

pub(crate) use message::TaskRunnerMessage;
pub use options::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};
use tokio::sync::oneshot;

use crate::id::TaskKey;
use crate::profiling::ProfileHandle;
use crate::task_runner::registry::TaskRegistry;

pub struct TaskRunnerActor {
    session_id: String,
    profile: Option<ProfileHandle>,
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    tasks: TaskRegistry,
    extensions: TaskRunnerExtensions,
    placement: TaskRunnerPlacement,
}
