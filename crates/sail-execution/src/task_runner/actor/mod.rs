// SPDX-License-Identifier: Apache-2.0

mod core;
mod handler;
mod message;
mod options;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

pub(crate) use message::TaskRunnerMessage;
pub use options::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};
use tokio::sync::oneshot;

use crate::id::TaskKey;
use crate::task_runner::registry::TaskRegistry;

pub struct TaskRunnerActor {
    session_id: String,
    compute: tokio::runtime::Handle,
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    tasks: TaskRegistry,
    extensions: TaskRunnerExtensions,
    placement: TaskRunnerPlacement,
}
