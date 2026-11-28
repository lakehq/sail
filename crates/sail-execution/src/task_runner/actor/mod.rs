// SPDX-License-Identifier: Apache-2.0

mod core;
mod handler;
mod message;
mod options;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::TaskContext;
pub(crate) use message::{TaskBatchContext, TaskRunnerMessage};
pub use options::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};
use tokio::sync::oneshot;

use crate::id::{JobId, TaskKey};
use crate::task_runner::registry::TaskRegistry;

pub struct TaskRunnerActor {
    session_id: String,
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    tasks: TaskRegistry,
    /// The task context of each job that runs on this worker, built from the driver session.
    job_contexts: HashMap<JobId, Arc<TaskContext>>,
    extensions: TaskRunnerExtensions,
    placement: TaskRunnerPlacement,
}
