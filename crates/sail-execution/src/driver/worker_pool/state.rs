use std::collections::HashSet;

use tokio::time::Instant;

use crate::id::{TaskKey, WorkerId};
use crate::worker::WorkerClientSet;

pub struct WorkerDescriptor {
    pub state: WorkerState,
    pub messages: Vec<String>,
    /// A list of peer workers known to the worker.
    /// The list may or may not cover all the running workers,
    /// but it does not affect the correctness of the cluster behavior.
    /// The list is only used by the driver to avoid redundant information
    /// when propagating worker locations when running tasks.
    pub peers: HashSet<WorkerId>,
}

pub enum WorkerState {
    Pending,
    Running {
        host: String,
        port: u16,
        /// The task slots on the worker.
        slots: Vec<TaskSlot>,
        /// The active local task streams that the worker owns.
        ///
        /// This is used to support "shuffle tracking" similar to the mechanism in Spark.
        /// We track the streams at the granularity of task attempts even if a task can
        /// produce multiple channels of output streams.
        ///
        /// The task stream is considered active if the tasks depending on it has not
        /// yet completed (either succeeded or failed), or if the task stream is part of
        /// the job output to be consumed.
        ///
        /// The worker needs to be running if there are active local task streams, even if
        /// no tasks are currently assigned to it. But the worker can be stopped if there are
        /// active remote task streams stored in object storage.
        streams: Vec<TaskKey>,
        updated_at: Instant,
        heartbeat_at: Instant,
        /// The gRPC client to communicate with the worker if the connection is established.
        client: Option<WorkerClientSet>,
    },
    Stopped,
    Failed,
}

/// A task slot on a worker.
///
/// Each task slot can run multiple tasks from the same job,
/// as long as they belong to different stages of the same group.
/// Tasks from different partitions in the same stage cannot
/// share the same task slot.
/// The job scheduler decides how the tasks share the task slots.
///
/// A task slot only represents logical task assignment on a worker.
/// There is no physical resource isolation since they run within
/// the same session context.
#[derive(Clone)]
pub struct TaskSlot {
    tasks: HashSet<TaskKey>,
}

impl Default for TaskSlot {
    fn default() -> Self {
        TaskSlot {
            tasks: HashSet::new(),
        }
    }
}

impl TaskSlot {
    pub fn is_vacant(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn is_occupied(&self) -> bool {
        !self.tasks.is_empty()
    }

    pub fn add(&mut self, task: TaskKey) {
        self.tasks.insert(task);
    }

    pub fn remove(&mut self, task: &TaskKey) -> bool {
        self.tasks.remove(task)
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskKey> {
        self.tasks.iter()
    }
}
