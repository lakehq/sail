use crate::id::{TaskKey, WorkerId};
use crate::job_graph::TaskPlacement;

/// A task region represents multiple task sets that should be scheduled together.
/// Each task set can be placed either on the driver or on the workers.
/// Failure of any tasks in the region should trigger a rescheduling of the entire region.
///
/// A task region is formed under either of the following ways:
///   1. The tasks of all partitions of stages that satisfy the following:
///      1. The inputs of the initial stages, if any, are blocking.
///      2. The outputs of the final stages are either blocking or the job output.
///      3. The outputs among stages are pipelined.
///   2. The task of a single partition of stages that satisfy the following:
///      1. The inputs of the initial stages, if any, are blocking.
///      2. The output of the final stages are either blocking or the job output.
///      3. The outputs among stages are pipelined with forward input mode,
///         if there are more than one stage involved.
#[derive(Debug, Clone)]
pub struct TaskRegion {
    pub tasks: Vec<(TaskPlacement, TaskSet)>,
}

/// A task set represents a collection of tasks that are assigned to
/// a single task slot on the driver or a worker.
/// The tasks must come from different stages of the same "slot sharing group".
/// The tasks of different partitions of the same stage must be assigned to
/// different task sets.
#[derive(Debug, Clone)]
pub struct TaskSet {
    pub entries: Vec<TaskSetEntry>,
}

#[derive(Debug, Clone)]
pub struct TaskSetEntry {
    pub key: TaskKey,
    pub output: TaskOutputKind,
}

#[derive(Debug, Clone)]
pub enum TaskOutputKind {
    Local,
    Remote,
}

impl TaskRegion {
    pub fn contains(&self, key: &TaskKey) -> bool {
        self.tasks.iter().any(|(_, set)| set.contains(key))
    }
}

impl TaskSet {
    pub fn tasks(&self) -> impl Iterator<Item = &TaskKey> {
        self.entries.iter().map(|entry| &entry.key)
    }

    pub fn local_streams(&self) -> impl Iterator<Item = &TaskKey> {
        self.entries
            .iter()
            .filter(|entry| matches!(entry.output, TaskOutputKind::Local))
            .map(|entry| &entry.key)
    }

    pub fn remote_streams(&self) -> impl Iterator<Item = &TaskKey> {
        self.entries
            .iter()
            .filter(|entry| matches!(entry.output, TaskOutputKind::Remote))
            .map(|entry| &entry.key)
    }

    pub fn contains(&self, key: &TaskKey) -> bool {
        self.entries.iter().any(|entry| &entry.key == key)
    }
}

#[derive(Debug, Clone)]
pub struct TaskSetAssignment {
    pub set: TaskSet,
    pub assignment: TaskAssignment,
}

#[derive(Debug, Clone)]
pub enum TaskAssignment {
    Driver,
    Worker { worker_id: WorkerId, slot: usize },
}

pub trait TaskAssignmentGetter {
    fn get(&self, key: &TaskKey) -> Option<&TaskAssignment>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskStreamAssignment {
    Driver,
    Worker { worker_id: WorkerId },
}
