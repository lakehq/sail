use crate::id::{TaskKey, WorkerId};
use crate::job_graph::TaskPlacement;

#[derive(Clone)]
pub struct TaskRegion {
    pub tasks: Vec<(TaskPlacement, TaskSet)>,
}

#[derive(Clone)]
pub struct TaskSet {
    pub entries: Vec<TaskSetEntry>,
}

#[derive(Clone)]
pub struct TaskSetEntry {
    pub key: TaskKey,
    pub output: TaskOutputKind,
}

#[derive(Clone)]
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

    pub fn contains(&self, key: &TaskKey) -> bool {
        self.entries.iter().any(|entry| &entry.key == key)
    }
}

#[derive(Clone)]
pub struct TaskSetAssignment {
    pub set: TaskSet,
    pub assignment: TaskAssignment,
}

#[derive(Clone)]
pub enum TaskAssignment {
    Driver,
    Worker { worker_id: WorkerId, slot: usize },
}

pub trait TaskAssignmentGetter {
    fn get(&self, key: &TaskKey) -> Option<&TaskAssignment>;
}

pub enum TaskStreamAssignment {
    Driver,
    Worker { worker_id: WorkerId },
}
