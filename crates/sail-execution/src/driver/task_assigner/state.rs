use indexmap::IndexSet;
use log::warn;

use crate::id::{JobId, TaskKey};
use crate::task::scheduling::TaskSet;

#[derive(Debug, Default)]
pub struct DriverResource {
    /// The task slots on the driver.
    /// The number of task slot can grow indefinitely.
    task_slots: Vec<TaskSlot>,
    /// The active local task streams that the driver owns.
    local_streams: IndexSet<TaskKey>,
    /// The active remote task streams.
    /// Remote task streams are tracked by the driver
    /// regardless of whether they are created by the driver or workers.
    remote_streams: IndexSet<TaskKey>,
}

impl DriverResource {
    pub fn add_task_set(&mut self, set: TaskSet) {
        for slot in &mut self.task_slots {
            if slot.is_vacant() {
                slot.add_tasks(set.tasks().cloned());
                return;
            }
        }
        let mut slot = TaskSlot::default();
        slot.add_tasks(set.tasks().cloned());
        self.task_slots.push(slot);
    }

    pub fn remove_task(&mut self, key: &TaskKey) -> bool {
        for slot in &mut self.task_slots {
            if slot.remove_task(key) {
                return true;
            }
        }
        false
    }

    pub fn track_local_streams(&mut self, set: &TaskSet) {
        self.local_streams.extend(set.local_streams().cloned());
    }

    pub fn track_remote_streams(&mut self, set: &TaskSet) {
        self.remote_streams.extend(set.remote_streams().cloned());
    }

    pub fn untrack_local_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        let count = self.local_streams.len();
        if let Some(stage) = stage {
            self.local_streams
                .retain(|x| x.job_id != job_id || x.stage != stage);
        } else {
            self.local_streams.retain(|x| x.job_id != job_id);
        }
        count != self.local_streams.len()
    }

    pub fn untrack_remote_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        let count = self.remote_streams.len();
        if let Some(stage) = stage {
            self.remote_streams
                .retain(|x| x.job_id != job_id || x.stage != stage);
        } else {
            self.remote_streams.retain(|x| x.job_id != job_id);
        }
        count != self.remote_streams.len()
    }
}

#[derive(Debug)]
pub enum WorkerResource {
    Active {
        /// The task slots on the worker.
        task_slots: Vec<TaskSlot>,
        /// The active local task streams that the worker owns.
        ///
        /// This is used to support "shuffle tracking" similar to the mechanism in Spark.
        /// We track the streams at the granularity of task attempts even if
        /// each task attempt can produce multiple channels of output streams.
        ///
        /// The task stream is considered active if the tasks depending on it has not
        /// yet completed (either succeeded or failed), or if the task stream is part of
        /// the job output to be consumed.
        ///
        /// The worker needs to be running if there are active local task streams, even if
        /// no tasks are currently assigned to it. But the worker can be stopped if there are
        /// active remote task streams stored in object storage.
        local_streams: IndexSet<TaskKey>,
    },
    Inactive,
}

impl WorkerResource {
    pub fn add_task_set(&mut self, slot: usize, set: TaskSet) {
        match self {
            WorkerResource::Active { task_slots, .. } => {
                if let Some(slot) = task_slots.get_mut(slot) {
                    slot.add_tasks(set.tasks().cloned());
                } else {
                    warn!("invalid task slot {slot} on worker");
                }
            }
            WorkerResource::Inactive => {
                warn!("cannot add tasks to inactive worker");
            }
        }
    }

    pub fn remove_task(&mut self, key: &TaskKey, slot: usize) -> bool {
        match self {
            WorkerResource::Active { task_slots, .. } => {
                if let Some(slot) = task_slots.get_mut(slot) {
                    slot.remove_task(key)
                } else {
                    warn!("invalid task slot {slot} on worker");
                    false
                }
            }
            WorkerResource::Inactive => {
                warn!("cannot remove tasks from inactive worker");
                false
            }
        }
    }

    pub fn track_local_streams(&mut self, set: &TaskSet) {
        match self {
            WorkerResource::Active { local_streams, .. } => {
                local_streams.extend(set.local_streams().cloned());
            }
            WorkerResource::Inactive => {
                warn!("cannot track local streams on inactive worker");
            }
        }
    }

    pub fn untrack_local_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        match self {
            WorkerResource::Active { local_streams, .. } => {
                let count = local_streams.len();
                if let Some(stage) = stage {
                    local_streams.retain(|x| x.job_id != job_id || x.stage != stage);
                } else {
                    local_streams.retain(|x| x.job_id != job_id);
                }
                count != local_streams.len()
            }
            WorkerResource::Inactive => {
                warn!("cannot untrack local streams from inactive worker");
                false
            }
        }
    }
}

/// A task slot on the driver or a worker.
///
/// Each task slot can run multiple tasks from the same job,
/// as long as they belong to different stages of the same group.
/// Tasks from different partitions in the same stage cannot
/// share the same task slot.
/// The job scheduler decides how the tasks share the task slots.
///
/// A task slot only represents logical task assignment.
/// There is no physical resource isolation since the session context
/// is shared within the driver or each worker.
#[derive(Debug, Clone)]
pub struct TaskSlot {
    tasks: IndexSet<TaskKey>,
}

impl Default for TaskSlot {
    fn default() -> Self {
        TaskSlot {
            tasks: IndexSet::new(),
        }
    }
}

impl TaskSlot {
    pub fn is_vacant(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn add_tasks(&mut self, tasks: impl IntoIterator<Item = TaskKey>) {
        for task in tasks {
            self.tasks.insert(task);
        }
    }

    pub fn remove_task(&mut self, task: &TaskKey) -> bool {
        self.tasks.swap_remove(task)
    }

    pub fn list_tasks(&self) -> impl Iterator<Item = &TaskKey> {
        self.tasks.iter()
    }
}
