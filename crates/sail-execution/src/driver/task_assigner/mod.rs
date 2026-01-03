mod core;
mod options;
mod state;

use std::collections::VecDeque;

use indexmap::IndexMap;
pub use options::TaskAssignerOptions;

use crate::driver::task::{TaskAssignment, TaskRegion};
use crate::driver::task_assigner::state::{DriverResource, WorkerResource};
use crate::id::{TaskKey, WorkerId};

pub struct TaskAssigner {
    options: TaskAssignerOptions,
    driver: DriverResource,
    workers: IndexMap<WorkerId, WorkerResource>,
    /// A lookup table from task attempts to the place they are assigned to.
    /// This is more convenient than finding the task attempt in the task slots.
    /// Each task attempt can only be assigned once throughout its lifetime.
    /// This lookup table is updated when the task attempt is assigned,
    /// but there is no need to remove the task attempt when it is completed, as
    /// the mapping is still valid for historical purposes.
    task_assignments: IndexMap<TaskKey, TaskAssignment>,
    task_queue: VecDeque<TaskRegion>,
}

impl TaskAssigner {
    pub fn new(options: TaskAssignerOptions) -> Self {
        Self {
            options,
            driver: DriverResource::default(),
            workers: IndexMap::new(),
            task_assignments: IndexMap::new(),
            task_queue: VecDeque::new(),
        }
    }
}
