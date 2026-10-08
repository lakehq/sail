use crate::driver::DriverOptions;

#[readonly::make]
pub struct TaskAssignerOptions {
    pub worker_task_slots: usize,
    pub worker_max_count: usize,
}

#[cfg(test)]
impl TaskAssignerOptions {
    pub(super) fn new(worker_task_slots: usize, worker_max_count: usize) -> Self {
        Self {
            worker_task_slots,
            worker_max_count,
        }
    }
}

impl From<&DriverOptions> for TaskAssignerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            worker_task_slots: options.worker_task_slots,
            worker_max_count: options.worker_max_count,
        }
    }
}
