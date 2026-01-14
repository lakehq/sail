use crate::driver::DriverOptions;

#[readonly::make]
pub struct TaskAssignerOptions {
    pub worker_task_slots: usize,
    pub worker_max_count: usize,
}

impl From<&DriverOptions> for TaskAssignerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            worker_task_slots: options.worker_task_slots,
            worker_max_count: options.worker_max_count,
        }
    }
}
