use crate::worker::client::WorkerHandle;

pub(super) struct DriverState {
    workers: Vec<WorkerHandle>,
}

impl DriverState {
    pub fn new() -> Self {
        Self { workers: vec![] }
    }

    pub fn add_worker(&mut self, worker: WorkerHandle) {
        self.workers.push(worker);
    }
}
