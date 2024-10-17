use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::id::{JobId, WorkerId};

pub struct DriverState {
    workers: HashMap<WorkerId, WorkerDescriptor>,
    jobs: HashMap<JobId, JobDescriptor>,
}

impl DriverState {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            jobs: HashMap::new(),
        }
    }

    pub fn add_worker(&mut self, id: WorkerId, descriptor: WorkerDescriptor) {
        self.workers.insert(id, descriptor);
    }

    pub fn add_job(&mut self, id: JobId, descriptor: JobDescriptor) {
        self.jobs.insert(id, descriptor);
    }

    pub fn get_worker(&self, id: &WorkerId) -> Option<&WorkerDescriptor> {
        self.workers.get(id)
    }
}

pub struct WorkerDescriptor {
    pub host: String,
    pub port: u16,
    pub active: bool,
}

pub struct JobDescriptor {
    pub plan: Arc<dyn ExecutionPlan>,
}
