use std::collections::HashMap;

use sail_server::actor::ActorHandle;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerOptions};
use crate::worker_manager::{WorkerLaunchContext, WorkerManager};

pub struct LocalWorkerManager {
    workers: HashMap<WorkerId, ActorHandle<WorkerActor>>,
}

impl LocalWorkerManager {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl WorkerManager for LocalWorkerManager {
    async fn launch_worker(
        &mut self,
        id: WorkerId,
        ctx: WorkerLaunchContext,
    ) -> ExecutionResult<()> {
        let options = WorkerOptions {
            worker_id: id,
            enable_tls: false,
            driver_host: ctx.driver_host,
            driver_port: ctx.driver_port,
            worker_listen_host: "127.0.0.1".to_string(),
            worker_listen_port: 0,
            worker_external_host: "127.0.0.1".to_string(),
            worker_external_port: None,
        };
        let handle = ActorHandle::new(options);
        self.workers.insert(id, handle);
        Ok(())
    }
}
