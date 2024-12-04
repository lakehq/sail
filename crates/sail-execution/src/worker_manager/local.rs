use std::collections::HashMap;

use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::Mutex;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerOptions};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

struct LocalWorkerManagerState {
    system: ActorSystem,
    workers: HashMap<WorkerId, ActorHandle<WorkerActor>>,
}

impl LocalWorkerManagerState {
    fn new() -> Self {
        Self {
            system: ActorSystem::new(),
            workers: HashMap::new(),
        }
    }
}

pub struct LocalWorkerManager {
    state: Mutex<LocalWorkerManagerState>,
}

impl LocalWorkerManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LocalWorkerManagerState::new()),
        }
    }
}

#[tonic::async_trait]
impl WorkerManager for LocalWorkerManager {
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let options = WorkerOptions {
            worker_id: id,
            enable_tls: options.enable_tls,
            driver_host: options.driver_external_host,
            driver_port: options.driver_external_port,
            worker_listen_host: "127.0.0.1".to_string(),
            worker_listen_port: 0,
            worker_external_host: "127.0.0.1".to_string(),
            worker_external_port: 0,
            worker_heartbeat_interval: options.worker_heartbeat_interval,
            worker_stream_buffer: options.worker_stream_buffer,
            rpc_retry_strategy: options.rpc_retry_strategy,
        };
        let mut state = self.state.lock().await;
        let handle = state.system.spawn(options);
        state.workers.insert(id, handle);
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        let mut state = self.state.lock().await;
        state.system.join().await;
        Ok(())
    }
}
