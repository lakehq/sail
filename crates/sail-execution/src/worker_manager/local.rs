use std::collections::HashMap;

use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::Mutex;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerEvent, WorkerOptions};
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
    async fn start_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let options = WorkerOptions {
            worker_id: id,
            enable_tls: false,
            driver_host: options.driver_host,
            driver_port: options.driver_port,
            worker_listen_host: "127.0.0.1".to_string(),
            worker_listen_port: 0,
            worker_external_host: "127.0.0.1".to_string(),
            worker_external_port: None,
            memory_stream_buffer: 16,
        };
        let mut state = self.state.lock().await;
        let handle = state.system.spawn(options);
        state.workers.insert(id, handle);
        Ok(())
    }

    async fn stop_worker(&self, id: WorkerId) -> ExecutionResult<()> {
        let mut state = self.state.lock().await;
        if let Some(handle) = state.workers.remove(&id) {
            let _ = handle.send(WorkerEvent::Shutdown).await;
        }
        Ok(())
    }

    async fn stop_all_workers(&self) -> ExecutionResult<()> {
        let mut state = self.state.lock().await;
        let handles = state
            .workers
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = handle.send(WorkerEvent::Shutdown).await;
        }
        state.system.join().await;
        Ok(())
    }
}
