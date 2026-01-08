use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use sail_common::runtime::RuntimeHandle;
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
    runtime: RuntimeHandle,
    session: SessionContext,
}

impl LocalWorkerManager {
    pub fn new(runtime: RuntimeHandle, session: SessionContext) -> Self {
        Self {
            state: Mutex::new(LocalWorkerManagerState::new()),
            runtime,
            session,
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
        let options = WorkerOptions::local(id, options, self.runtime.clone(), self.session.clone());
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
