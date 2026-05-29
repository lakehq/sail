use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::Mutex;

use crate::error::{ExecutionError, ExecutionResult};
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

pub struct NoopWorkerManager;

#[tonic::async_trait]
impl WorkerManager for NoopWorkerManager {
    async fn launch_worker(
        &self,
        _id: WorkerId,
        _options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        Err(ExecutionError::InternalError(
            "unexpected call to launch_worker in local execution mode; local execution does not \
             launch worker processes"
                .to_string(),
        ))
    }

    async fn stop(&self) -> ExecutionResult<()> {
        Ok(())
    }
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
