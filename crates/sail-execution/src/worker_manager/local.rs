use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::Mutex;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerEvent, WorkerOptions};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

struct LocalWorkerManagerState {
    system: ActorSystem,
    workers: HashMap<WorkerId, ActorHandle<WorkerActor>>,
    stopping: bool,
}

impl LocalWorkerManagerState {
    fn new() -> Self {
        Self {
            system: ActorSystem::new(),
            workers: HashMap::new(),
            stopping: false,
        }
    }

    fn ensure_accepting_launches(&self) -> ExecutionResult<()> {
        if self.stopping {
            Err(ExecutionError::InvalidArgument(
                "local worker manager is stopping".to_string(),
            ))
        } else {
            Ok(())
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
        state.ensure_accepting_launches()?;
        let handle = state.system.spawn(options);
        state.workers.insert(id, handle);
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        let mut state = self.state.lock().await;
        state.stopping = true;
        let mut completions = Vec::with_capacity(state.workers.len());
        for worker in state.workers.values() {
            let (result, completion) = tokio::sync::oneshot::channel();
            if worker
                .send(WorkerEvent::StopWorker { result })
                .await
                .is_ok()
            {
                completions.push(completion);
            }
        }
        for completion in completions {
            let _ = completion.await;
        }
        state.system.join().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_worker_manager_rejects_launch_after_stop_begins() {
        let mut state = LocalWorkerManagerState::new();
        assert!(state.ensure_accepting_launches().is_ok());
        state.stopping = true;
        assert!(state.ensure_accepting_launches().is_err());
    }
}
