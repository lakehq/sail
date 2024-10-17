use std::collections::HashMap;

use sail_server::actor::{Actor, ActorAction, ActorHandle};

use crate::driver::actor::server::DriverServerStatus;
use crate::driver::state::DriverState;
use crate::driver::{DriverEvent, DriverOptions};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};
use crate::worker::WorkerClient;
use crate::worker_manager::{LocalWorkerManager, WorkerManager};

pub struct DriverActor {
    pub(super) state: DriverState,
    options: DriverOptions,
    server: DriverServerStatus,
    pub(super) job_id_generator: IdGenerator<JobId>,
    pub(super) task_id_generator: IdGenerator<TaskId>,
    pub(super) worker_id_generator: IdGenerator<WorkerId>,
    pub(super) worker_manager: Box<dyn WorkerManager>,
    worker_client_cache: HashMap<WorkerId, WorkerClient>,
}

#[tonic::async_trait]
impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;
    type Error = ExecutionError;

    fn new(options: DriverOptions) -> Self {
        let worker_manager = Box::new(LocalWorkerManager::new());
        Self {
            state: DriverState::new(),
            options,
            server: DriverServerStatus::Stopped,
            job_id_generator: IdGenerator::new(),
            task_id_generator: IdGenerator::new(),
            worker_id_generator: IdGenerator::new(),
            worker_manager,
            worker_client_cache: HashMap::new(),
        }
    }

    async fn start(&mut self, handle: &ActorHandle<Self>) -> ExecutionResult<()> {
        if matches!(self.server, DriverServerStatus::Stopped) {
            self.server = Self::start_server(handle.clone()).await?;
        }
        Ok(())
    }

    async fn receive(
        &mut self,
        message: DriverEvent,
        _handle: &ActorHandle<Self>,
    ) -> ExecutionResult<ActorAction> {
        match message {
            DriverEvent::ServerReady { port } => self.handle_server_ready(port).await,
            DriverEvent::RegisterWorker { id, host, port } => {
                self.handle_register_worker(id, host, port).await
            }
            DriverEvent::ExecuteJob { job } => self.handle_execute_job(job).await,
            DriverEvent::Shutdown => Ok(ActorAction::Stop),
        }
    }

    async fn stop(self) -> ExecutionResult<()> {
        match self.server {
            DriverServerStatus::Running {
                shutdown_signal, ..
            } => {
                let _ = shutdown_signal.send(());
            }
            DriverServerStatus::Stopped => {}
        }
        Ok(())
    }
}

impl DriverActor {
    pub(super) async fn worker_client(
        &mut self,
        id: WorkerId,
    ) -> ExecutionResult<&mut WorkerClient> {
        let worker = self
            .state
            .get_worker(&id)
            .ok_or_else(|| ExecutionError::InternalError(format!("worker not found: {id}")))?;
        if !worker.active {
            return Err(ExecutionError::InternalError(format!(
                "worker not active: {id}"
            )));
        }
        if !self.worker_client_cache.contains_key(&id) {
            let client =
                WorkerClient::connect(&worker.host.clone(), worker.port, self.options().enable_tls)
                    .await?;
            self.worker_client_cache.insert(id, client);
        }
        Ok(self.worker_client_cache.get_mut(&id).unwrap())
    }

    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }
}
