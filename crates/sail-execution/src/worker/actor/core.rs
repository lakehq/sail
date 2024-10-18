use sail_server::actor::{Actor, ActorAction, ActorHandle};

use crate::driver::DriverClient;
use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::actor::server::WorkerServerStatus;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::state::WorkerState;

pub struct WorkerActor {
    pub(super) state: WorkerState,
    options: WorkerOptions,
    server: WorkerServerStatus,
    driver_client_cache: Option<DriverClient>,
}

#[tonic::async_trait]
impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;
    type Error = ExecutionError;

    fn new(options: WorkerOptions) -> Self {
        Self {
            state: WorkerState::new(),
            options,
            server: WorkerServerStatus::Stopped,
            driver_client_cache: None,
        }
    }

    async fn start(&mut self, handle: &ActorHandle<Self>) -> ExecutionResult<()> {
        if matches!(self.server, WorkerServerStatus::Stopped) {
            let addr = format!(
                "{}:{}",
                self.options.worker_listen_host, self.options.worker_listen_port
            );
            self.server = Self::start_server(handle.clone(), addr).await?;
        }
        Ok(())
    }

    async fn receive(
        &mut self,
        message: Self::Message,
        handle: &ActorHandle<Self>,
    ) -> ExecutionResult<ActorAction> {
        match message {
            WorkerEvent::ServerReady { port } => self.handle_server_ready(port).await,
            WorkerEvent::Shutdown => Ok(ActorAction::Stop),
        }
    }

    async fn stop(self) -> ExecutionResult<()> {
        match self.server {
            WorkerServerStatus::Running { shutdown_signal } => {
                let _ = shutdown_signal.send(());
            }
            WorkerServerStatus::Stopped => {}
        }
        Ok(())
    }
}

impl WorkerActor {
    pub(super) async fn driver_client(&mut self) -> ExecutionResult<&mut DriverClient> {
        if self.driver_client_cache.is_none() {
            let host = self.options.driver_host.clone();
            let port = self.options.driver_port;
            let client = DriverClient::connect(&host, port, self.options.enable_tls).await?;
            Ok(self.driver_client_cache.insert(client))
        } else {
            Ok(self.driver_client_cache.as_mut().unwrap())
        }
    }

    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }
}
