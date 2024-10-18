use log::info;
use sail_server::actor::ActorAction;

use crate::error::ExecutionResult;
use crate::worker::actor::core::WorkerActor;

impl WorkerActor {
    pub(super) async fn handle_server_ready(&mut self, port: u16) -> ExecutionResult<ActorAction> {
        let worker_id = self.options().worker_id;
        info!("worker server for {worker_id} is ready on port {port}");
        let host = self.options().worker_external_host.clone();
        let port = self.options().worker_external_port.unwrap_or(port);
        let client = self.driver_client().await?;
        client.register_worker(worker_id, host, port).await?;
        Ok(ActorAction::Continue)
    }
}
