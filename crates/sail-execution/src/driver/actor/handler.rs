use log::info;
use sail_server::actor::ActorAction;

use crate::driver::actor::DriverActor;
use crate::driver::state::WorkerDescriptor;
use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::job::JobDefinition;
use crate::worker_manager::WorkerLaunchContext;

impl DriverActor {
    pub(super) async fn handle_server_ready(&mut self, port: u16) -> ExecutionResult<ActorAction> {
        info!("driver server is ready on port {port}");
        let id = self.worker_id_generator.next()?;
        let ctx = WorkerLaunchContext {
            driver_host: self.options().driver_external_host.to_string(),
            driver_port: self.options().driver_external_port.unwrap_or(port),
        };
        self.worker_manager.launch_worker(id, ctx).await?;
        Ok(ActorAction::Continue)
    }

    pub(super) async fn handle_register_worker(
        &mut self,
        id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<ActorAction> {
        info!("registering worker {id} at {host}:{port}");
        self.state.add_worker(
            id,
            WorkerDescriptor {
                host,
                port,
                active: true,
            },
        );
        Ok(ActorAction::Continue)
    }

    pub(super) async fn handle_execute_job(
        &mut self,
        job: JobDefinition,
    ) -> ExecutionResult<ActorAction> {
        todo!()
    }
}
