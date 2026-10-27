use std::mem;

use log::{error, info, warn};
use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::worker::WorkerMessage;
use crate::worker::actor::WorkerActor;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let worker_id = self.options.worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal) {
            Ok(x) => x,
            Err(e) => {
                error!("{e}");
                return ActorAction::Stop;
            }
        };
        let host = self.options.worker_external_host.clone();
        let port = if self.options.worker_external_port > 0 {
            self.options.worker_external_port
        } else {
            port
        };
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        let client = self.driver_client_set.core.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let host = host.clone();
                    async move { client.register_worker(worker_id, host, port).await }
                })
                .await
            {
                error!("failed to register worker with retries: {e}");
                let _ = handle.send(WorkerMessage::Shutdown).await;
            }
            if let Err(e) = handle.send(WorkerMessage::StartHeartbeat).await {
                error!("failed to start worker heartbeat: {e}");
                let _ = handle.send(WorkerMessage::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_start_heartbeat(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        let worker_id = self.options.worker_id;
        let client = self.driver_client_set.core.clone();
        let interval = self.options.worker_heartbeat_interval;
        ctx.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = client.report_worker_heartbeat(worker_id).await {
                    warn!("failed to report worker heartbeat: {e}");
                    // We do not retry heartbeat since we will report heartbeat again after
                    // the configured interval.
                }
            }
        });
        ActorAction::Continue
    }
}
