use std::mem;

use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::driver::actor::core::DriverActor;
use crate::driver::gen::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::rpc::ClientOptions;
use crate::worker::WorkerClient;

impl DriverActor {
    pub(super) fn start_server(&mut self, handle: &ActorHandle<Self>) -> ExecutionResult<()> {
        if self.server.is_running() {
            return Ok(());
        }
        let addr = (
            self.options().driver_listen_host.clone(),
            self.options().driver_listen_port,
        );
        tokio::spawn(Self::serve(handle.clone(), addr));
        Ok(())
    }

    pub(super) fn stop_server(&mut self) -> ExecutionResult<()> {
        let server = mem::take(&mut self.server);
        server.stop();
        self.server_listen_port = None;
        Ok(())
    }

    async fn serve(handle: ActorHandle<Self>, addr: impl ToSocketAddrs) -> ExecutionResult<()> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server = DriverServer::new(handle.clone());
        let service = DriverServiceServer::new(server)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        handle
            .send(DriverEvent::ServerReady { port, signal: tx })
            .await?;

        ServerBuilder::new("sail_driver", Default::default())
            .add_service(service, Some(crate::driver::gen::FILE_DESCRIPTOR_SET))
            .await
            .serve(listener, async {
                let _ = rx.await;
            })
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }

    pub(super) fn worker_client(&mut self, id: WorkerId) -> ExecutionResult<&WorkerClient> {
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
            let options = ClientOptions {
                enable_tls: self.options().enable_tls,
                host: worker.host.clone(),
                port: worker.port,
            };
            let client = WorkerClient::new(options);
            self.worker_client_cache.insert(id, client);
        }
        Ok(self.worker_client_cache.get_mut(&id).unwrap())
    }
}
