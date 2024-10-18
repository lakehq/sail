use std::mem;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::driver::DriverClient;
use crate::error::{ExecutionError, ExecutionResult};
use crate::rpc::ClientOptions;
use crate::worker::actor::core::WorkerActor;
use crate::worker::flight_server::WorkerFlightServer;
use crate::worker::gen::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;
use crate::worker::WorkerEvent;

impl WorkerActor {
    pub(super) fn start_server(
        &mut self,
        handle: &ActorHandle<WorkerActor>,
    ) -> ExecutionResult<()> {
        if self.server.is_running() {
            return Ok(());
        }
        let addr = (
            self.options().worker_listen_host.clone(),
            self.options().worker_listen_port,
        );
        tokio::spawn(Self::serve(handle.clone(), addr));
        Ok(())
    }

    pub(super) fn stop_server(&mut self) -> ExecutionResult<()> {
        let server = mem::take(&mut self.server);
        server.stop();
        Ok(())
    }

    async fn serve(
        handle: ActorHandle<WorkerActor>,
        addr: impl ToSocketAddrs,
    ) -> ExecutionResult<()> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server = WorkerServer::new(handle.clone());
        let service = WorkerServiceServer::new(server)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let flight_server = WorkerFlightServer::new(handle.clone());
        let flight_service = FlightServiceServer::new(flight_server)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        handle
            .send(WorkerEvent::ServerReady { port, signal: tx })
            .await?;

        ServerBuilder::new("sail_worker", Default::default())
            .add_service(service, Some(crate::worker::gen::FILE_DESCRIPTOR_SET))
            .await
            .add_service(flight_service, None)
            .await
            .serve(listener, async {
                let _ = rx.await;
            })
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }

    pub(super) fn driver_client(&mut self) -> ExecutionResult<&DriverClient> {
        if self.driver_client_cache.is_none() {
            let options = ClientOptions {
                enable_tls: self.options().enable_tls,
                host: self.options().driver_host.clone(),
                port: self.options().driver_port,
            };
            let client = DriverClient::new(options);
            Ok(self.driver_client_cache.insert(client))
        } else {
            Ok(self.driver_client_cache.as_mut().unwrap())
        }
    }
}
