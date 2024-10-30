use arrow_flight::flight_service_server::FlightServiceServer;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::rpc::ClientOptions;
use crate::worker::actor::core::WorkerActor;
use crate::worker::flight_server::WorkerFlightServer;
use crate::worker::gen::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;
use crate::worker::{WorkerClient, WorkerEvent};

impl WorkerActor {
    pub(super) async fn serve(
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

    pub(super) fn worker_client(
        &mut self,
        id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<&WorkerClient> {
        let enable_tls = self.options().enable_tls;
        let client = self.worker_clients.entry(id).or_insert_with(|| {
            let options = ClientOptions {
                enable_tls,
                host,
                port,
            };
            WorkerClient::new(options)
        });
        Ok(client)
    }
}
