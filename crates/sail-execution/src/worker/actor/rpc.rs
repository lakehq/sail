use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::actor::WorkerActor;
use crate::worker::flight_server::WorkerFlightServer;
use crate::worker::gen::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;
use crate::worker::WorkerEvent;

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
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let flight_server = WorkerFlightServer::new(handle.clone());
        let flight_service = FlightServiceServer::new(flight_server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
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
}
