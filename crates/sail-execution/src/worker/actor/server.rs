use std::future::Future;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::actor::core::WorkerActor;
use crate::worker::flight_server::WorkerFlightServer;
use crate::worker::rpc::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;
use crate::worker::WorkerEvent;

pub enum WorkerServerStatus {
    Running {
        shutdown_signal: tokio::sync::oneshot::Sender<()>,
    },
    Stopped,
}

impl WorkerActor {
    pub(super) async fn start_server(
        handle: ActorHandle<WorkerActor>,
        addr: impl ToSocketAddrs,
    ) -> ExecutionResult<WorkerServerStatus> {
        let listener = TcpListener::bind(addr).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(Self::serve(handle, listener, async {
            let _ = rx.await;
        }));
        Ok(WorkerServerStatus::Running {
            shutdown_signal: tx,
        })
    }

    async fn serve<F>(
        handle: ActorHandle<WorkerActor>,
        listener: TcpListener,
        signal: F,
    ) -> ExecutionResult<()>
    where
        F: Future<Output = ()>,
    {
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

        let port = listener.local_addr()?.port();
        handle.send(WorkerEvent::ServerReady { port }).await?;

        ServerBuilder::new("sail_worker", Default::default())
            .add_service(service, Some(crate::worker::rpc::FILE_DESCRIPTOR_SET))
            .await
            .add_service(flight_service, None)
            .await
            .serve(listener, signal)
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }
}
