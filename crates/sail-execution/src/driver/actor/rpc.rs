use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::codec::CompressionEncoding;

use crate::driver::actor::core::DriverActor;
use crate::driver::gen::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::state::WorkerState;
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::rpc::ClientOptions;

impl DriverActor {
    pub(super) async fn serve(
        handle: ActorHandle<Self>,
        addr: impl ToSocketAddrs,
    ) -> ExecutionResult<()> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server = DriverServer::new(handle.clone());
        let service = DriverServiceServer::new(server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
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

    pub(super) fn worker_client_options(&mut self, id: WorkerId) -> ExecutionResult<ClientOptions> {
        let worker = self
            .state
            .get_worker(id)
            .ok_or_else(|| ExecutionError::InternalError(format!("worker not found: {id}")))?;
        let (host, port) = match &worker.state {
            WorkerState::Running { host, port, .. } => (host.clone(), *port),
            _ => {
                return Err(ExecutionError::InternalError(format!(
                    "worker not active: {id}"
                )))
            }
        };
        let enable_tls = self.options().enable_tls;
        Ok(ClientOptions {
            enable_tls,
            host,
            port,
        })
    }
}
