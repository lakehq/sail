use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::oneshot::Sender;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;

use crate::driver::actor::DriverActor;
use crate::driver::gen::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskStreamKey;
use crate::stream::reader::TaskStreamSource;
use crate::stream_service::{TaskStreamFetcher, TaskStreamFlightServer};

struct DriverTaskStreamFetcher {
    handle: ActorHandle<DriverActor>,
}

#[async_trait]
impl TaskStreamFetcher for DriverTaskStreamFetcher {
    async fn fetch(
        &self,
        key: TaskStreamKey,
        sender: Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()> {
        let event = DriverEvent::FetchDriverStream {
            key,
            result: sender,
        };
        self.handle.send(event).await.map_err(ExecutionError::from)
    }
}

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

        let flight_server = TaskStreamFlightServer::new(Box::new(DriverTaskStreamFetcher {
            handle: handle.clone(),
        }));
        let flight_service = FlightServiceServer::new(flight_server)
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
            .add_service(flight_service, None)
            .await
            .serve(listener, async {
                let _ = rx.await;
            })
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }
}
