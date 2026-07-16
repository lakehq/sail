use arrow_flight::flight_service_server::FlightServiceServer;
use prost::Message;
use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_server::ServerBuilder;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::codec::CompressionEncoding;
use tonic::{Status, async_trait};

use crate::driver::r#gen::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::{DriverEvent, DriverRegistry};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{DriverId, TaskStreamKey};
use crate::stream::r#gen::{DriverTaskStreamTicket, TaskStreamTicket};
use crate::stream::reader::TaskStreamSource;
use crate::stream_service::{TaskStreamFetcher, TaskStreamFlightServer, TaskStreamKeyDecoder};

#[derive(Clone)]
pub struct DriverGatewayOptions {
    pub listen_host: String,
    pub listen_port: u16,
}

impl DriverGatewayOptions {
    pub fn new(config: &AppConfig) -> Self {
        Self {
            listen_host: config.cluster.driver_listen_host.clone(),
            listen_port: config.cluster.driver_listen_port,
        }
    }
}

struct DriverTaskStreamFetcher {
    registry: DriverRegistry,
}

#[derive(Debug)]
struct DriverTaskStreamKey {
    driver_id: DriverId,
    stream: TaskStreamKey,
}

impl TaskStreamKeyDecoder for DriverTaskStreamKey {
    fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let DriverTaskStreamTicket { driver_id, stream } = DriverTaskStreamTicket::decode(bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let stream: TaskStreamTicket =
            stream.ok_or_else(|| Status::invalid_argument("task stream ticket is required"))?;
        Ok(Self {
            driver_id: driver_id.into(),
            stream: stream.into(),
        })
    }
}

#[async_trait]
impl TaskStreamFetcher<DriverTaskStreamKey> for DriverTaskStreamFetcher {
    async fn fetch(
        &self,
        key: DriverTaskStreamKey,
        sender: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()> {
        self.registry
            .get(key.driver_id)?
            .send(DriverEvent::FetchDriverStream {
                key: key.stream,
                result: sender,
            })
            .await
            .map_err(ExecutionError::from)
    }
}

pub struct DriverGateway {
    port: u16,
    signal: oneshot::Sender<()>,
    handle: JoinHandle<ExecutionResult<()>>,
}

impl DriverGateway {
    pub async fn start(
        options: DriverGatewayOptions,
        registry: DriverRegistry,
    ) -> ExecutionResult<Self> {
        let listener = TcpListener::bind((options.listen_host, options.listen_port)).await?;
        let port = listener.local_addr()?.port();
        let (tx, rx) = oneshot::channel();

        let service = DriverServiceServer::new(DriverServer::new(registry.clone()))
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);
        let flight_service =
            FlightServiceServer::new(TaskStreamFlightServer::<DriverTaskStreamKey>::new(
                Box::new(DriverTaskStreamFetcher { registry }),
            ))
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let handle = tokio::spawn(async move {
            ServerBuilder::new("sail_driver", Default::default())
                .add_service(service, Some(crate::driver::r#gen::FILE_DESCRIPTOR_SET))
                .await
                .add_service(flight_service, None)
                .await
                .serve(listener, async {
                    let _ = rx.await;
                })
                .await
                .map_err(|e| ExecutionError::InternalError(e.to_string()))
        });
        Ok(Self {
            port,
            signal: tx,
            handle,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn stop(self) {
        let _ = self.signal.send(());
        let _ = self.handle.await;
    }
}
