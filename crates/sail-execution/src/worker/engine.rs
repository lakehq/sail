use std::future::Future;
use std::sync::{Arc, Mutex};

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::driver::DriverHandle;
use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::flight_server::WorkerFlightServer;
use crate::worker::rpc::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;
use crate::worker::state::WorkerState;

pub struct WorkerEngine {
    state: Arc<Mutex<WorkerState>>,
    host: String,
    port: u16,
    shutdown_signal: tokio::sync::oneshot::Sender<()>,
    server_handle: tokio::task::JoinHandle<ExecutionResult<()>>,
}

impl WorkerEngine {
    pub async fn start(driver_host: &str, driver_port: u16) -> ExecutionResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = "127.0.0.1".to_string();
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut driver = DriverHandle::connect(driver_host.to_string(), driver_port).await?;
        driver.register_worker("0", &host, port).await?;
        let state = Arc::new(Mutex::new(WorkerState::new(driver)));
        let server_handle = tokio::spawn(serve(
            Arc::clone(&state),
            listener,
            Some(async {
                _ = rx.await;
            }),
        ));
        Ok(WorkerEngine {
            state,
            host,
            port,
            shutdown_signal: tx,
            server_handle,
        })
    }

    pub async fn stop(self) -> ExecutionResult<()> {
        self.shutdown_signal.send(()).map_err(|e| {
            ExecutionError::InternalError(format!(
                "failed to send shutdown signal to worker server: {:?}",
                e
            ))
        })?;
        _ = self.server_handle.await?;
        Ok(())
    }
}

async fn serve<F>(
    state: Arc<Mutex<WorkerState>>,
    listener: TcpListener,
    signal: Option<F>,
) -> ExecutionResult<()>
where
    F: Future<Output = ()>,
{
    let server = WorkerServer::new(Arc::clone(&state));
    let service = WorkerServiceServer::new(server)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);

    let flight_server = WorkerFlightServer::new(state);
    let flight_service = FlightServiceServer::new(flight_server)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    sail_grpc::ServerBuilder::new("sail_worker", Default::default())
        .add_service(service, Some(crate::worker::rpc::FILE_DESCRIPTOR_SET))
        .await
        .add_service(flight_service, None)
        .await
        .serve(listener, signal)
        .await
        .map_err(|e| ExecutionError::InternalError(e.to_string()))
}
