use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::execution::SendableRecordBatchStream;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::driver::rpc::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::state::DriverState;
use crate::error::{ExecutionError, ExecutionResult};
use crate::job::JobDefinition;
use crate::worker::WorkerEngine;

pub struct DriverEngine {
    state: Arc<Mutex<DriverState>>,
    host: String,
    port: u16,
    shutdown_signal: tokio::sync::oneshot::Sender<()>,
    server_handle: tokio::task::JoinHandle<ExecutionResult<()>>,
    worker: WorkerEngine,
}

impl DriverEngine {
    pub async fn start() -> ExecutionResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = "127.0.0.1".to_string();
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let state = Arc::new(Mutex::new(DriverState::new()));
        let server_handle = tokio::spawn(serve(
            Arc::clone(&state),
            listener,
            Some(async {
                _ = rx.await;
            }),
        ));
        let worker = WorkerEngine::start(&host, port).await?;

        Ok(Self {
            state,
            host,
            port,
            shutdown_signal: tx,
            server_handle,
            worker,
        })
    }

    pub async fn stop(self) -> ExecutionResult<()> {
        self.shutdown_signal.send(()).map_err(|e| {
            ExecutionError::InternalError(format!(
                "failed to send shutdown signal to driver server: {:?}",
                e
            ))
        })?;
        _ = self.server_handle.await?;
        Ok(())
    }

    pub fn execute(&self, _job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream> {
        todo!()
    }
}

async fn serve<F>(
    state: Arc<Mutex<DriverState>>,
    listener: TcpListener,
    signal: Option<F>,
) -> ExecutionResult<()>
where
    F: Future<Output = ()>,
{
    let server = DriverServer::new(state);
    let service = DriverServiceServer::new(server)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    sail_grpc::ServerBuilder::new("sail_driver", Default::default())
        .add_service(service, Some(crate::driver::rpc::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
        .map_err(|e| ExecutionError::InternalError(e.to_string()))
}
