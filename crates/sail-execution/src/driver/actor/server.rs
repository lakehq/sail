use std::future::Future;

use sail_server::actor::ActorHandle;
use sail_server::ServerBuilder;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::driver::actor::core::DriverActor;
use crate::driver::rpc::driver_service_server::DriverServiceServer;
use crate::driver::server::DriverServer;
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};

pub enum DriverServerStatus {
    Running {
        host: String,
        port: u16,
        shutdown_signal: tokio::sync::oneshot::Sender<()>,
    },
    Stopped,
}

impl DriverActor {
    pub(super) async fn start_server(
        handle: ActorHandle<Self>,
    ) -> ExecutionResult<DriverServerStatus> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = "127.0.0.1".to_string();
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(Self::serve(handle, listener, async {
            let _ = rx.await;
        }));
        Ok(DriverServerStatus::Running {
            host,
            port,
            shutdown_signal: tx,
        })
    }

    async fn serve<F>(
        handle: ActorHandle<Self>,
        listener: TcpListener,
        signal: F,
    ) -> ExecutionResult<()>
    where
        F: Future<Output = ()>,
    {
        let server = DriverServer::new(handle.clone());
        let service = DriverServiceServer::new(server)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let port = listener.local_addr()?.port();
        handle.send(DriverEvent::ServerReady { port }).await?;

        ServerBuilder::new("sail_driver", Default::default())
            .add_service(service, Some(crate::driver::rpc::FILE_DESCRIPTOR_SET))
            .await
            .serve(listener, signal)
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }
}
