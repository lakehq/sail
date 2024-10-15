use std::future::Future;

use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;

use crate::distributed::worker::server::WorkerServer;
use crate::rpc::worker_service_server::WorkerServiceServer;

pub async fn serve<F>(
    listener: TcpListener,
    signal: Option<F>,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let server = WorkerServer::new();

    let nodelay = true;
    let keepalive = None;
    let incoming = TcpIncoming::from_listener(listener, nodelay, keepalive)
        .map_err(|e| e as Box<dyn std::error::Error>)?;
    let server = tonic::transport::Server::builder()
        .tcp_nodelay(nodelay)
        .tcp_keepalive(keepalive)
        .add_service(
            WorkerServiceServer::new(server)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Zstd),
        );

    match signal {
        None => {
            server.serve_with_incoming(incoming).await?;
        }
        Some(signal) => {
            server
                .serve_with_incoming_shutdown(incoming, signal)
                .await?;
        }
    }

    Ok(())
}
