use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeHandle;
use sail_server::ServerBuilder;
pub use sail_session::session_manager::SessionManagerOptions;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::artifact::flush_artifact_cleanup_journal;
use crate::server::SparkConnectServer;
use crate::service::{
    artifact_cleanup_journal_flush_timeout, artifact_io_drain_timeout, drain_artifact_io,
};
use crate::session_manager::create_spark_session_manager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

/// The meat of the gRPC server.
pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()> + Send,
{
    let session_manager = create_spark_session_manager(config.clone(), runtime)?;
    let session_manager_for_shutdown = session_manager.clone();
    let artifact_io_timeout = Duration::from_secs(config.spark.artifact_transfer_timeout_secs);
    let artifact_io_drain_timeout = artifact_io_drain_timeout(artifact_io_timeout);
    let artifact_cleanup_flush_timeout = artifact_cleanup_journal_flush_timeout();
    let server = SparkConnectServer::new(session_manager, config);
    let service = SparkConnectServiceServer::new(server)
        // The original Spark Connect server seems to have configuration for inbound (decoding) message size only.
        // .max_encoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    let server_result = ServerBuilder::new("sail_spark_connect", Default::default())
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
        .map_err(|error| error.to_string());
    let session_shutdown_result = session_manager_for_shutdown
        .shutdown()
        .await
        .map_err(|error| error.to_string());
    let artifact_io_result = drain_artifact_io(artifact_io_drain_timeout)
        .await
        .map_err(|error| error.to_string());
    let artifact_cleanup_result = flush_artifact_cleanup_journal(artifact_cleanup_flush_timeout)
        .await
        .map_err(|error| error.to_string());
    let artifact_cache_shutdown_result =
        match tokio::task::spawn_blocking(sail_python_udf::shutdown_python_artifact_cache).await {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(error) => Err(format!(
                "failed to join Python artifact cache shutdown: {error}"
            )),
        };

    let mut errors = Vec::new();
    if let Err(error) = server_result {
        errors.push(format!("Spark Connect server failed: {error}"));
    }
    if let Err(error) = session_shutdown_result {
        errors.push(format!("session manager shutdown failed: {error}"));
    }
    if let Err(error) = artifact_io_result {
        errors.push(format!("artifact I/O drain failed: {error}"));
    }
    if let Err(error) = artifact_cleanup_result {
        errors.push(format!("artifact cleanup journal flush failed: {error}"));
    }
    if let Err(error) = artifact_cache_shutdown_result {
        errors.push(format!("Python artifact cache shutdown failed: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Box::new(std::io::Error::other(errors.join("; "))))
    }
}
