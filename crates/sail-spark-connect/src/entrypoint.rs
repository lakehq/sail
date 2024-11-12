use std::future::Future;
use std::sync::Arc;

use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_plan::object_store::ObjectStoreConfig;
use sail_server::ServerBuilder;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::server::SparkConnectServer;
use crate::session_manager::SessionManager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

pub async fn serve<F>(listener: TcpListener, signal: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    ObjectStoreConfig::initialize().await;
    let config = AppConfig::load()?;
    let session_manager = SessionManager::new(Arc::new(config));
    let server = SparkConnectServer::new(session_manager);
    let service = SparkConnectServiceServer::new(server)
        // The original Spark Connect server seems to have configuration for inbound (decoding) message size only.
        // .max_encoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    ServerBuilder::new("sail_spark_connect", Default::default())
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
}
