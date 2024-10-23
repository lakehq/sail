use std::future::Future;

use sail_plan::object_store::{load_aws_config, ObjectStoreConfig};
use sail_server::ServerBuilder;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::server::SparkConnectServer;
use crate::session::SessionManager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

// Same default as Spark
// https://github.com/apache/spark/blob/9cec3c4f7c1b467023f0eefff69e8b7c5105417d/python/pyspark/sql/connect/client/core.py#L126
pub const GRPC_MAX_MESSAGE_LENGTH_DEFAULT: usize = 128 * 1024 * 1024;

pub async fn serve<F>(listener: TcpListener, signal: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let object_store_config = ObjectStoreConfig::default().with_aws(load_aws_config().await);
    let session_manager = SessionManager::new().with_object_store_config(object_store_config);
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
