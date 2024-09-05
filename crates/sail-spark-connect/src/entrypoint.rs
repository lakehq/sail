use std::future::Future;

use sail_plan::object_store::{load_aws_config, ObjectStoreConfig};
use tokio::net::TcpListener;
use tonic::codegen::http;
use tonic::transport::server::TcpIncoming;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, Span};

use crate::server::SparkConnectServer;
use crate::session::SessionManager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

// Same default as Spark
// https://github.com/apache/spark/blob/9cec3c4f7c1b467023f0eefff69e8b7c5105417d/python/pyspark/sql/connect/client/core.py#L126
pub const GRPC_MAX_MESSAGE_LENGTH_DEFAULT: usize = 128 * 1024 * 1024;

pub async fn serve<F>(
    // We must use the TCP listener from tokio.
    // The TCP listener from the standard library does not work with graceful shutdown.
    // See also: https://github.com/hyperium/tonic/issues/1424
    listener: TcpListener,
    signal: Option<F>,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let (mut health_reporter, health_server) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SparkConnectServiceServer<SparkConnectServer>>()
        .await;

    let reflect_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(crate::spark::connect::FILE_DESCRIPTOR_SET)
        .build()?;

    let object_store_config = ObjectStoreConfig::default().with_aws(load_aws_config().await);
    let session_manager = SessionManager::new().with_object_store_config(object_store_config);
    let server = SparkConnectServer::new(session_manager);

    let layer = ServiceBuilder::new()
        .layer(
            TraceLayer::new_for_grpc()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_request(|request: &http::Request<_>, _: &Span| {
                    debug!("{:?}", request);
                })
                .on_response(|response: &http::response::Response<_>, _, _: &Span| {
                    debug!("{:?}", response);
                }),
        )
        .into_inner();

    let nodelay = true;
    let keepalive = None;
    let incoming = TcpIncoming::from_listener(listener, nodelay, keepalive)
        .map_err(|e| e as Box<dyn std::error::Error>)?;
    let server = tonic::transport::Server::builder()
        .tcp_nodelay(nodelay)
        .tcp_keepalive(keepalive)
        .layer(layer)
        .add_service(reflect_server)
        .add_service(health_server)
        .add_service(
            SparkConnectServiceServer::new(server)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT),
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