use std::future::Future;

use tokio::net::TcpListener;
use tonic::codegen::http;
use tonic::transport::server::TcpIncoming;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, Span};

use crate::server::SparkConnectServer;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

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

    let server = SparkConnectServer::default();

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
        .add_service(SparkConnectServiceServer::new(server));

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
