use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastrace::future::FutureExt;
use fastrace::prelude::*;
use sail_plan::object_store::{load_aws_config, ObjectStoreConfig};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;
use tower::{Layer, Service, ServiceBuilder};

use crate::server::SparkConnectServer;
use crate::session::SessionManager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

// Same default as Spark
// https://github.com/apache/spark/blob/9cec3c4f7c1b467023f0eefff69e8b7c5105417d/python/pyspark/sql/connect/client/core.py#L126
pub const GRPC_MAX_MESSAGE_LENGTH_DEFAULT: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct TraceMiddleware<S> {
    inner: S,
}

impl<S, Request> Service<Request> for TraceMiddleware<S>
where
    S: Service<Request> + Clone + Send + 'static,
    Request: std::fmt::Debug,
    S::Future: Send + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let root_ctx = SpanContext::random();
        let root_span = Span::root("trace_id", root_ctx);
        let future = self.inner.call(request).in_span(root_span);
        Box::pin(future)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TraceLayer;

impl<S> Layer<S> for TraceLayer {
    type Service = TraceMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

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
        // FIXME: Unsure why this doesn't work. Might be fixed when we upgrade to tower-http 0.5.2
        //  Might be related: https://github.com/tower-rs/tower-http/issues/420
        // .layer(
        //     CompressionLayer::new().gzip(true).zstd(true),
        // )
        .layer(TraceLayer)
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
                .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
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
