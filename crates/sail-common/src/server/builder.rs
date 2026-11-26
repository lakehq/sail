// SPDX-License-Identifier: Apache-2.0

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http::Request;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::server::NamedService;
use tonic::transport::server::{Router, TcpIncoming};
use tonic_health::server::HealthReporter;
use tower::ServiceBuilder;
use tower::layer::util::{Identity, Stack};

use crate::server::connection::ServerConnection;
use crate::telemetry::TracingServerLayer;

pub struct ServerBuilderOptions {
    pub nodelay: bool,
    pub keepalive: Option<std::time::Duration>,
    pub http2_keepalive_interval: Option<std::time::Duration>,
    pub http2_keepalive_timeout: Option<std::time::Duration>,
    pub http2_adaptive_window: Option<bool>,
    /// The initial HTTP/2 connection window size, or the default of Hyper if not set.
    /// The adaptive window must be off for this to take effect.
    pub http2_initial_connection_window_size: Option<u32>,
}

impl Default for ServerBuilderOptions {
    fn default() -> Self {
        Self {
            // Disables Nagle's algorithm
            nodelay: true,
            keepalive: Some(std::time::Duration::from_mins(1)),
            http2_keepalive_interval: Some(std::time::Duration::from_mins(1)),
            http2_keepalive_timeout: Some(std::time::Duration::from_secs(10)),
            http2_adaptive_window: Some(true),
            http2_initial_connection_window_size: None,
        }
    }
}

pub struct ServerBuilder<'b> {
    #[expect(dead_code)]
    name: &'static str,
    options: ServerBuilderOptions,
    shutdown_grace_period: Option<Duration>,
    health_reporter: HealthReporter,
    reflection_server_builder: tonic_reflection::server::Builder<'b>,
    // The router type has to change accordingly when layers are added.
    router: Router<Stack<Stack<TracingServerLayer, Identity>, Identity>>,
}

impl<'b> ServerBuilder<'b> {
    pub fn new(name: &'static str, options: ServerBuilderOptions) -> Self {
        let (health_reporter, health_server) = tonic_health::server::health_reporter();

        // TODO: We may want to turn off reflection in production if it affects performance.
        let reflection_server_builder = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET);

        let layer = ServiceBuilder::new()
            // FIXME: Unsure why this doesn't work. Might be fixed when we upgrade to tower-http 0.5.2
            //  Might be related: https://github.com/tower-rs/tower-http/issues/420
            // .layer(
            //     CompressionLayer::new().gzip(true).zstd(true),
            // )
            .layer(TracingServerLayer)
            .into_inner();

        let router = tonic::transport::Server::builder()
            .tcp_nodelay(options.nodelay)
            .tcp_keepalive(options.keepalive)
            .http2_keepalive_interval(options.http2_keepalive_interval)
            .http2_keepalive_timeout(options.http2_keepalive_timeout)
            .http2_adaptive_window(options.http2_adaptive_window)
            .initial_connection_window_size(options.http2_initial_connection_window_size)
            .layer(layer)
            .add_service(health_server);

        Self {
            name,
            options,
            shutdown_grace_period: None,
            health_reporter,
            reflection_server_builder,
            router,
        }
    }

    /// Close connections that remain open after graceful shutdown begins.
    pub fn with_shutdown_grace_period(mut self, grace_period: Duration) -> Self {
        self.shutdown_grace_period = Some(grace_period);
        self
    }

    pub async fn add_service<S>(mut self, service: S, file_descriptor_set: Option<&'b [u8]>) -> Self
    where
        S: Service<Request<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        self.health_reporter.set_serving::<S>().await;
        if let Some(file_descriptor_set) = file_descriptor_set {
            self.reflection_server_builder = self
                .reflection_server_builder
                .register_encoded_file_descriptor_set(file_descriptor_set);
        }
        self.router = self.router.add_service(service);
        self
    }

    pub async fn serve<F>(
        self,
        // We must use the TCP listener from tokio.
        // The TCP listener from the standard library does not work with graceful shutdown.
        // See also: https://github.com/hyperium/tonic/issues/1424
        listener: TcpListener,
        signal: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Future<Output = ()>,
    {
        let reflection_server = self.reflection_server_builder.build_v1()?;
        let router = self.router.add_service(reflection_server);

        let incoming = TcpIncoming::from(listener)
            .with_nodelay(Some(self.options.nodelay))
            .with_keepalive(self.options.keepalive);

        if let Some(grace_period) = self.shutdown_grace_period {
            let close = CancellationToken::new();
            let incoming = incoming
                .map(|stream| stream.map(|stream| ServerConnection::new(stream, close.clone())));
            // Dropping the timer when the server finishes lets responsive clients exit immediately.
            let mut timer = JoinSet::new();
            let signal = async {
                signal.await;
                let close = close.clone();
                timer.spawn(async move {
                    tokio::time::sleep(grace_period).await;
                    close.cancel();
                });
            };
            router
                .serve_with_incoming_shutdown(incoming, signal)
                .await?;
        } else {
            router
                .serve_with_incoming_shutdown(incoming, signal)
                .await?;
        }

        Ok(())
    }
}
