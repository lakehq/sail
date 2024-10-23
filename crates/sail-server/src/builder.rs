use std::convert::Infallible;
use std::future::Future;

use sail_telemetry::trace_layer::TraceLayer;
use tokio::net::TcpListener;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::server::{Router, TcpIncoming};
use tonic_health::server::HealthReporter;
use tower::layer::util::{Identity, Stack};
use tower::ServiceBuilder;

pub struct ServerBuilderOptions {
    pub nodelay: bool,
    pub keepalive: Option<std::time::Duration>,
}

impl Default for ServerBuilderOptions {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive: None,
        }
    }
}

pub struct ServerBuilder<'b> {
    #[allow(dead_code)]
    name: &'static str,
    options: ServerBuilderOptions,
    health_reporter: HealthReporter,
    reflection_server_builder: tonic_reflection::server::Builder<'b>,
    // The router type has to change accordingly when layers are added.
    router: Router<Stack<Stack<TraceLayer, Identity>, Identity>>,
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
            .layer(TraceLayer::new(name))
            .into_inner();

        let router = tonic::transport::Server::builder()
            .tcp_nodelay(options.nodelay)
            .tcp_keepalive(options.keepalive)
            .layer(layer)
            .add_service(health_server);

        Self {
            name,
            options,
            health_reporter,
            reflection_server_builder,
            router,
        }
    }

    pub async fn add_service<S>(mut self, service: S, file_descriptor_set: Option<&'b [u8]>) -> Self
    where
        S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
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

        let incoming =
            TcpIncoming::from_listener(listener, self.options.nodelay, self.options.keepalive)
                .map_err(|e| e as Box<dyn std::error::Error>)?;

        router
            .serve_with_incoming_shutdown(incoming, signal)
            .await?;

        Ok(())
    }
}
