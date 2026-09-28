use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_flight::flight_service_client::FlightServiceClient;
use futures::future::BoxFuture;
use hyper::body::Incoming;
use hyper::client::conn::http2::{Builder, SendRequest};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder, MaybeHttpsStream};
use hyper_util::client::legacy::connect::{Connection, HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use sail_common::telemetry::{TracingClientLayer, TracingClientService};
use tokio::sync::Mutex;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response, Uri};
use tower::{Service, ServiceBuilder, ServiceExt};

use crate::error::{ExecutionError, ExecutionResult};
use crate::rpc::{CLIENT_MAX_HEADER_LIST_SIZE, ClientBuilder, ClientOptions};

#[cfg(test)]
mod tests;

type TransportError = Box<dyn std::error::Error + Send + Sync>;

pub(super) type FlightClient = FlightServiceClient<TracingClientService<FlightTransport>>;

#[tonic::async_trait]
impl ClientBuilder for FlightClient {
    async fn connect(options: &ClientOptions) -> ExecutionResult<Self> {
        let transport = FlightTransport::connect(options)
            .await
            .map_err(ExecutionError::FlightTransportError)?;
        let origin = transport.inner.origin.clone();
        let service = ServiceBuilder::new()
            .layer(TracingClientLayer)
            .service(transport);
        Ok(Self::with_origin(service, origin))
    }
}

/// Internal Flight connections allow early termination of many shuffle streams (e.g. LIMIT).
/// Tonic's Channel does not expose Hyper's client reset budget, so we use Hyper directly.
/// All clones share connection establishment and reconnection, but dispatch requests concurrently.
#[derive(Clone)]
pub(super) struct FlightTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    origin: Uri,
    connector: Connector,
    sender: Mutex<Option<SendRequest<Body>>>,
}

enum Connector {
    Http(HttpConnector),
    Https(HttpsConnector<HttpConnector>),
}

impl FlightTransport {
    async fn connect(options: &ClientOptions) -> Result<Self, TransportError> {
        let origin = options.to_url_string().parse()?;
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        let connector = if options.enable_tls {
            http.enforce_http(false);
            // Match Tonic's provider selection and native trust roots. Do not load certificates
            // for plaintext cluster connections.
            let provider = rustls::crypto::CryptoProvider::get_default()
                .cloned()
                .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
            Connector::Https(
                HttpsConnectorBuilder::new()
                    .with_provider_and_native_roots(provider)?
                    .https_only()
                    .enable_http2()
                    .wrap_connector(http),
            )
        } else {
            Connector::Http(http)
        };
        let transport = Self {
            inner: Arc::new(TransportInner {
                origin,
                connector,
                sender: Mutex::new(None),
            }),
        };
        transport.sender().await?;
        Ok(transport)
    }

    async fn sender(&self) -> Result<SendRequest<Body>, TransportError> {
        // Hold the lock through connection establishment to avoid a connection stampede.
        // Never hold it while awaiting response headers or consuming response bodies.
        let mut sender = self.inner.sender.lock().await;
        if let Some(sender) = sender.as_ref()
            && !sender.is_closed()
        {
            return Ok(sender.clone());
        }
        let origin = self.inner.origin.clone();
        let io = match &self.inner.connector {
            Connector::Http(connector) => {
                MaybeHttpsStream::Http(connector.clone().oneshot(origin).await?)
            }
            Connector::Https(connector) => {
                let io = connector.clone().oneshot(origin).await?;
                if !io.connected().is_negotiated_h2() {
                    return Err(std::io::Error::other(
                        "Flight TLS connection did not negotiate h2",
                    )
                    .into());
                }
                io
            }
        };
        let (new_sender, connection) = Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .max_header_list_size(CLIENT_MAX_HEADER_LIST_SIZE)
            // Trusted internal shuffle traffic can legitimately cancel many streams. Late frames
            // may count as internal resets; the lifetime budget must not kill unrelated streams.
            .max_local_error_reset_streams(None)
            .adaptive_window(true)
            .handshake(io)
            .await?;
        // Hyper closes the connection after senders and active responses are released. Let the
        // driver outlive the transport so dropping a client does not interrupt a response body.
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                log::debug!("Flight HTTP/2 connection closed: {error}");
            }
        });
        *sender = Some(new_sender.clone());
        Ok(new_sender)
    }
}

impl Service<Request<Body>> for FlightTransport {
    type Response = Response<Incoming>;
    type Error = TransportError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Hyper's HTTP/2 dispatcher is unbounded; connection readiness is checked in call().
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let transport = self.clone();
        Box::pin(async move {
            let mut sender = transport.sender().await?;
            // A close racing with dispatch is returned to the task scheduler. Never replay a
            // DoGet request: subscribing to a shuffle stream can only be done once per replica.
            Ok(sender.send_request(request).await?)
        })
    }
}
