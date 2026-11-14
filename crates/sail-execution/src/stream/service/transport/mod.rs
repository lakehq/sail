use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use arrow_flight::flight_service_client::FlightServiceClient;
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::{BodyExt, Full};
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::client::conn::http2::{Builder, SendRequest};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder, MaybeHttpsStream};
use hyper_util::client::legacy::connect::{Connection, HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
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

/// The largest HTTP/2 flow-control window size.
const MAX_WINDOW_SIZE: u32 = i32::MAX as u32;

/// The initial stream window size that is used unless configured.
/// This is our own choice and does not follow the default of Hyper.
const DEFAULT_INITIAL_STREAM_WINDOW_SIZE: u32 = 2 * 1024 * 1024;

/// The largest initial stream window size that is used. A larger window would allow
/// too few streams per connection.
const MAX_INITIAL_STREAM_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

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
        // A message holds at least one row, so a large value can exceed tonic's 4 MiB default.
        Ok(FlightClient::with_origin(service, origin)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT))
    }
}

/// Internal Flight connections allow early termination of many shuffle streams (e.g. LIMIT).
/// Tonic's Channel does not expose Hyper's client reset budget, so we use Hyper directly.
/// All clones share a pool of connections and dispatch requests in round-robin order.
///
/// A shuffle stream that is open but not being read holds up to one stream window of
/// the window that all streams on its connection share. Each connection therefore
/// serves a limited number of streams at a time, so that such streams hold at most
/// half of the connection window and the other streams on the connection can always
/// receive data. The pool grows when all its connections have reached the limit.
#[derive(Clone)]
pub(super) struct FlightTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    origin: Uri,
    connector: Connector,
    connections: parking_lot::Mutex<Vec<Arc<PooledConnection>>>,
    next_connection: AtomicUsize,
    max_connection_streams: usize,
    initial_stream_window_size: u32,
    initial_connection_window_size: u32,
}

#[derive(Default)]
struct PooledConnection {
    sender: Mutex<Option<SendRequest<Body>>>,
    /// The number of requests whose responses have not been dropped.
    streams: AtomicUsize,
}

/// A stream reserved on a connection, which is released when dropped.
struct ConnectionStream {
    connection: Arc<PooledConnection>,
}

impl Drop for ConnectionStream {
    fn drop(&mut self) {
        self.connection.streams.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A response body that keeps the stream on its connection reserved.
pub(super) struct FlightResponseBody {
    body: Incoming,
    _stream: ConnectionStream,
}

impl hyper::body::Body for FlightResponseBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

enum Connector {
    Http(HttpConnector),
    Https(HttpsConnector<HttpConnector>),
}

impl FlightTransport {
    async fn connect(options: &ClientOptions) -> Result<Self, TransportError> {
        for (name, size) in [
            ("stream", options.flight_initial_stream_window_size),
            ("connection", options.flight_initial_connection_window_size),
        ] {
            if size.is_some_and(|size| size > MAX_WINDOW_SIZE) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Flight initial {name} window size must not exceed 2147483647 bytes"),
                )
                .into());
            }
        }
        let mut initial_stream_window_size = options
            .flight_initial_stream_window_size
            .unwrap_or(DEFAULT_INITIAL_STREAM_WINDOW_SIZE);
        if initial_stream_window_size > MAX_INITIAL_STREAM_WINDOW_SIZE {
            log::warn!(
                "Flight initial stream window size is reduced from {initial_stream_window_size} to {MAX_INITIAL_STREAM_WINDOW_SIZE} bytes"
            );
            initial_stream_window_size = MAX_INITIAL_STREAM_WINDOW_SIZE;
        }
        // The configured connection window size is not used. A smaller window would not
        // save memory since the pool grows, and would only allow fewer streams per connection.
        Self::connect_with_windows(options, initial_stream_window_size, MAX_WINDOW_SIZE).await
    }

    async fn connect_with_windows(
        options: &ClientOptions,
        initial_stream_window_size: u32,
        initial_connection_window_size: u32,
    ) -> Result<Self, TransportError> {
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
        // Streams that are not being read hold at most half of the connection window.
        let max_connection_streams =
            initial_connection_window_size.div_ceil(2) / initial_stream_window_size;
        let transport = Self {
            inner: Arc::new(TransportInner {
                origin,
                connector,
                connections: parking_lot::Mutex::new(
                    (0..options.flight_connection_count.get())
                        .map(|_| Arc::default())
                        .collect(),
                ),
                next_connection: AtomicUsize::new(0),
                max_connection_streams: max_connection_streams as usize,
                initial_stream_window_size,
                initial_connection_window_size,
            }),
        };
        transport.sender(0).await?;
        Ok(transport)
    }

    /// Reserves a stream on the next connection in round-robin order that has not
    /// reached the limit, or on a new connection if there is none.
    fn reserve_stream(&self) -> (usize, ConnectionStream) {
        let mut connections = self.inner.connections.lock();
        let start = self.inner.next_connection.fetch_add(1, Ordering::Relaxed) % connections.len();
        let index = (0..connections.len())
            .map(|offset| (start + offset) % connections.len())
            .find(|&index| {
                connections[index].streams.load(Ordering::Relaxed)
                    < self.inner.max_connection_streams
            })
            .unwrap_or_else(|| {
                connections.push(Arc::default());
                connections.len() - 1
            });
        // The number only increases while the pool is locked, so it stays within the limit.
        let connection = connections[index].clone();
        connection.streams.fetch_add(1, Ordering::Relaxed);
        (index, ConnectionStream { connection })
    }

    async fn sender(&self, index: usize) -> Result<SendRequest<Body>, TransportError> {
        let connection = self.inner.connections.lock()[index].clone();
        // Hold the lock through connection establishment to avoid a connection stampede.
        // Never hold it while awaiting response headers or consuming response bodies.
        let mut sender = connection.sender.lock().await;
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
            .initial_stream_window_size(Some(self.inner.initial_stream_window_size))
            .initial_connection_window_size(Some(self.inner.initial_connection_window_size))
            .max_header_list_size(CLIENT_MAX_HEADER_LIST_SIZE)
            // Trusted internal shuffle traffic can legitimately cancel many streams. Late frames
            // may count as internal resets; the lifetime budget must not kill unrelated streams.
            .max_local_error_reset_streams(None)
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
    type Response = Response<FlightResponseBody>;
    type Error = TransportError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Hyper's HTTP/2 dispatcher is unbounded; connection readiness is checked in call().
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let transport = self.clone();
        let (index, stream) = self.reserve_stream();
        Box::pin(async move {
            // This transport only sends unary DoGet tickets. Buffer the small request
            // so Hyper marks its last DATA frame END_STREAM. Tonic's streaming body
            // otherwise sends a non-final small frame, and concurrent tickets can
            // exhaust h2's small-frame budget before the server consumes them.
            let (parts, body) = request.into_parts();
            let body = Body::new(Full::new(body.collect().await?.to_bytes()));
            let request = Request::from_parts(parts, body);
            let mut sender = transport.sender(index).await?;
            // A close racing with dispatch is returned to the task scheduler. Never replay a
            // DoGet request: subscribing to a shuffle stream can only be done once per replica.
            let response = sender.send_request(request).await?;
            Ok(response.map(|body| FlightResponseBody {
                body,
                _stream: stream,
            }))
        })
    }
}
