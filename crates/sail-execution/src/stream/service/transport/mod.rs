use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
const DEFAULT_INITIAL_STREAM_WINDOW_SIZE: u32 = 256 * 1024;

/// The largest initial stream window size that is used. A larger window would allow
/// too few streams per connection.
const MAX_INITIAL_STREAM_WINDOW_SIZE: u32 = 2 * 1024 * 1024;

/// The number of connections that a pool can grow to, unless more are configured.
const MAX_CONNECTIONS: usize = 16;

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
/// When the pool cannot grow further, the streams share the connections beyond the
/// limit, since a request must not fail for this reason.
#[derive(Clone)]
pub(super) struct FlightTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    origin: Uri,
    connector: Connector,
    connections: parking_lot::Mutex<Vec<Arc<PooledConnection>>>,
    next_connection: AtomicUsize,
    /// The number of connections that the requests are distributed across.
    /// The other connections of the pool are only used when these have reached the limit.
    initial_connections: usize,
    max_connections: usize,
    max_connection_streams: usize,
    /// Whether it has been reported that streams share connections beyond the limit.
    limit_exceeded: AtomicBool,
    initial_stream_window_size: u32,
    initial_connection_window_size: u32,
}

#[derive(Default)]
struct PooledConnection {
    sender: Mutex<Option<SendRequest<Body>>>,
    /// The number of requests whose responses have not been dropped.
    streams: AtomicUsize,
    /// Whether the connection could not be opened when the pool tried to grow.
    failed: AtomicBool,
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
        let max_connections = options.flight_connection_count.get().max(MAX_CONNECTIONS);
        Self::connect_with_limits(
            options,
            initial_stream_window_size,
            MAX_WINDOW_SIZE,
            max_connections,
        )
        .await
    }

    async fn connect_with_limits(
        options: &ClientOptions,
        initial_stream_window_size: u32,
        initial_connection_window_size: u32,
        max_connections: usize,
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
                initial_connections: options.flight_connection_count.get(),
                max_connections,
                max_connection_streams: max_connection_streams as usize,
                limit_exceeded: AtomicBool::new(false),
                initial_stream_window_size,
                initial_connection_window_size,
            }),
        };
        transport.sender(0).await?;
        Ok(transport)
    }

    /// Reserves a stream on the next initial connection in round-robin order that has
    /// not reached the limit, or else on the first other connection that has not
    /// reached it, or else on a new connection. If the pool cannot grow, the stream is
    /// reserved on the connection with the fewest streams, beyond the limit.
    fn reserve_stream(&self) -> (usize, ConnectionStream) {
        let inner = &self.inner;
        let mut connections = inner.connections.lock();
        let available = |connection: &PooledConnection| {
            connection.streams.load(Ordering::Relaxed) < inner.max_connection_streams
                && !connection.failed.load(Ordering::Relaxed)
        };
        let initial = inner.initial_connections;
        let start = inner.next_connection.fetch_add(1, Ordering::Relaxed) % initial;
        let mut index = (0..initial)
            .map(|offset| (start + offset) % initial)
            .find(|&index| available(&connections[index]));
        if index.is_none() {
            index = (initial..connections.len()).find(|&index| available(&connections[index]));
        }
        if index.is_none() && connections.len() < inner.max_connections {
            connections.push(Arc::default());
            index = Some(connections.len() - 1);
        }
        let index = index.unwrap_or_else(|| {
            self.report_limit_exceeded();
            (0..connections.len())
                .filter(|&index| !connections[index].failed.load(Ordering::Relaxed))
                .min_by_key(|&index| connections[index].streams.load(Ordering::Relaxed))
                .unwrap_or(0)
        });
        // The number only increases while the pool is locked.
        let connection = connections[index].clone();
        connection.streams.fetch_add(1, Ordering::Relaxed);
        (index, ConnectionStream { connection })
    }

    /// Reserves a stream on the open connection with the fewest streams, for a request
    /// whose connection could not be opened. Returns `None` if no other connection is open.
    fn reserve_open_stream(&self, failed: usize) -> Option<(usize, ConnectionStream)> {
        let inner = &self.inner;
        let connections = inner.connections.lock();
        if failed >= inner.initial_connections {
            // The pool does not try to grow with this connection again.
            connections[failed].failed.store(true, Ordering::Relaxed);
        }
        let index = (0..connections.len())
            .filter(|&index| {
                index != failed
                    && connections[index]
                        .sender
                        .try_lock()
                        .is_ok_and(|sender| sender.as_ref().is_some_and(|x| !x.is_closed()))
            })
            .min_by_key(|&index| connections[index].streams.load(Ordering::Relaxed))?;
        self.report_limit_exceeded();
        let connection = connections[index].clone();
        connection.streams.fetch_add(1, Ordering::Relaxed);
        Some((index, ConnectionStream { connection }))
    }

    fn report_limit_exceeded(&self) {
        if !self.inner.limit_exceeded.swap(true, Ordering::Relaxed) {
            log::warn!(
                "the Flight connection pool for {} cannot grow, so its connections serve more streams than intended",
                self.inner.origin
            );
        }
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
        let (index, mut stream) = self.reserve_stream();
        Box::pin(async move {
            // This transport only sends unary DoGet tickets. Buffer the small request
            // so Hyper marks its last DATA frame END_STREAM. Tonic's streaming body
            // otherwise sends a non-final small frame, and concurrent tickets can
            // exhaust h2's small-frame budget before the server consumes them.
            let (parts, body) = request.into_parts();
            let body = Body::new(Full::new(body.collect().await?.to_bytes()));
            let request = Request::from_parts(parts, body);
            let mut sender = match transport.sender(index).await {
                Ok(sender) => sender,
                Err(error) => {
                    // The limit of streams per connection must not make a request fail.
                    // If the connection cannot be opened, use another one that is open.
                    let Some((index, reserved)) = transport.reserve_open_stream(index) else {
                        return Err(error);
                    };
                    stream = reserved;
                    transport.sender(index).await?
                }
            };
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
