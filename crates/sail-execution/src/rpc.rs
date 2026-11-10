use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http_body_util::{BodyExt, Full};
use sail_common::telemetry::{TracingClientLayer, TracingClientService};
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;
use tonic::body::Body;
use tonic::codegen::StdError;
use tonic::codegen::http::{Request, Response};
use tonic::transport::Channel;
use tower::{Service, ServiceBuilder};

use crate::driver::{CelebornLifecycleManagerServiceClient, DriverServiceClient};
use crate::error::{ExecutionError, ExecutionResult};
use crate::worker::WorkerServiceClient;

pub enum ServerMonitor {
    Stopped,
    Pending {
        handle: JoinHandle<ExecutionResult<()>>,
    },
    Running {
        /// The shutdown signal to send to the server,
        /// or `None` if the server is not running.
        signal: oneshot::Sender<()>,
        /// The join handle of the server task.
        handle: JoinHandle<ExecutionResult<()>>,
    },
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerMonitor {
    pub fn new() -> Self {
        Self::Stopped
    }

    pub async fn start(
        self,
        f: impl Future<Output = ExecutionResult<()>> + Send + 'static,
    ) -> Self {
        self.stop().await;
        Self::Pending {
            handle: tokio::spawn(f),
        }
    }

    pub fn ready(self, signal: oneshot::Sender<()>) -> ExecutionResult<Self> {
        match self {
            Self::Pending { handle } => Ok(Self::Running { signal, handle }),
            _ => Err(ExecutionError::InternalError(
                "the server must be in pending state before it can be ready".to_string(),
            )),
        }
    }

    pub async fn stop(self) {
        match self {
            Self::Stopped => {}
            Self::Pending { handle } => {
                handle.abort();
            }
            Self::Running { signal, handle } => {
                let _ = signal.send(());
                let _ = handle.await;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub enable_tls: bool,
    pub host: String,
    pub port: u16,
    pub flight_connection_count: NonZeroUsize,
    pub flight_initial_stream_window_size: Option<u32>,
    pub flight_initial_connection_window_size: Option<u32>,
}

impl ClientOptions {
    pub fn to_url_string(&self) -> String {
        let scheme = if self.enable_tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

#[tonic::async_trait]
pub trait ClientBuilder: Sized {
    async fn connect(options: &ClientOptions) -> ExecutionResult<Self>;
}

/// Maximum header list size for gRPC clients.
/// The value here is larger than the default, so that the clients can receive long error details
/// (e.g. Python traceback) from the server via HTTP headers.
/// The error details are stored as binary data in the Tonic status.
/// If the header list size is larger than the allowed size, the error details would be
/// dropped silently.
pub(crate) const CLIENT_MAX_HEADER_LIST_SIZE: u32 = 1024 * 1024;

macro_rules! impl_client_builder {
    ($client_type:ty) => {
        #[tonic::async_trait]
        impl ClientBuilder for $client_type {
            async fn connect(options: &ClientOptions) -> ExecutionResult<Self> {
                let channel = tonic::transport::Endpoint::new(options.to_url_string())?
                    .http2_max_header_list_size(CLIENT_MAX_HEADER_LIST_SIZE)
                    .connect()
                    .await?;
                let channel = ServiceBuilder::new()
                    .layer(TracingClientLayer)
                    .service(UnaryChannel { inner: channel });
                Ok(<$client_type>::new(channel))
            }
        }
    };
}

pub type ClientService = TracingClientService<UnaryChannel>;

/// A channel for clients whose RPCs are all unary.
/// Tonic's streaming body sends the request message in a DATA frame without END_STREAM,
/// followed by an empty final frame. h2 charges such small non-final frames against a
/// per-connection budget until the server consumes them, so hundreds of concurrent task
/// status reports or stop requests can make the server close the connection with
/// `too_many_data_frames`. Buffering the request lets Hyper mark its last DATA frame
/// END_STREAM, which h2 exempts from the budget. A request can still be split into a
/// small non-final frame when the connection window is exhausted.
/// The whole request body is collected before the call, so a client-streaming RPC
/// must not use this channel.
#[derive(Debug, Clone)]
pub struct UnaryChannel {
    inner: Channel,
}

impl Service<Request<Body>> for UnaryChannel {
    type Response = Response<Body>;
    type Error = StdError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // The capacity reserved by `poll_ready` belongs to this instance, not to a clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let body = Body::new(Full::new(body.collect().await?.to_bytes()));
            let request = Request::from_parts(parts, body);
            Ok(inner.call(request).await?)
        })
    }
}

impl_client_builder!(DriverServiceClient<ClientService>);
impl_client_builder!(CelebornLifecycleManagerServiceClient<ClientService>);
impl_client_builder!(WorkerServiceClient<ClientService>);

/// A handle to a gRPC client to support connection reuse.
/// The handle can be cheaply cloned and the underlying connection is shared.
#[derive(Debug, Clone)]
pub struct ClientHandle<T> {
    /// The client options.
    options: Arc<ClientOptions>,
    /// The shared gRPC client which is lazily initialized.
    /// Note that this must be `Arc<OnceCell<T>>` instead of `OnceCell<Arc<T>>`.
    /// If we use the latter, when the client is not initialized, an empty `OnceCell` would be
    /// cloned and later initialized independently, resulting in multiple connections.
    /// This could then easily overwhelm the server, and the client would see the
    /// "connection refused" Tonic transport error.
    inner: Arc<OnceCell<T>>,
}

impl<T: ClientBuilder + Clone> ClientHandle<T> {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            options: Arc::new(options),
            inner: Arc::new(OnceCell::new()),
        }
    }

    /// Returns a clone of the RPC client.
    /// The client requires `&mut self` when making RPC requests,
    /// so it is less useful to return `&T` here.
    /// It is cheap to clone the client and return `T`: both [Channel] and the Flight transport
    /// share the underlying connection among clones of the client.
    /// Also, since the client can be cheaply cloned, we avoid the overhead of using a mutex
    /// to protect a shared client instance.
    pub async fn get(&self) -> ExecutionResult<T> {
        let options = Arc::clone(&self.options);
        self.inner
            .get_or_try_init(|| T::connect(&options))
            .await
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::net::TcpListener;
    use tokio_util::task::AbortOnDropHandle;
    use tonic::codegen::http::{HeaderMap, HeaderValue};

    use super::*;
    use crate::driver::r#gen::{CelebornGetShuffleIdRequest, ReportTaskStatusRequest};
    use crate::worker::r#gen::StopTaskRequest;

    type TestError = Box<dyn std::error::Error + Send + Sync>;

    /// Accepts one connection and answers one unary call with an empty message.
    async fn call_once<C, F, R>(call: F) -> Result<(), TestError>
    where
        C: ClientBuilder,
        F: FnOnce(C) -> R,
        R: Future<Output = Result<(), tonic::Status>>,
    {
        tokio::time::timeout(Duration::from_secs(15), async {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = AbortOnDropHandle::new(tokio::spawn(async move {
                let (socket, _) = listener.accept().await?;
                // Reject even one non-final small DATA frame. A final frame is exempt,
                // so this checks the framing without depending on timing or concurrency.
                let mut connection = h2::server::Builder::new()
                    .data_frame_budget(0)
                    .handshake::<_, Bytes>(socket)
                    .await?;
                let (request, mut respond) =
                    connection.accept().await.ok_or("missing request")??;
                let handled = tokio::spawn(async move {
                    let mut body = request.into_body();
                    while let Some(data) = body.data().await {
                        let data = data?;
                        body.flow_control().release_capacity(data.len())?;
                    }
                    let response = Response::builder()
                        .header("content-type", "application/grpc")
                        .body(())?;
                    let mut stream = respond.send_response(response, false)?;
                    stream.send_data(Bytes::from_static(&[0; 5]), false)?;
                    let mut trailers = HeaderMap::new();
                    trailers.insert("grpc-status", HeaderValue::from_static("0"));
                    stream.send_trailers(trailers)?;
                    Ok::<_, TestError>(())
                });
                while let Some(request) = connection.accept().await {
                    request?;
                }
                handled.await??;
                Ok::<_, TestError>(())
            }));
            let options = ClientOptions {
                enable_tls: false,
                host: address.ip().to_string(),
                port: address.port(),
                flight_connection_count: NonZeroUsize::MIN,
                flight_initial_stream_window_size: None,
                flight_initial_connection_window_size: None,
            };
            call(C::connect(&options).await?).await?;
            server.await??;
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn driver_client_sends_final_data_frame() -> Result<(), TestError> {
        call_once(
            |mut client: DriverServiceClient<ClientService>| async move {
                client
                    .report_task_status(ReportTaskStatusRequest::default())
                    .await?;
                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn celeborn_client_sends_final_data_frame() -> Result<(), TestError> {
        call_once(
            |mut client: CelebornLifecycleManagerServiceClient<ClientService>| async move {
                client
                    .get_shuffle_id(CelebornGetShuffleIdRequest::default())
                    .await?;
                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn worker_client_sends_final_data_frame() -> Result<(), TestError> {
        call_once(
            |mut client: WorkerServiceClient<ClientService>| async move {
                client.stop_task(StopTaskRequest::default()).await?;
                Ok(())
            },
        )
        .await
    }
}
