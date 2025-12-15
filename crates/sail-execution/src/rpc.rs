use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use sail_telemetry::layers::{TracingClientLayer, TracingClientService};
use tokio::sync::{oneshot, OnceCell};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tower::ServiceBuilder;

use crate::driver::DriverServiceClient;
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
        /// The server port.
        port: u16,
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

    pub fn ready(self, signal: oneshot::Sender<()>, port: u16) -> ExecutionResult<Self> {
        match self {
            Self::Pending { handle } => Ok(Self::Running {
                signal,
                handle,
                port,
            }),
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
            Self::Running {
                signal,
                handle,
                port: _,
            } => {
                let _ = signal.send(());
                let _ = handle.await;
            }
        }
    }

    pub fn port(&self) -> Option<u16> {
        match self {
            Self::Running { port, .. } => Some(*port),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub enable_tls: bool,
    pub host: String,
    pub port: u16,
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
const CLIENT_MAX_HEADER_LIST_SIZE: u32 = 1024 * 1024;

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
                    .service(channel);
                Ok(<$client_type>::new(channel))
            }
        }
    };
}

pub type ClientService = TracingClientService<Channel>;

impl_client_builder!(DriverServiceClient<ClientService>);
impl_client_builder!(WorkerServiceClient<ClientService>);
impl_client_builder!(FlightServiceClient<ClientService>);

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
    /// It is cheap to clone the client and return `T`, since they rely on [Channel] which is
    /// cheap to clone. The underlying connection is reused among clones of the client.
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
